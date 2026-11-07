using System.Buffers.Binary;

namespace StorageEngine;

/// <summary>
/// One immutable sorted table file. Layout (all integers little-endian):
///
///   Data section — entries in ascending key order:
///     [keyLen i32][key][flags u8: 0 = value, 1 = tombstone]
///     [valueLen i32][value]              (omitted for tombstones)
///   Index section — sparse index, one entry per sampled key:
///     [keyLen i32][key][dataOffset i64]  (first entry is the smallest key;
///                                         the final entry is the largest key)
///   Bloom section — [hashCount i32][bit bytes]
///   Footer — fixed 32 bytes at end of file:
///     [indexOffset i64][bloomOffset i64][bloomLength i32][entryCount i32]
///     [magic 8 bytes "LSMDB001"]
///
/// A point read checks the bloom filter before touching the index, binary
/// searches the sparse index, then scans forward from the sampled offset —
/// at most <see cref="StorageOptions.IndexSampleInterval"/> entries.
/// </summary>
internal static class SSTable
{
    internal const int FooterSize = 32;
    private static readonly byte[] Magic = "LSMDB001"u8.ToArray();
    private const byte FlagValue = 0;
    private const byte FlagTombstone = 1;

    internal static string FileName(long id) => $"sst-{id:D6}.lsm";
    internal static string TempFileName(long id) => $"sst-{id:D6}.tmp";

    internal sealed class Writer : IDisposable
    {
        private readonly FileStream _stream;
        private readonly int _sampleInterval;
        private readonly List<(byte[] Key, long Offset)> _index = new();
        private readonly BloomFilter _bloom;
        private byte[]? _lastKey;
        private long _lastOffset;
        private int _entryCount;
        private bool _finished;

        /// <param name="path">File to write (a temporary name; the caller renames it into place).</param>
        /// <param name="options">Engine options (index sampling, bloom sizing).</param>
        /// <param name="expectedEntries">
        /// The number of entries that will be added. The bloom filter is sized
        /// from it up front — a bloom filter cannot grow after keys are added.
        /// </param>
        public Writer(string path, StorageOptions options, int expectedEntries)
        {
            _stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096);
            _sampleInterval = options.IndexSampleInterval;
            _bloom = BloomFilter.Create(Math.Max(expectedEntries, 1), options.BloomBitsPerKey);
        }

        public int EntryCount => _entryCount;

        /// <summary>Entries must be added in strictly ascending key order. A null value writes a tombstone.</summary>
        public void Add(byte[] key, byte[]? value)
        {
            if (_finished)
                throw new InvalidOperationException("Cannot add to a finished SSTable.");
            if (_lastKey is not null && ByteArrayComparer.Instance.Compare(key, _lastKey) <= 0)
                throw new InvalidOperationException("SSTable entries must be added in strictly ascending key order.");

            var offset = _stream.Position;
            if (_entryCount % _sampleInterval == 0)
                _index.Add((key, offset));

            Span<byte> word = stackalloc byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(word, key.Length);
            _stream.Write(word);
            _stream.Write(key);
            if (value is null)
            {
                _stream.WriteByte(FlagTombstone);
            }
            else
            {
                _stream.WriteByte(FlagValue);
                BinaryPrimitives.WriteInt32LittleEndian(word, value.Length);
                _stream.Write(word);
                _stream.Write(value);
            }

            _bloom.Add(key);
            _lastKey = key;
            _lastOffset = offset;
            _entryCount++;
        }

        /// <summary>Writes index, bloom filter, and footer; fsyncs; returns the entry count.</summary>
        public int Finish()
        {
            if (_finished)
                throw new InvalidOperationException("SSTable was already finished.");
            _finished = true;

            // The reader needs the exact largest key: make the final entry an
            // index entry too when sampling did not already land on it.
            if (_lastKey is not null &&
                (_index.Count == 0 || ByteArrayComparer.Instance.Compare(_index[^1].Key, _lastKey) != 0))
            {
                _index.Add((_lastKey, _lastOffset));
            }

            var indexOffset = _stream.Position;
            Span<byte> buffer = stackalloc byte[8];
            foreach (var (key, offset) in _index)
            {
                BinaryPrimitives.WriteInt32LittleEndian(buffer, key.Length);
                _stream.Write(buffer[..4]);
                _stream.Write(key);
                BinaryPrimitives.WriteInt64LittleEndian(buffer, offset);
                _stream.Write(buffer);
            }

            var bloomBytes = _bloom.ToBytes();
            var bloomOffset = _stream.Position;
            _stream.Write(bloomBytes);

            Span<byte> footer = stackalloc byte[FooterSize];
            BinaryPrimitives.WriteInt64LittleEndian(footer, indexOffset);
            BinaryPrimitives.WriteInt64LittleEndian(footer[8..], bloomOffset);
            BinaryPrimitives.WriteInt32LittleEndian(footer[16..], bloomBytes.Length);
            BinaryPrimitives.WriteInt32LittleEndian(footer[20..], _entryCount);
            Magic.CopyTo(footer[24..]);
            _stream.Write(footer);

            _stream.Flush(flushToDisk: true);
            return _entryCount;
        }

        public void Dispose() => _stream.Dispose();
    }

    internal sealed class Reader : IDisposable
    {
        private readonly FileStream _stream;
        private readonly object _gate = new();
        private readonly long _indexOffset;
        private readonly List<(byte[] Key, long Offset)> _index;
        private readonly BloomFilter _bloom;

        public long Id { get; }
        public string FilePath { get; }
        public int EntryCount { get; }
        public byte[] MinKey { get; }
        public byte[] MaxKey { get; }

        private Reader(long id, string path, FileStream stream, long indexOffset,
            List<(byte[] Key, long Offset)> index, BloomFilter bloom, int entryCount)
        {
            Id = id;
            FilePath = path;
            _stream = stream;
            _indexOffset = indexOffset;
            _index = index;
            _bloom = bloom;
            EntryCount = entryCount;
            MinKey = index[0].Key;
            MaxKey = index[^1].Key;
        }

        public static Reader Open(long id, string path)
        {
            var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize: 4096);
            try
            {
                if (stream.Length < FooterSize)
                    throw new StorageCorruptionException($"SSTable '{path}' is smaller than its footer.");
                stream.Seek(-FooterSize, SeekOrigin.End);
                var footer = new byte[FooterSize];
                stream.ReadExactly(footer);
                if (!footer.AsSpan(24).SequenceEqual(Magic))
                    throw new StorageCorruptionException($"SSTable '{path}' does not carry the LSMDB001 magic; it is not a table of this engine or it is damaged.");

                var indexOffset = BinaryPrimitives.ReadInt64LittleEndian(footer);
                var bloomOffset = BinaryPrimitives.ReadInt64LittleEndian(footer.AsSpan(8));
                var bloomLength = BinaryPrimitives.ReadInt32LittleEndian(footer.AsSpan(16));
                var entryCount = BinaryPrimitives.ReadInt32LittleEndian(footer.AsSpan(20));
                if (indexOffset < 0 || bloomOffset < indexOffset || bloomLength < 4 ||
                    bloomOffset + bloomLength != stream.Length - FooterSize || entryCount < 0)
                    throw new StorageCorruptionException($"SSTable '{path}' footer offsets are inconsistent.");

                var bloomBytes = new byte[bloomLength];
                stream.Seek(bloomOffset, SeekOrigin.Begin);
                stream.ReadExactly(bloomBytes);
                var bloom = BloomFilter.FromBytes(bloomBytes);

                var index = new List<(byte[] Key, long Offset)>();
                stream.Seek(indexOffset, SeekOrigin.Begin);
                var offsetBytes = new byte[8];
                while (stream.Position < bloomOffset)
                {
                    var key = ReadLengthPrefixed(stream);
                    stream.ReadExactly(offsetBytes);
                    index.Add((key, BinaryPrimitives.ReadInt64LittleEndian(offsetBytes)));
                }
                if (index.Count == 0)
                    throw new StorageCorruptionException($"SSTable '{path}' has an empty index.");

                return new Reader(id, path, stream, indexOffset, index, bloom, entryCount);
            }
            catch
            {
                stream.Dispose();
                throw;
            }
        }

        /// <summary>Returns true when the key exists in this table; <paramref name="value"/> is null for a tombstone.</summary>
        public bool TryGet(byte[] key, out byte[]? value)
        {
            value = null;
            if (EntryCount == 0)
                return false;
            if (ByteArrayComparer.Instance.Compare(key, MinKey) < 0 ||
                ByteArrayComparer.Instance.Compare(key, MaxKey) > 0)
                return false;
            if (!_bloom.MightContain(key))
                return false;

            // Binary search: the last sampled key <= target starts the scan.
            var lo = 0;
            var hi = _index.Count - 1;
            var start = 0;
            while (lo <= hi)
            {
                var mid = lo + (hi - lo) / 2;
                var cmp = ByteArrayComparer.Instance.Compare(_index[mid].Key, key);
                if (cmp == 0) { start = mid; break; }
                if (cmp < 0) { start = mid; lo = mid + 1; }
                else hi = mid - 1;
            }

            lock (_gate)
            {
                _stream.Seek(_index[start].Offset, SeekOrigin.Begin);
                while (_stream.Position < _indexOffset)
                {
                    var entryKey = ReadLengthPrefixed(_stream);
                    var cmp = ByteArrayComparer.Instance.Compare(entryKey, key);
                    if (cmp > 0)
                        return false;
                    var flag = _stream.ReadByte();
                    if (flag < 0)
                        throw new StorageCorruptionException($"SSTable '{FilePath}' ends inside an entry.");
                    byte[]? entryValue = null;
                    if (flag == FlagValue)
                        entryValue = ReadLengthPrefixed(_stream);
                    else if (flag != FlagTombstone)
                        throw new StorageCorruptionException($"SSTable '{FilePath}' carries unknown entry flag {flag}.");
                    if (cmp == 0)
                    {
                        value = entryValue;
                        return true;
                    }
                }
            }
            return false;
        }

        /// <summary>Streams every entry in ascending key order, tombstones included.</summary>
        public IEnumerable<KeyValuePair<byte[], byte[]?>> EnumerateAll()
        {
            // A dedicated stream: enumeration never fights point reads over a
            // shared seek position, and a snapshot scan keeps working even if
            // this reader has since been retired by a compaction.
            using var stream = new FileStream(FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize: 4096);
            while (stream.Position < _indexOffset)
            {
                var key = ReadLengthPrefixed(stream);
                var flag = stream.ReadByte();
                if (flag < 0)
                    throw new StorageCorruptionException($"SSTable '{FilePath}' ends inside an entry.");
                byte[]? value = null;
                if (flag == FlagValue)
                    value = ReadLengthPrefixed(stream);
                else if (flag != FlagTombstone)
                    throw new StorageCorruptionException($"SSTable '{FilePath}' carries unknown entry flag {flag}.");
                yield return new KeyValuePair<byte[], byte[]?>(key, value);
            }
        }

        public void Dispose()
        {
            lock (_gate)
                _stream.Dispose();
        }

        private static byte[] ReadLengthPrefixed(Stream stream)
        {
            Span<byte> lengthBytes = stackalloc byte[4];
            stream.ReadExactly(lengthBytes);
            var length = BinaryPrimitives.ReadInt32LittleEndian(lengthBytes);
            if (length < 0)
                throw new StorageCorruptionException("A length-prefixed field carries a negative length.");
            var bytes = new byte[length];
            stream.ReadExactly(bytes);
            return bytes;
        }
    }
}
