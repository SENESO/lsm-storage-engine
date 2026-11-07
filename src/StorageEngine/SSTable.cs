using System.Buffers.Binary;

namespace StorageEngine;

/// <summary>
/// One immutable sorted table file. Two format versions exist; the reader
/// accepts both, the writer writes v2.
///
/// v1 layout (magic "LSMDB001", 32-byte footer):
///   Data section — entries in ascending key order:
///     [keyLen i32][key][flags u8: 0 = value, 1 = tombstone]
///     [valueLen i32][value]              (omitted for tombstones)
///   Index section — sparse index: [keyLen i32][key][dataOffset i64]
///   Bloom section — [hashCount i32][bit bytes]
///   Footer — [indexOffset i64][bloomOffset i64][bloomLength i32]
///            [entryCount i32][magic 8 bytes]
///
/// v2 (magic "LSMDB002", 40-byte footer) extends v1:
///   - two more entry flags: 2 = value with expiry
///     ([expiresAtMs i64][valueLen i32][value] follow the flag), and
///     3 = range tombstone ([endLen i32][end] follows; the entry key is the
///     range start).
///   - a ranges section after the bloom section holding the table's range
///     tombstones as normalized (disjoint, start-sorted) intervals:
///     [count i32] then per interval [startLen i32][start][endLen i32][end].
///     It exists so a point lookup can answer "is this key covered by a
///     range tombstone?" without scanning the data section.
///   - the footer gains [rangeOffset i64] before the magic.
///
/// A point read checks coverage and the bloom filter (the filter only
/// under the built-in byte ordering — see the reader) before touching the
/// index, binary searches the sparse index, then scans forward from the
/// sampled offset — at most <see cref="StorageOptions.IndexSampleInterval"/>
/// entries. Within a table, at equal keys a point entry always precedes a
/// range tombstone that starts at the same key (producers emit in that
/// order); the reader does not rely on it — it skips range entries while
/// scanning for a point entry.
/// </summary>
internal static class SSTable
{
    internal const int FooterSizeV1 = 32;
    internal const int FooterSize = 40;
    private static readonly byte[] MagicV1 = "LSMDB001"u8.ToArray();
    private static readonly byte[] Magic = "LSMDB002"u8.ToArray();
    private const byte FlagValue = 0;
    private const byte FlagTombstone = 1;
    private const byte FlagValueWithExpiry = 2;
    private const byte FlagRangeTombstone = 3;

    internal static string FileName(long id) => $"sst-{id:D6}.lsm";
    internal static string TempFileName(long id) => $"sst-{id:D6}.tmp";

    /// <summary>
    /// Merges intervals (in any order, possibly overlapping or touching) into
    /// a disjoint, start-sorted list with the same union: [a,b) and [b,c)
    /// together cover exactly [a,c).
    /// </summary>
    internal static List<(byte[] Start, byte[] End)> NormalizeIntervals(
        IEnumerable<(byte[] Start, byte[] End)> intervals, IComparer<byte[]> comparer)
    {
        var sorted = intervals.ToList();
        sorted.Sort((x, y) =>
        {
            var byStart = comparer.Compare(x.Start, y.Start);
            return byStart != 0 ? byStart : comparer.Compare(x.End, y.End);
        });
        var merged = new List<(byte[] Start, byte[] End)>();
        foreach (var (start, end) in sorted)
        {
            if (merged.Count > 0 && comparer.Compare(start, merged[^1].End) <= 0)
            {
                if (comparer.Compare(end, merged[^1].End) > 0)
                    merged[^1] = (merged[^1].Start, end);
            }
            else
            {
                merged.Add((start, end));
            }
        }
        return merged;
    }

    internal sealed class Writer : IDisposable
    {
        private readonly FileStream _stream;
        private readonly int _sampleInterval;
        private readonly IComparer<byte[]> _comparer;
        private readonly List<(byte[] Key, long Offset)> _index = new();
        private readonly List<(byte[] Start, byte[] End)> _intervals = new();
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
            _comparer = options.Comparer;
            _bloom = BloomFilter.Create(Math.Max(expectedEntries, 1), options.BloomBitsPerKey);
        }

        public int EntryCount => _entryCount;

        /// <summary>Bytes written so far (the stream position); used by compaction output splitting.</summary>
        public long BytesWritten => _stream.Position;

        /// <summary>
        /// Entries must be added in non-descending key order under the
        /// database comparer (equal keys are allowed: a point entry and a
        /// range tombstone starting at the same key, point first).
        /// </summary>
        public void Add(StoreEntry entry)
        {
            if (_finished)
                throw new InvalidOperationException("Cannot add to a finished SSTable.");
            if (_lastKey is not null && _comparer.Compare(entry.Key, _lastKey) < 0)
                throw new InvalidOperationException("SSTable entries must be added in non-descending key order.");

            var key = entry.Key;
            var offset = _stream.Position;
            if (_entryCount % _sampleInterval == 0)
                _index.Add((key, offset));

            Span<byte> word = stackalloc byte[8];
            BinaryPrimitives.WriteInt32LittleEndian(word, key.Length);
            _stream.Write(word[..4]);
            _stream.Write(key);
            switch (entry.Kind)
            {
                case EntryKind.Tombstone:
                    _stream.WriteByte(FlagTombstone);
                    break;
                case EntryKind.RangeTombstone:
                    _stream.WriteByte(FlagRangeTombstone);
                    BinaryPrimitives.WriteInt32LittleEndian(word, entry.RangeEnd!.Length);
                    _stream.Write(word[..4]);
                    _stream.Write(entry.RangeEnd);
                    _intervals.Add((key, entry.RangeEnd));
                    break;
                case EntryKind.Value when entry.ExpiresAtMs != 0:
                    _stream.WriteByte(FlagValueWithExpiry);
                    BinaryPrimitives.WriteInt64LittleEndian(word, entry.ExpiresAtMs);
                    _stream.Write(word);
                    BinaryPrimitives.WriteInt32LittleEndian(word, entry.Value!.Length);
                    _stream.Write(word[..4]);
                    _stream.Write(entry.Value);
                    break;
                case EntryKind.Value:
                    _stream.WriteByte(FlagValue);
                    BinaryPrimitives.WriteInt32LittleEndian(word, entry.Value!.Length);
                    _stream.Write(word[..4]);
                    _stream.Write(entry.Value);
                    break;
            }

            _bloom.Add(key);
            _lastKey = key;
            _lastOffset = offset;
            _entryCount++;
        }

        /// <summary>Convenience for a plain point entry (a null value writes a tombstone).</summary>
        public void Add(byte[] key, byte[]? value) =>
            Add(value is null ? StoreEntry.Delete(key) : StoreEntry.Put(key, value));

        /// <summary>Writes index, bloom filter, ranges, and footer; fsyncs; returns the entry count.</summary>
        public int Finish()
        {
            if (_finished)
                throw new InvalidOperationException("SSTable was already finished.");
            _finished = true;

            // The reader needs the exact largest key: make the final entry an
            // index entry too when sampling did not already land on it.
            if (_lastKey is not null &&
                (_index.Count == 0 || _comparer.Compare(_index[^1].Key, _lastKey) != 0))
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

            var rangeOffset = _stream.Position;
            var intervals = NormalizeIntervals(_intervals, _comparer);
            BinaryPrimitives.WriteInt32LittleEndian(buffer, intervals.Count);
            _stream.Write(buffer[..4]);
            foreach (var (start, end) in intervals)
            {
                BinaryPrimitives.WriteInt32LittleEndian(buffer, start.Length);
                _stream.Write(buffer[..4]);
                _stream.Write(start);
                BinaryPrimitives.WriteInt32LittleEndian(buffer, end.Length);
                _stream.Write(buffer[..4]);
                _stream.Write(end);
            }

            Span<byte> footer = stackalloc byte[FooterSize];
            BinaryPrimitives.WriteInt64LittleEndian(footer, indexOffset);
            BinaryPrimitives.WriteInt64LittleEndian(footer[8..], bloomOffset);
            BinaryPrimitives.WriteInt32LittleEndian(footer[16..], bloomBytes.Length);
            BinaryPrimitives.WriteInt32LittleEndian(footer[20..], _entryCount);
            BinaryPrimitives.WriteInt64LittleEndian(footer[24..], rangeOffset);
            Magic.CopyTo(footer[32..]);
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
        private readonly IComparer<byte[]> _comparer;
        private readonly bool _useBloom;
        private readonly List<(byte[] Key, long Offset)> _index;
        private readonly BloomFilter _bloom;
        private readonly List<(byte[] Start, byte[] End)> _intervals;

        public long Id { get; }
        public string FilePath { get; }
        public int EntryCount { get; }
        public byte[] MinKey { get; }
        public byte[] MaxKey { get; }

        /// <summary>The largest key this table can affect: the maximum of MaxKey and every range tombstone's end.</summary>
        public byte[] MaxCoveredKey { get; }

        /// <summary>The table's file size in bytes.</summary>
        public long SizeBytes { get; }

        /// <summary>The compaction level this table belongs to (from the manifest; 0 for tiered use).</summary>
        public int Level { get; internal set; }

        /// <summary>The table's range tombstones as normalized, disjoint, start-sorted intervals.</summary>
        public IReadOnlyList<(byte[] Start, byte[] End)> Intervals => _intervals;

        private Reader(long id, string path, FileStream stream, long indexOffset,
            IComparer<byte[]> comparer, List<(byte[] Key, long Offset)> index, BloomFilter bloom,
            List<(byte[] Start, byte[] End)> intervals, int entryCount, long sizeBytes)
        {
            Id = id;
            FilePath = path;
            _stream = stream;
            _indexOffset = indexOffset;
            _comparer = comparer;
            // The bloom filter is keyed on exact key bytes: it was fed the
            // stored byte form of each key. Under the built-in byte ordering,
            // comparer-equality IS byte equality, so a bloom miss proves
            // absence. A custom comparer may equate distinct byte strings
            // (case-insensitive orderings do), and then a lookup key that
            // compares equal to a stored key can hash differently — a bloom
            // miss would be a false negative. So under a custom comparer the
            // filter is simply not consulted: the index walk alone decides.
            _useBloom = comparer is ByteArrayComparer;
            _index = index;
            _bloom = bloom;
            _intervals = intervals;
            EntryCount = entryCount;
            SizeBytes = sizeBytes;
            MinKey = index[0].Key;
            MaxKey = index[^1].Key;
            var covered = MaxKey;
            foreach (var (_, end) in intervals)
            {
                if (comparer.Compare(end, covered) > 0)
                    covered = end;
            }
            MaxCoveredKey = covered;
        }

        public static Reader Open(long id, string path, IComparer<byte[]>? comparer = null)
        {
            comparer ??= ByteArrayComparer.Instance;
            var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize: 4096);
            try
            {
                var length = stream.Length;
                long indexOffset, bloomOffset, rangeOffset = -1;
                int bloomLength, entryCount;
                var isV2 = false;

                if (length >= FooterSize)
                {
                    var footer = new byte[FooterSize];
                    stream.Seek(-FooterSize, SeekOrigin.End);
                    stream.ReadExactly(footer);
                    if (footer.AsSpan(32).SequenceEqual(Magic))
                    {
                        isV2 = true;
                        indexOffset = BinaryPrimitives.ReadInt64LittleEndian(footer);
                        bloomOffset = BinaryPrimitives.ReadInt64LittleEndian(footer.AsSpan(8));
                        bloomLength = BinaryPrimitives.ReadInt32LittleEndian(footer.AsSpan(16));
                        entryCount = BinaryPrimitives.ReadInt32LittleEndian(footer.AsSpan(20));
                        rangeOffset = BinaryPrimitives.ReadInt64LittleEndian(footer.AsSpan(24));
                    }
                    else
                    {
                        (indexOffset, bloomOffset, bloomLength, entryCount) = ReadV1Footer(stream, path);
                    }
                }
                else if (length >= FooterSizeV1)
                {
                    (indexOffset, bloomOffset, bloomLength, entryCount) = ReadV1Footer(stream, path);
                }
                else
                {
                    throw new StorageCorruptionException($"SSTable '{path}' is smaller than its footer.");
                }

                var dataEnd = length - (isV2 ? FooterSize : FooterSizeV1);
                if (indexOffset < 0 || bloomOffset < indexOffset || bloomLength < 4 ||
                    bloomOffset + bloomLength > dataEnd || entryCount < 0 ||
                    (isV2 && (rangeOffset != bloomOffset + bloomLength || rangeOffset > dataEnd)))
                    throw new StorageCorruptionException($"SSTable '{path}' footer offsets are inconsistent.");

                var bloomBytes = new byte[bloomLength];
                stream.Seek(bloomOffset, SeekOrigin.Begin);
                stream.ReadExactly(bloomBytes);
                var bloom = BloomFilter.FromBytes(bloomBytes);

                var intervals = new List<(byte[] Start, byte[] End)>();
                if (isV2)
                {
                    stream.Seek(rangeOffset, SeekOrigin.Begin);
                    var countBytes = new byte[4];
                    stream.ReadExactly(countBytes);
                    var count = BinaryPrimitives.ReadInt32LittleEndian(countBytes);
                    if (count < 0)
                        throw new StorageCorruptionException($"SSTable '{path}' carries a negative range-tombstone count.");
                    for (var i = 0; i < count; i++)
                    {
                        var start = ReadLengthPrefixed(stream);
                        var end = ReadLengthPrefixed(stream);
                        intervals.Add((start, end));
                    }
                    if (stream.Position != dataEnd)
                        throw new StorageCorruptionException($"SSTable '{path}' ranges section does not end where the footer begins.");
                }

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

                return new Reader(id, path, stream, indexOffset, comparer, index, bloom, intervals, entryCount, length);
            }
            catch
            {
                stream.Dispose();
                throw;
            }
        }

        private static (long IndexOffset, long BloomOffset, int BloomLength, int EntryCount) ReadV1Footer(
            FileStream stream, string path)
        {
            stream.Seek(-FooterSizeV1, SeekOrigin.End);
            var footer = new byte[FooterSizeV1];
            stream.ReadExactly(footer);
            if (!footer.AsSpan(24).SequenceEqual(MagicV1))
                throw new StorageCorruptionException($"SSTable '{path}' carries neither the LSMDB001 nor the LSMDB002 magic; it is not a table of this engine or it is damaged.");
            return (
                BinaryPrimitives.ReadInt64LittleEndian(footer),
                BinaryPrimitives.ReadInt64LittleEndian(footer.AsSpan(8)),
                BinaryPrimitives.ReadInt32LittleEndian(footer.AsSpan(16)),
                BinaryPrimitives.ReadInt32LittleEndian(footer.AsSpan(20)));
        }

        /// <summary>True when one of this table's range tombstones covers <paramref name="key"/>.</summary>
        public bool CoversRange(byte[] key)
        {
            // Intervals are disjoint and start-sorted: the only candidate is
            // the last interval whose start is at or before the key.
            var lo = 0;
            var hi = _intervals.Count - 1;
            var candidate = -1;
            while (lo <= hi)
            {
                var mid = lo + (hi - lo) / 2;
                if (_comparer.Compare(_intervals[mid].Start, key) <= 0)
                {
                    candidate = mid;
                    lo = mid + 1;
                }
                else
                {
                    hi = mid - 1;
                }
            }
            return candidate >= 0 && _comparer.Compare(key, _intervals[candidate].End) < 0;
        }

        /// <summary>
        /// Resolves <paramref name="key"/> in this table. Returns true when the
        /// table has a verdict for the key: a point entry (value, value with
        /// expiry, or tombstone), or — with no point entry — a covering range
        /// tombstone (entry kind RangeTombstone: the key is deleted here).
        /// Returns false when the table says nothing about the key.
        /// </summary>
        public bool TryGet(byte[] key, out StoreEntry entry)
        {
            entry = default;
            if (EntryCount == 0)
                return false;
            if (_comparer.Compare(key, MinKey) < 0)
                return false;

            if (TryGetPoint(key, out entry))
                return true;
            if (CoversRange(key))
            {
                entry = StoreEntry.CoveredByRange(key);
                return true;
            }
            return false;
        }

        private bool TryGetPoint(byte[] key, out StoreEntry entry)
        {
            entry = default;
            // The MaxKey prune is unsound when range tombstones exist: a key
            // past MaxKey has no point entry by definition, so skipping the
            // prune only costs the index walk below.
            if (_intervals.Count == 0 && _comparer.Compare(key, MaxKey) > 0)
                return false;
            if (_useBloom && !_bloom.MightContain(key))
                return false;

            // Binary search: the last sampled key <= target starts the scan.
            var lo = 0;
            var hi = _index.Count - 1;
            var start = 0;
            while (lo <= hi)
            {
                var mid = lo + (hi - lo) / 2;
                var cmp = _comparer.Compare(_index[mid].Key, key);
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
                    var cmp = _comparer.Compare(entryKey, key);
                    if (cmp > 0)
                        return false;
                    var flag = _stream.ReadByte();
                    if (flag < 0)
                        throw new StorageCorruptionException($"SSTable '{FilePath}' ends inside an entry.");
                    switch (flag)
                    {
                        case FlagValue:
                        {
                            var value = ReadLengthPrefixed(_stream);
                            if (cmp == 0)
                            {
                                entry = StoreEntry.Put(entryKey, value);
                                return true;
                            }
                            break;
                        }
                        case FlagValueWithExpiry:
                        {
                            var expiryBytes = new byte[8];
                            _stream.ReadExactly(expiryBytes);
                            var expiresAtMs = BinaryPrimitives.ReadInt64LittleEndian(expiryBytes);
                            var value = ReadLengthPrefixed(_stream);
                            if (cmp == 0)
                            {
                                entry = StoreEntry.Put(entryKey, value, expiresAtMs);
                                return true;
                            }
                            break;
                        }
                        case FlagTombstone:
                            if (cmp == 0)
                            {
                                entry = StoreEntry.Delete(entryKey);
                                return true;
                            }
                            break;
                        case FlagRangeTombstone:
                            // Not a point verdict: parse past it and keep scanning.
                            _ = ReadLengthPrefixed(_stream);
                            break;
                        default:
                            throw new StorageCorruptionException($"SSTable '{FilePath}' carries unknown entry flag {flag}.");
                    }
                }
            }
            return false;
        }

        /// <summary>Streams every entry in ascending key order — values, tombstones, and range tombstones.</summary>
        public IEnumerable<StoreEntry> EnumerateAll()
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
                switch (flag)
                {
                    case FlagValue:
                        yield return StoreEntry.Put(key, ReadLengthPrefixed(stream));
                        break;
                    case FlagValueWithExpiry:
                    {
                        var expiryBytes = new byte[8];
                        stream.ReadExactly(expiryBytes);
                        var expiresAtMs = BinaryPrimitives.ReadInt64LittleEndian(expiryBytes);
                        yield return StoreEntry.Put(key, ReadLengthPrefixed(stream), expiresAtMs);
                        break;
                    }
                    case FlagTombstone:
                        yield return StoreEntry.Delete(key);
                        break;
                    case FlagRangeTombstone:
                        yield return StoreEntry.Range(key, ReadLengthPrefixed(stream));
                        break;
                    default:
                        throw new StorageCorruptionException($"SSTable '{FilePath}' carries unknown entry flag {flag}.");
                }
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
