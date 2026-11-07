using System.Buffers.Binary;
using static StorageEngine.Tests.TestSupport;

namespace StorageEngine.Tests;

/// <summary>
/// Backward compatibility with the previous on-disk generation: a table
/// file in the ORIGINAL SSTable format (magic LSMDB001, 32-byte footer,
/// value/tombstone flags only), referenced by a manifest whose sst lines
/// carry no level, must open and read correctly alongside new-format
/// tables, and merge cleanly with them.
/// </summary>
public class FormatCompatibilityTests : DbTestBase
{
    /// <summary>
    /// Builds a table file byte-for-byte the way the previous engine
    /// generation wrote it: data entries [keyLen][key][flag][valueLen][value],
    /// sparse index (first + last key pinned), raw bloom bytes, then the
    /// 32-byte footer [indexOffset][bloomOffset][bloomLength][entryCount]
    /// [magic LSMDB001].
    /// </summary>
    private string WriteV1Table(long id, params (string Key, string? Value)[] entries)
    {
        var path = Path.Combine(Dir, $"sst-{id:D6}.lsm");
        var bloom = BloomFilter.Create(Math.Max(entries.Length, 1), 10);
        var index = new List<(byte[] Key, long Offset)>();
        using (var stream = new FileStream(path, FileMode.Create, FileAccess.Write))
        {
            void WriteInt32(int value)
            {
                Span<byte> b = stackalloc byte[4];
                BinaryPrimitives.WriteInt32LittleEndian(b, value);
                stream.Write(b);
            }
            void WriteInt64(long value)
            {
                Span<byte> b = stackalloc byte[8];
                BinaryPrimitives.WriteInt64LittleEndian(b, value);
                stream.Write(b);
            }

            long lastOffset = 0;
            byte[]? lastKey = null;
            for (var i = 0; i < entries.Length; i++)
            {
                var (key, value) = entries[i];
                var keyBytes = B(key);
                var offset = stream.Position;
                if (i % 16 == 0)
                    index.Add((keyBytes, offset));
                WriteInt32(keyBytes.Length);
                stream.Write(keyBytes);
                if (value is null)
                {
                    stream.WriteByte(1); // tombstone
                }
                else
                {
                    stream.WriteByte(0); // value
                    var valueBytes = B(value);
                    WriteInt32(valueBytes.Length);
                    stream.Write(valueBytes);
                }
                bloom.Add(keyBytes);
                lastKey = keyBytes;
                lastOffset = offset;
            }
            if (lastKey is not null &&
                (index.Count == 0 || !index[^1].Key.AsSpan().SequenceEqual(lastKey)))
                index.Add((lastKey, lastOffset));

            var indexOffset = stream.Position;
            foreach (var (key, offset) in index)
            {
                WriteInt32(key.Length);
                stream.Write(key);
                WriteInt64(offset);
            }

            var bloomBytes = bloom.ToBytes();
            var bloomOffset = stream.Position;
            stream.Write(bloomBytes);

            Span<byte> footer = stackalloc byte[32];
            BinaryPrimitives.WriteInt64LittleEndian(footer, indexOffset);
            BinaryPrimitives.WriteInt64LittleEndian(footer[8..], bloomOffset);
            BinaryPrimitives.WriteInt32LittleEndian(footer[16..], bloomBytes.Length);
            BinaryPrimitives.WriteInt32LittleEndian(footer[20..], entries.Length);
            "LSMDB001"u8.CopyTo(footer[24..]);
            stream.Write(footer);
            stream.Flush(flushToDisk: true);
        }
        return path;
    }

    private void WriteManifest(params string[] lines)
    {
        var all = new List<string> { "format 2", "comparator bytewise", "next-sst-id 100", "wal-gen 1" };
        all.AddRange(lines);
        File.WriteAllLines(Path.Combine(Dir, "MANIFEST"), all);
        // Generation 1 is listed, so its file must exist (an empty WAL is a
        // valid WAL: the format is headerless framed records).
        File.WriteAllBytes(Path.Combine(Dir, "wal-1.log"), Array.Empty<byte>());
    }

    [Fact]
    public void V1Table_OpensReadsAndHonorsItsTombstones()
    {
        WriteV1Table(1, ("a", "va"), ("b", null), ("c", "vc"));
        WriteManifest("sst 1"); // no level field: the pre-levels grammar

        using var db = LsmDb.Open(Options());
        Assert.Equal("va", S(db.Get(B("a"))!));
        Assert.Null(db.Get(B("b"))); // the v1 tombstone is honored
        Assert.Equal("vc", S(db.Get(B("c"))!));
        Assert.Null(db.Get(B("zz")));
        Assert.Equal(new[] { "a", "c" }, db.Scan().Select(kv => S(kv.Key)).ToArray());
    }

    [Fact]
    public void V1Table_MergesWithNewTables_ThroughFlushAndCompaction()
    {
        WriteV1Table(1, ("a", "va"), ("b", null), ("c", "vc"));
        WriteManifest("sst 1");

        using (var db = LsmDb.Open(Options()))
        {
            db.Put(B("b"), B("revived"));
            db.Put(B("d"), B("vd"));
            db.Flush(); // a new-format table alongside the v1 one
            Assert.Equal("revived", S(db.Get(B("b"))!));
            db.Compact(); // the merge reads the v1 table through the same path
            Assert.Equal("va", S(db.Get(B("a"))!));
            Assert.Equal("revived", S(db.Get(B("b"))!));
            Assert.Equal("vc", S(db.Get(B("c"))!));
            Assert.Equal("vd", S(db.Get(B("d"))!));
        }
        using (var db = LsmDb.Open(Options()))
        {
            Assert.Equal(new[] { "a", "b", "c", "d" },
                db.Scan().Select(kv => S(kv.Key)).ToArray());
        }
    }
}
