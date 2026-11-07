namespace StorageEngine;

/// <summary>
/// Lexicographic ordering over raw bytes (unsigned, byte-by-byte) — the single
/// key ordering used by the memtable, the SSTables, and every merge in the engine.
/// </summary>
public sealed class ByteArrayComparer : IComparer<byte[]>, IEqualityComparer<byte[]>
{
    public static readonly ByteArrayComparer Instance = new();

    private ByteArrayComparer()
    {
    }

    public int Compare(byte[]? x, byte[]? y)
    {
        if (ReferenceEquals(x, y)) return 0;
        if (x is null) return -1;
        if (y is null) return 1;
        return x.AsSpan().SequenceCompareTo(y.AsSpan());
    }

    public bool Equals(byte[]? x, byte[]? y) => Compare(x, y) == 0;

    public int GetHashCode(byte[] obj)
    {
        // FNV-1a 32-bit.
        var hash = 2166136261u;
        foreach (var b in obj)
        {
            hash ^= b;
            hash *= 16777619u;
        }
        return (int)hash;
    }
}
