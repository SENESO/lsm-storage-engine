namespace StorageEngine;

/// <summary>
/// A classic bloom filter over byte-array keys. Membership is probabilistic in
/// one direction only: a key that was added always reports present (no false
/// negatives); a key that was never added reports present with a small,
/// tunable probability. Two independent 32-bit hashes (CRC-32 and FNV-1a) are
/// combined by double hashing to derive the k probe positions.
/// </summary>
internal sealed class BloomFilter
{
    private readonly byte[] _bits;
    private readonly int _hashCount;

    private BloomFilter(byte[] bits, int hashCount)
    {
        _bits = bits;
        _hashCount = hashCount;
    }

    public int BitCount => _bits.Length * 8;

    public static BloomFilter Create(int expectedEntries, int bitsPerKey)
    {
        var bitCount = Math.Max(64, expectedEntries * bitsPerKey);
        // Round up to a whole number of bytes.
        var byteCount = (bitCount + 7) / 8;
        var hashCount = Math.Max(1, (int)Math.Round(bitsPerKey * Math.Log(2)));
        return new BloomFilter(new byte[byteCount], hashCount);
    }

    public void Add(ReadOnlySpan<byte> key)
    {
        var (h1, h2) = Hashes(key);
        var bitCount = BitCount;
        for (var i = 0; i < _hashCount; i++)
        {
            var pos = (int)((h1 + (uint)i * h2) % (uint)bitCount);
            _bits[pos >> 3] |= (byte)(1 << (pos & 7));
        }
    }

    public bool MightContain(ReadOnlySpan<byte> key)
    {
        var (h1, h2) = Hashes(key);
        var bitCount = BitCount;
        for (var i = 0; i < _hashCount; i++)
        {
            var pos = (int)((h1 + (uint)i * h2) % (uint)bitCount);
            if ((_bits[pos >> 3] & (1 << (pos & 7))) == 0)
                return false;
        }
        return true;
    }

    /// <summary>Serializes as [hashCount int32][bit bytes].</summary>
    public byte[] ToBytes()
    {
        var result = new byte[4 + _bits.Length];
        BitConverter.TryWriteBytes(result.AsSpan(0, 4), _hashCount);
        _bits.CopyTo(result.AsSpan(4));
        return result;
    }

    public static BloomFilter FromBytes(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length < 4)
            throw new StorageCorruptionException("Bloom filter block is too small to contain its header.");
        var hashCount = BitConverter.ToInt32(bytes[..4]);
        if (hashCount <= 0 || hashCount > 64)
            throw new StorageCorruptionException($"Bloom filter header carries an implausible hash count ({hashCount}).");
        return new BloomFilter(bytes[4..].ToArray(), hashCount);
    }

    private static (uint H1, uint H2) Hashes(ReadOnlySpan<byte> key)
    {
        var h1 = Crc32.Compute(key);
        // FNV-1a 32-bit, forced odd and non-zero so the double-hashing stride
        // can reach every bit position.
        var h2 = 2166136261u;
        foreach (var b in key)
        {
            h2 ^= b;
            h2 *= 16777619u;
        }
        h2 |= 1u;
        return (h1, h2);
    }
}
