namespace StorageEngine;

/// <summary>
/// Table-based CRC-32 (IEEE 802.3, polynomial 0xEDB88320) — the same checksum
/// used by zip and PNG. Implemented by hand so the library stays dependency-free.
/// </summary>
internal static class Crc32
{
    private const uint Polynomial = 0xEDB88320u;
    private static readonly uint[] Table = BuildTable();

    private static uint[] BuildTable()
    {
        var table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            var crc = i;
            for (var bit = 0; bit < 8; bit++)
                crc = (crc & 1) != 0 ? (crc >> 1) ^ Polynomial : crc >> 1;
            table[i] = crc;
        }
        return table;
    }

    public static uint Compute(ReadOnlySpan<byte> data)
    {
        var crc = 0xFFFFFFFFu;
        foreach (var b in data)
            crc = Table[(crc ^ b) & 0xFF] ^ (crc >> 8);
        return crc ^ 0xFFFFFFFFu;
    }
}
