using System.Globalization;

namespace StorageEngine;

/// <summary>
/// The manifest is the single source of truth for which files are live: the set
/// of SSTable ids and the current WAL generation. It is a small text file
/// replaced atomically (write temp file, fsync, rename over the old one), so a
/// crash leaves either the old or the new manifest — never a half-written one.
/// Files on disk that the manifest does not name are orphans of interrupted
/// operations and are removed on open.
/// </summary>
internal sealed class Manifest
{
    public const string FileName = "MANIFEST";
    internal const string TempFileName = "MANIFEST.tmp";

    public List<long> SstIds { get; } = new();
    public long NextSstId { get; set; } = 1;
    public long WalGeneration { get; set; } = 1;

    public static Manifest Load(string directory)
    {
        var path = Path.Combine(directory, FileName);
        if (!File.Exists(path))
            return new Manifest();

        var manifest = new Manifest();
        foreach (var rawLine in File.ReadAllLines(path))
        {
            var line = rawLine.Trim();
            if (line.Length == 0 || line.StartsWith('#'))
                continue;
            var parts = line.Split(' ', 2);
            if (parts.Length != 2)
                throw new StorageCorruptionException($"Manifest line is malformed: '{line}'.");
            var value = long.Parse(parts[1], CultureInfo.InvariantCulture);
            switch (parts[0])
            {
                case "next-sst-id": manifest.NextSstId = value; break;
                case "wal-gen": manifest.WalGeneration = value; break;
                case "sst": manifest.SstIds.Add(value); break;
                default: throw new StorageCorruptionException($"Manifest carries an unknown entry '{parts[0]}'.");
            }
        }
        return manifest;
    }

    public void Save(string directory)
    {
        var tempPath = Path.Combine(directory, TempFileName);
        var finalPath = Path.Combine(directory, FileName);
        using (var stream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
        using (var writer = new StreamWriter(stream))
        {
            writer.WriteLine("# LSM storage engine manifest. Replaced atomically; do not edit while a database is open.");
            writer.WriteLine($"next-sst-id {NextSstId}");
            writer.WriteLine($"wal-gen {WalGeneration}");
            foreach (var id in SstIds)
                writer.WriteLine($"sst {id}");
            writer.Flush();
            stream.Flush(flushToDisk: true);
        }
        File.Move(tempPath, finalPath, overwrite: true);
    }
}
