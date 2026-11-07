using System.Globalization;

namespace StorageEngine;

/// <summary>
/// The manifest is the single source of truth for which files are live: the set
/// of SSTables (each with its compaction level), the list of live WAL
/// generations, and — while a background flush is in flight — the pending-flush
/// slot naming the generation being flushed and the table id reserved for its
/// output. It also records the identity of the database's key comparer. It is
/// a small text file replaced atomically (write temp file, fsync, rename over
/// the old one), so a crash leaves either the old or the new manifest — never
/// a half-written one. Files on disk that the manifest does not name are
/// orphans of interrupted operations and are removed on open.
///
/// Format v2 (what <see cref="Save"/> writes):
///   format 2
///   comparator &lt;name&gt;        the key comparer's identity ("bytewise"
///                                unless a custom comparer was supplied)
///   next-sst-id &lt;n&gt;
///   wal-gen &lt;g&gt;            one line per live generation, oldest first;
///                                the last one is the append target
///   pending-flush &lt;g&gt; &lt;id&gt;   optional, at most one: generation g is being
///                                flushed into table id
///   sst &lt;id&gt; &lt;level&gt;       one line per live table, with its
///                                compaction level
///
/// The v2 grammar is extended compatibly rather than version-bumped: the
/// comparator line is optional (absent means "bytewise" — every pre-existing
/// directory), and an sst line's level is optional (absent means level 0).
/// Every v1 or v2 file written before these fields existed therefore still
/// loads with identical meaning, and the first save after open rewrites the
/// file with the fields present. (A pre-existing engine binary, whose loader
/// rejects unknown lines, cannot open a directory once the new fields are
/// written — the extension is backward-compatible for reading, as the format
/// contract has always required, not for downgrading.)
///
/// A file without a "format" line is format v1 (the original layout: exactly
/// one "wal-gen" line, no pending slot, no comparator, sst lines without
/// levels). V1 manifests load as a one-element generation list and are
/// upgraded in place: the first save after open rewrites the file as v2 with
/// identical meaning.
/// </summary>
internal sealed class Manifest
{
    public const string FileName = "MANIFEST";
    internal const string TempFileName = "MANIFEST.tmp";

    /// <summary>Live tables with their compaction levels, in adoption order.</summary>
    public List<(long Id, int Level)> Tables { get; } = new();

    public long NextSstId { get; set; } = 1;

    /// <summary>
    /// The recorded identity of the database's key comparer. Opening refuses
    /// when the name supplied in the options differs: the on-disk order was
    /// laid down under this ordering and is meaningless under another.
    /// </summary>
    public string ComparerName { get; set; } = "bytewise";

    /// <summary>
    /// Live WAL generations, oldest first. Every generation in this list holds
    /// records that are not yet in any manifest table; recovery replays them
    /// in list order. The last entry is the generation being appended to.
    /// </summary>
    public List<long> WalGenerations { get; } = new() { 1 };

    /// <summary>The current (append-target) WAL generation: the last live generation.</summary>
    public long WalGeneration => WalGenerations[^1];

    /// <summary>
    /// The in-flight background flush, if any: the generation whose records are
    /// being written into the reserved table id. The generation stays in
    /// <see cref="WalGenerations"/> until the flush commits; the table id joins
    /// <see cref="Tables"/> only at that same commit.
    /// </summary>
    public (long Generation, long TableId)? PendingFlush { get; set; }

    /// <summary>True when the file on disk was format v1 (or absent) and this instance has not been saved yet.</summary>
    public bool LoadedAsVersion1 { get; private set; }

    public bool HasTable(long id) => Tables.Any(t => t.Id == id);

    public static Manifest Load(string directory)
    {
        var path = Path.Combine(directory, FileName);
        if (!File.Exists(path))
        {
            return new Manifest { LoadedAsVersion1 = true };
        }

        var manifest = new Manifest();
        var generations = new List<long>();
        var format = 1;
        var sawGeneration = false;
        foreach (var rawLine in File.ReadAllLines(path))
        {
            var line = rawLine.Trim();
            if (line.Length == 0 || line.StartsWith('#'))
                continue;
            var parts = line.Split(' ');
            switch (parts[0])
            {
                case "format":
                    var version = ParseValue(parts, line);
                    if (version is not (1 or 2))
                        throw new StorageCorruptionException($"Manifest carries unsupported format version {version}.");
                    format = (int)version;
                    manifest.LoadedAsVersion1 = format == 1;
                    break;
                case "comparator":
                    if (parts.Length != 2 || parts[1].Length == 0)
                        throw new StorageCorruptionException($"Manifest line is malformed: '{line}'.");
                    manifest.ComparerName = parts[1];
                    break;
                case "next-sst-id":
                    manifest.NextSstId = ParseValue(parts, line);
                    break;
                case "wal-gen":
                    generations.Add(ParseValue(parts, line));
                    sawGeneration = true;
                    break;
                case "pending-flush":
                    if (parts.Length != 3 ||
                        !long.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var pendingGen) ||
                        !long.TryParse(parts[2], NumberStyles.Integer, CultureInfo.InvariantCulture, out var pendingTable))
                        throw new StorageCorruptionException($"Manifest line is malformed: '{line}'.");
                    if (manifest.PendingFlush is not null)
                        throw new StorageCorruptionException("Manifest carries more than one pending-flush entry.");
                    manifest.PendingFlush = (pendingGen, pendingTable);
                    break;
                case "sst":
                    if (parts.Length is < 2 or > 3 ||
                        !long.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var sstId))
                        throw new StorageCorruptionException($"Manifest line is malformed: '{line}'.");
                    var level = 0;
                    if (parts.Length == 3 &&
                        !int.TryParse(parts[2], NumberStyles.Integer, CultureInfo.InvariantCulture, out level))
                        throw new StorageCorruptionException($"Manifest line is malformed: '{line}'.");
                    manifest.Tables.Add((sstId, level));
                    break;
                default:
                    throw new StorageCorruptionException($"Manifest carries an unknown entry '{parts[0]}'.");
            }
        }

        if (sawGeneration)
        {
            manifest.WalGenerations.Clear();
            manifest.WalGenerations.AddRange(generations);
        }
        // No "format" line at all means format 1 — the common v1 case.
        manifest.LoadedAsVersion1 = format == 1;
        ValidateLoaded(manifest, format);
        return manifest;
    }

    private static long ParseValue(string[] parts, string line)
    {
        if (parts.Length != 2 ||
            !long.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
            throw new StorageCorruptionException($"Manifest line is malformed: '{line}'.");
        return value;
    }

    /// <summary>
    /// Cross-field invariants the writer maintains. A manifest violating them
    /// was not produced by a crash of this engine (every engine state satisfies
    /// them), so it is damage, and opening refuses loudly.
    /// </summary>
    private static void ValidateLoaded(Manifest manifest, int format)
    {
        var generations = manifest.WalGenerations;
        if (generations.Count == 0)
            throw new StorageCorruptionException("Manifest lists no live WAL generation.");
        for (var i = 1; i < generations.Count; i++)
        {
            if (generations[i] <= generations[i - 1])
                throw new StorageCorruptionException("Manifest WAL generations are not strictly increasing.");
        }
        foreach (var (id, level) in manifest.Tables)
        {
            if (level < 0)
                throw new StorageCorruptionException($"Manifest assigns table {id} a negative level ({level}).");
        }
        if (manifest.PendingFlush is { } pending)
        {
            if (format == 1)
                throw new StorageCorruptionException("A format v1 manifest cannot carry a pending-flush entry.");
            if (!generations.Contains(pending.Generation))
                throw new StorageCorruptionException(
                    $"Manifest pending-flush names WAL generation {pending.Generation}, which is not a live generation.");
            if (pending.Generation == manifest.WalGeneration)
                throw new StorageCorruptionException(
                    "Manifest pending-flush names the current WAL generation; a flush is only pending on a sealed generation.");
            if (manifest.HasTable(pending.TableId))
                throw new StorageCorruptionException(
                    $"Manifest pending-flush names table {pending.TableId}, which is already a live table.");
        }
    }

    public void Save(string directory)
    {
        var tempPath = Path.Combine(directory, TempFileName);
        var finalPath = Path.Combine(directory, FileName);
        using (var stream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
        using (var writer = new StreamWriter(stream))
        {
            writer.WriteLine("# LSM storage engine manifest. Replaced atomically; do not edit while a database is open.");
            writer.WriteLine("format 2");
            writer.WriteLine($"comparator {ComparerName}");
            writer.WriteLine($"next-sst-id {NextSstId}");
            foreach (var generation in WalGenerations)
                writer.WriteLine($"wal-gen {generation}");
            if (PendingFlush is { } pending)
                writer.WriteLine($"pending-flush {pending.Generation} {pending.TableId}");
            foreach (var (id, level) in Tables)
                writer.WriteLine($"sst {id} {level}");
            writer.Flush();
            stream.Flush(flushToDisk: true);
        }
        File.Move(tempPath, finalPath, overwrite: true);
        LoadedAsVersion1 = false;
    }
}
