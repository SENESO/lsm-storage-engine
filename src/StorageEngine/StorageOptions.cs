namespace StorageEngine;

/// <summary>How the write-ahead log trades write latency for durability.</summary>
public enum FsyncPolicy
{
    /// <summary>
    /// fsync after every record. A write that returned survives a process crash,
    /// an OS crash, and power loss. Slowest — one disk flush per write.
    /// </summary>
    Always,

    /// <summary>
    /// Records are handed to the OS on every write but fsynced on a timer
    /// (<see cref="StorageOptions.FsyncIntervalMilliseconds"/>). A write that
    /// returned survives a process crash, but an OS crash or power loss can lose
    /// everything written since the last completed fsync.
    /// </summary>
    Interval,
}

/// <summary>How SSTables are compacted.</summary>
public enum CompactionStrategy
{
    /// <summary>
    /// Size-tiered in spirit: tables accumulate as a tier of flush outputs and
    /// compaction is a full merge of every live table into one. Simple and
    /// always safe for tombstones (every merge is a bottom merge), at the cost
    /// of rewriting all data on every compaction. The default.
    /// </summary>
    Tiered,

    /// <summary>
    /// Leveled: flush outputs land in level 0 (overlapping); each deeper level
    /// is key-range partitioned and size-capped at a multiple of the level
    /// above. Compactions merge one level's slice into the next, so steady-state
    /// write amplification is far lower than a full merge; tombstones are only
    /// dropped when provably nothing live beneath them can be resurrected.
    /// </summary>
    Leveled,
}

/// <summary>Options for <see cref="LsmDb.Open"/>.</summary>
public sealed class StorageOptions
{
    /// <summary>Directory holding the WAL, SSTables, manifest, and lock file. Created if missing.</summary>
    public required string DirectoryPath { get; init; }

    /// <summary>
    /// Approximate in-memory size of the memtable (sum of key + value bytes plus a
    /// fixed per-entry overhead) at which it is flushed to an SSTable.
    /// </summary>
    public long MemTableSizeBytes { get; init; } = 4 * 1024 * 1024;

    /// <summary>WAL durability policy. Default <see cref="FsyncPolicy.Always"/>.</summary>
    public FsyncPolicy FsyncPolicy { get; init; } = FsyncPolicy.Always;

    /// <summary>fsync cadence when <see cref="FsyncPolicy"/> is <see cref="StorageEngine.FsyncPolicy.Interval"/>.</summary>
    public int FsyncIntervalMilliseconds { get; init; } = 200;

    /// <summary>
    /// Tiered: number of SSTables at which an automatic (full-merge) compaction
    /// runs after a flush. Leveled: number of level-0 tables at which L0 is
    /// compacted into level 1.
    /// </summary>
    public int CompactionTableThreshold { get; init; } = 4;

    /// <summary>Bloom filter size per SSTable entry. 10 bits/key ≈ 1% false-positive rate.</summary>
    public int BloomBitsPerKey { get; init; } = 10;

    /// <summary>Every Nth entry of an SSTable is recorded in its sparse index.</summary>
    public int IndexSampleInterval { get; init; } = 16;

    /// <summary>
    /// The key ordering for the whole database: memtable, SSTables, merges,
    /// scans, and range operations. Null (the default) is unsigned
    /// lexicographic byte order (<see cref="ByteArrayComparer"/>). A custom
    /// comparer requires <see cref="KeyComparerName"/>: the name is recorded
    /// in the manifest, and opening a directory whose recorded name differs
    /// from the name supplied here is refused — the on-disk order would be
    /// meaningless under a different ordering. A comparer may equate distinct
    /// byte strings (a case-insensitive ordering does): reads stay correct —
    /// the per-table bloom filter, which is keyed on exact bytes, is only
    /// consulted under the built-in byte ordering.
    /// </summary>
    public IComparer<byte[]>? KeyComparer { get; init; }

    /// <summary>
    /// The identity of <see cref="KeyComparer"/>, recorded in the manifest.
    /// Required when a custom comparer is supplied; must be a single token
    /// (no whitespace). The built-in byte order's name is "bytewise".
    /// </summary>
    public string? KeyComparerName { get; init; }

    /// <summary>
    /// The clock TTL deadlines are measured against. Null (the default) is the
    /// system UTC clock. Supplying a controllable clock is a test seam: it
    /// lets a suite advance time deterministically instead of sleeping.
    /// Deadlines are persisted as absolute unix milliseconds, so a directory
    /// reopened under a different clock observes the same deadlines.
    /// </summary>
    public Func<DateTimeOffset>? Clock { get; init; }

    /// <summary>Compaction strategy. Default <see cref="CompactionStrategy.Tiered"/>.</summary>
    public CompactionStrategy CompactionStrategy { get; init; } = CompactionStrategy.Tiered;

    /// <summary>Leveled only: the size budget of level 1 in bytes; each deeper level's budget is the previous level's times <see cref="LeveledSizeRatio"/>.</summary>
    public long LeveledBaseLevelSizeBytes { get; init; } = 10 * 1024 * 1024;

    /// <summary>Leveled only: how much larger each level's size budget is than the level above it.</summary>
    public int LeveledSizeRatio { get; init; } = 10;

    /// <summary>Leveled only: approximate size at which a compaction's output is split into another table.</summary>
    public long LeveledTargetTableSizeBytes { get; init; } = 2 * 1024 * 1024;

    /// <summary>The resolved key comparer: the custom one, or byte order.</summary>
    internal IComparer<byte[]> Comparer => KeyComparer ?? ByteArrayComparer.Instance;

    /// <summary>The resolved comparer identity recorded in the manifest.</summary>
    internal string ComparerName => KeyComparerName ?? "bytewise";

    /// <summary>Current time per the configured clock, in unix milliseconds.</summary>
    internal long NowMs() =>
        (Clock?.Invoke() ?? DateTimeOffset.UtcNow).ToUnixTimeMilliseconds();

    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(DirectoryPath))
            throw new ArgumentException("DirectoryPath must not be empty.", nameof(DirectoryPath));
        if (MemTableSizeBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(MemTableSizeBytes), "MemTableSizeBytes must be positive.");
        if (FsyncIntervalMilliseconds <= 0)
            throw new ArgumentOutOfRangeException(nameof(FsyncIntervalMilliseconds), "FsyncIntervalMilliseconds must be positive.");
        if (CompactionTableThreshold < 2)
            throw new ArgumentOutOfRangeException(nameof(CompactionTableThreshold), "CompactionTableThreshold must be at least 2.");
        if (BloomBitsPerKey <= 0)
            throw new ArgumentOutOfRangeException(nameof(BloomBitsPerKey), "BloomBitsPerKey must be positive.");
        if (IndexSampleInterval <= 0)
            throw new ArgumentOutOfRangeException(nameof(IndexSampleInterval), "IndexSampleInterval must be positive.");
        if (KeyComparer is not null && string.IsNullOrWhiteSpace(KeyComparerName))
            throw new ArgumentException(
                "A custom KeyComparer requires a KeyComparerName: the name is the comparer's recorded identity in the manifest.",
                nameof(KeyComparerName));
        if (KeyComparer is null && KeyComparerName is not null)
            throw new ArgumentException(
                "KeyComparerName was supplied without a KeyComparer.", nameof(KeyComparerName));
        if (KeyComparerName is not null && KeyComparerName.Any(char.IsWhiteSpace))
            throw new ArgumentException("KeyComparerName must be a single token without whitespace.", nameof(KeyComparerName));
        if (LeveledBaseLevelSizeBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(LeveledBaseLevelSizeBytes), "LeveledBaseLevelSizeBytes must be positive.");
        if (LeveledSizeRatio < 2)
            throw new ArgumentOutOfRangeException(nameof(LeveledSizeRatio), "LeveledSizeRatio must be at least 2.");
        if (LeveledTargetTableSizeBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(LeveledTargetTableSizeBytes), "LeveledTargetTableSizeBytes must be positive.");
    }
}
