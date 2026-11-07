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

    /// <summary>Number of SSTables at which an automatic (full-merge) compaction runs after a flush.</summary>
    public int CompactionTableThreshold { get; init; } = 4;

    /// <summary>Bloom filter size per SSTable entry. 10 bits/key ≈ 1% false-positive rate.</summary>
    public int BloomBitsPerKey { get; init; } = 10;

    /// <summary>Every Nth entry of an SSTable is recorded in its sparse index.</summary>
    public int IndexSampleInterval { get; init; } = 16;

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
    }
}
