namespace StorageEngine;

/// <summary>
/// The in-memory sorted store. A red-black tree (<see cref="SortedDictionary{TKey,TValue}"/>)
/// keyed by raw bytes under <see cref="ByteArrayComparer"/>; a null value is a
/// tombstone. Chosen over a skip list deliberately: the engine has exactly one
/// writer and guards the memtable with the database lock, so the skip list's one
/// advantage (lock-free concurrent mutation) buys nothing, while the tree gives
/// deterministic O(log n) operations, in-order enumeration for flushes and scans,
/// and no probabilistic edge cases. Not thread-safe by itself — <see cref="LsmDb"/>
/// serializes all access.
/// </summary>
internal sealed class Memtable
{
    // Rough per-entry bookkeeping cost on top of key+value bytes: tree node,
    // array headers, dictionary slack. Approximate by design; the threshold is
    // a flush trigger, not an accounting guarantee.
    private const int EntryOverheadBytes = 48;

    private readonly SortedDictionary<byte[], byte[]?> _map = new(ByteArrayComparer.Instance);

    public long SizeBytes { get; private set; }

    public int Count => _map.Count;

    public void Put(byte[] key, byte[]? value)
    {
        if (_map.TryGetValue(key, out var old))
            SizeBytes -= EntryCost(key, old);
        _map[key] = value;
        SizeBytes += EntryCost(key, value);
    }

    /// <summary>Returns true when the key is present at all; <paramref name="value"/> is null for a tombstone.</summary>
    public bool TryGet(byte[] key, out byte[]? value) => _map.TryGetValue(key, out value);

    /// <summary>All entries in ascending key order.</summary>
    public IReadOnlyList<KeyValuePair<byte[], byte[]?>> SnapshotEntries()
    {
        var entries = new List<KeyValuePair<byte[], byte[]?>>(_map.Count);
        foreach (var pair in _map)
            entries.Add(pair);
        return entries;
    }

    private static long EntryCost(byte[] key, byte[]? value) =>
        key.Length + (value?.Length ?? 0) + EntryOverheadBytes;
}
