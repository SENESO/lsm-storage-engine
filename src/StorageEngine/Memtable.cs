namespace StorageEngine;

/// <summary>
/// The in-memory sorted store. A red-black tree (<see cref="SortedDictionary{TKey,TValue}"/>)
/// keyed by raw bytes under the database's key comparer; a null value in a
/// point entry is a tombstone, and a point entry may carry an absolute expiry
/// deadline. Chosen over a skip list deliberately: the engine has exactly one
/// writer and guards the memtable with the database lock, so the skip list's one
/// advantage (lock-free concurrent mutation) buys nothing, while the tree gives
/// deterministic O(log n) operations, in-order enumeration for flushes and scans,
/// and no probabilistic edge cases.
///
/// Range tombstones live in a side list in application order. Applying one
/// physically removes the point entries it covers (they are dead at this
/// memtable's recency), and the tombstone itself stays on the list so that
/// lookups and merges can still shadow older layers for the keys that were
/// not re-put afterwards. Not thread-safe by itself — <see cref="LsmDb"/>
/// serializes all access.
/// </summary>
internal sealed class Memtable
{
    // Rough per-entry bookkeeping cost on top of key+value bytes: tree node,
    // array headers, dictionary slack. Approximate by design; the threshold is
    // a flush trigger, not an accounting guarantee.
    private const int EntryOverheadBytes = 48;

    /// <summary>A point entry: a value plus its expiry deadline (0 = none), or a tombstone when the value is null.</summary>
    internal readonly record struct PointValue(byte[]? Value, long ExpiresAtMs);

    private readonly SortedDictionary<byte[], PointValue> _map;
    private readonly List<(byte[] Start, byte[] End)> _ranges = new();
    private readonly IComparer<byte[]> _comparer;

    public Memtable(IComparer<byte[]>? comparer = null)
    {
        _comparer = comparer ?? ByteArrayComparer.Instance;
        _map = new SortedDictionary<byte[], PointValue>(_comparer);
    }

    public long SizeBytes { get; private set; }

    public int Count => _map.Count;

    public int RangeCount => _ranges.Count;

    public void Put(byte[] key, byte[]? value) => Put(key, value, 0);

    public void Put(byte[] key, byte[]? value, long expiresAtMs)
    {
        if (_map.TryGetValue(key, out var old))
            SizeBytes -= EntryCost(key, old.Value);
        _map[key] = new PointValue(value, expiresAtMs);
        SizeBytes += EntryCost(key, value);
    }

    /// <summary>Applies any entry kind: point value, point tombstone, or range tombstone.</summary>
    public void Apply(StoreEntry entry)
    {
        switch (entry.Kind)
        {
            case EntryKind.Value:
                Put(entry.Key, entry.Value, entry.ExpiresAtMs);
                break;
            case EntryKind.Tombstone:
                Put(entry.Key, null, 0);
                break;
            case EntryKind.RangeTombstone:
                ApplyRangeDelete(entry.Key, entry.RangeEnd!);
                break;
        }
    }

    /// <summary>
    /// Records a range tombstone over [startInclusive, endExclusive) and
    /// removes the point entries it covers — they are dead at this recency,
    /// and the tombstone on the list keeps shadowing older layers for them.
    /// </summary>
    public void ApplyRangeDelete(byte[] startInclusive, byte[] endExclusive)
    {
        if (_map.Count > 0)
        {
            // Collect first: the dictionary cannot change during enumeration.
            List<byte[]>? covered = null;
            foreach (var key in _map.Keys)
            {
                if (_comparer.Compare(key, startInclusive) >= 0 &&
                    _comparer.Compare(key, endExclusive) < 0)
                    (covered ??= new List<byte[]>()).Add(key);
            }
            if (covered is not null)
            {
                foreach (var key in covered)
                {
                    SizeBytes -= EntryCost(key, _map[key].Value);
                    _map.Remove(key);
                }
            }
        }
        _ranges.Add((startInclusive, endExclusive));
        SizeBytes += startInclusive.Length + endExclusive.Length + EntryOverheadBytes;
    }

    /// <summary>Returns true when the key has a point entry at all; a null value in it is a tombstone.</summary>
    public bool TryGet(byte[] key, out PointValue value) => _map.TryGetValue(key, out value);

    /// <summary>True when any range tombstone recorded here covers <paramref name="key"/>.</summary>
    public bool CoversRange(byte[] key)
    {
        foreach (var (start, end) in _ranges)
        {
            if (_comparer.Compare(start, key) <= 0 && _comparer.Compare(key, end) < 0)
                return true;
        }
        return false;
    }

    /// <summary>
    /// All entries in ascending key order under the comparer: point entries
    /// merged with range tombstones (sorted by their start key; ranges keep
    /// application order among equal starts). This is the collapsed state —
    /// a point entry that shares its key with a range start survived that
    /// tombstone (it was put afterwards).
    /// </summary>
    public IReadOnlyList<StoreEntry> SnapshotEntries()
    {
        var entries = new List<StoreEntry>(_map.Count + _ranges.Count);
        var sortedRanges = _ranges
            .Select(r => StoreEntry.Range(r.Start, r.End))
            .OrderBy(r => r.Key, _comparer) // stable: equal starts keep application order
            .ToList();
        var ri = 0;
        foreach (var pair in _map)
        {
            while (ri < sortedRanges.Count && _comparer.Compare(sortedRanges[ri].Key, pair.Key) < 0)
                entries.Add(sortedRanges[ri++]);
            entries.Add(pair.Value.Value is null
                ? StoreEntry.Delete(pair.Key)
                : StoreEntry.Put(pair.Key, pair.Value.Value, pair.Value.ExpiresAtMs));
        }
        while (ri < sortedRanges.Count)
            entries.Add(sortedRanges[ri++]);
        return entries;
    }

    private static long EntryCost(byte[] key, byte[]? value) =>
        key.Length + (value?.Length ?? 0) + EntryOverheadBytes;
}
