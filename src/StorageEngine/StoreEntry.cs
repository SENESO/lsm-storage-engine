namespace StorageEngine;

/// <summary>The kind of a stored entry, at every layer of the engine.</summary>
internal enum EntryKind : byte
{
    /// <summary>A live value (possibly carrying an expiry deadline).</summary>
    Value = 0,

    /// <summary>A point tombstone: this key was deleted at this entry's recency.</summary>
    Tombstone = 1,

    /// <summary>
    /// A range tombstone: every key in [Key, RangeEnd) was deleted at this
    /// entry's recency. In a point lookup result it also serves as the
    /// "covered by a range tombstone" marker (RangeEnd null there).
    /// </summary>
    RangeTombstone = 2,
}

/// <summary>
/// One entry as it flows through the memtable, the flush pipeline, SSTables,
/// merges, and scans — the single internal representation every layer shares.
/// A point entry is a value (with an optional absolute expiry deadline) or a
/// tombstone; a range entry is a range tombstone over [Key, RangeEnd).
/// </summary>
internal readonly struct StoreEntry
{
    public byte[] Key { get; }
    public byte[]? Value { get; }
    public long ExpiresAtMs { get; }
    public EntryKind Kind { get; }
    public byte[]? RangeEnd { get; }

    private StoreEntry(byte[] key, byte[]? value, long expiresAtMs, EntryKind kind, byte[]? rangeEnd)
    {
        Key = key;
        Value = value;
        ExpiresAtMs = expiresAtMs;
        Kind = kind;
        RangeEnd = rangeEnd;
    }

    public static StoreEntry Put(byte[] key, byte[] value, long expiresAtMs = 0) =>
        new(key, value, expiresAtMs, EntryKind.Value, null);

    public static StoreEntry Delete(byte[] key) =>
        new(key, null, 0, EntryKind.Tombstone, null);

    public static StoreEntry Range(byte[] startInclusive, byte[] endExclusive) =>
        new(startInclusive, null, 0, EntryKind.RangeTombstone, endExclusive);

    /// <summary>The lookup-result marker: the key is covered by a range tombstone in the source that answered.</summary>
    public static StoreEntry CoveredByRange(byte[] key) =>
        new(key, null, 0, EntryKind.RangeTombstone, null);

    /// <summary>True when this is a value whose deadline has been reached at <paramref name="nowMs"/>.</summary>
    public bool IsExpiredAt(long nowMs) =>
        Kind == EntryKind.Value && ExpiresAtMs != 0 && ExpiresAtMs <= nowMs;
}
