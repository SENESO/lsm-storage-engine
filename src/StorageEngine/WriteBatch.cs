namespace StorageEngine;

/// <summary>
/// A single write operation against an <see cref="LsmDb"/>. A batch is appended
/// to the WAL as one record, so it is atomic: after a crash, recovery applies
/// either every operation in the batch or none of them. Operations apply in
/// the order they were added.
/// </summary>
public sealed class WriteBatch
{
    private readonly List<RecordOp> _ops = new();

    public int Count => _ops.Count;

    public WriteBatch Put(byte[] key, byte[] value)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        _ops.Add(new RecordOp(key, value));
        return this;
    }

    /// <summary>
    /// Puts a value that expires after <paramref name="ttl"/>, measured on the
    /// database's clock at the moment the batch is written (the relative TTL
    /// becomes an absolute deadline in the WAL record, so recovery replays
    /// the same deadline).
    /// </summary>
    public WriteBatch Put(byte[] key, byte[] value, TimeSpan ttl)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        if (ttl <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ttl), "TTL must be positive.");
        _ops.Add(new RecordOp(key, value) { Ttl = ttl });
        return this;
    }

    public WriteBatch Delete(byte[] key)
    {
        ArgumentNullException.ThrowIfNull(key);
        _ops.Add(new RecordOp(key, null));
        return this;
    }

    /// <summary>Deletes every key in [startInclusive, endExclusive) — one range tombstone operation.</summary>
    public WriteBatch DeleteRange(byte[] startInclusive, byte[] endExclusive)
    {
        ArgumentNullException.ThrowIfNull(startInclusive);
        ArgumentNullException.ThrowIfNull(endExclusive);
        _ops.Add(new RecordOp(startInclusive, null) { RangeEnd = endExclusive });
        return this;
    }

    internal IReadOnlyList<RecordOp> Ops => _ops;
}
