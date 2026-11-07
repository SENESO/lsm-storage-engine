namespace StorageEngine;

/// <summary>
/// A single write operation against an <see cref="LsmDb"/>. A batch is appended
/// to the WAL as one record, so it is atomic: after a crash, recovery applies
/// either every operation in the batch or none of them.
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

    public WriteBatch Delete(byte[] key)
    {
        ArgumentNullException.ThrowIfNull(key);
        _ops.Add(new RecordOp(key, null));
        return this;
    }

    internal IReadOnlyList<RecordOp> Ops => _ops;
}
