using System.Buffers.Binary;

namespace StorageEngine;

/// <summary>
/// One decoded WAL payload operation. Exactly one shape is set:
/// a put (<see cref="Value"/> non-null, <see cref="ExpiresAtMs"/> optionally
/// carrying an absolute expiry deadline in unix milliseconds), a point delete
/// (<see cref="Value"/> null), or a range delete (<see cref="RangeEnd"/>
/// non-null: [Key, RangeEnd) deleted).
/// </summary>
internal readonly record struct RecordOp(byte[] Key, byte[]? Value)
{
    /// <summary>Absolute expiry deadline (unix ms) for a put; 0 = no expiry.</summary>
    public long ExpiresAtMs { get; init; }

    /// <summary>
    /// Relative TTL carried by a not-yet-written batch put. <see cref="LsmDb"/>
    /// converts it to an absolute <see cref="ExpiresAtMs"/> against its clock
    /// at write time, so the WAL only ever stores absolute deadlines. Never
    /// set on a decoded op.
    /// </summary>
    public TimeSpan? Ttl { get; init; }

    /// <summary>Exclusive end key for a range delete; null for any other operation.</summary>
    public byte[]? RangeEnd { get; init; }

    public bool IsDelete => Value is null && RangeEnd is null;
    public bool IsRangeDelete => RangeEnd is not null;
}

/// <summary>
/// Encodes and decodes WAL record payloads. Layouts (all integers little-endian):
///   Put:          [0x01][keyLen i32][key][valueLen i32][value]
///   Delete:       [0x02][keyLen i32][key]
///   Batch:        [0x03][opCount i32] then per op: [opType u8] + the op body
///   PutWithExpiry:[0x04][keyLen i32][key][expiresAtMs i64][valueLen i32][value]
///   RangeDelete:  [0x05][startLen i32][start][endLen i32][end]
/// Batch op bodies reuse the single-record layouts minus the leading type byte,
/// with opType in {0x01, 0x02, 0x04, 0x05}. The original layouts (0x01, 0x02,
/// and batches of them) are byte-for-byte what the first engine wrote, so old
/// WALs decode identically; the new types are additive.
/// A batch is a single WAL record, so the WAL's CRC makes it atomic: on recovery
/// it is applied whole or (as part of a torn tail) not at all.
/// </summary>
internal static class RecordCodec
{
    private const byte TypePut = 0x01;
    private const byte TypeDelete = 0x02;
    private const byte TypeBatch = 0x03;
    private const byte TypePutWithExpiry = 0x04;
    private const byte TypeRangeDelete = 0x05;

    public static byte[] EncodePut(byte[] key, byte[] value)
    {
        var payload = new byte[1 + 4 + key.Length + 4 + value.Length];
        var span = payload.AsSpan();
        span[0] = TypePut;
        BinaryPrimitives.WriteInt32LittleEndian(span[1..], key.Length);
        key.CopyTo(span[5..]);
        BinaryPrimitives.WriteInt32LittleEndian(span[(5 + key.Length)..], value.Length);
        value.CopyTo(span[(9 + key.Length)..]);
        return payload;
    }

    public static byte[] EncodeDelete(byte[] key)
    {
        var payload = new byte[1 + 4 + key.Length];
        payload[0] = TypeDelete;
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(1), key.Length);
        key.CopyTo(payload.AsSpan(5));
        return payload;
    }

    public static byte[] EncodePutWithExpiry(byte[] key, byte[] value, long expiresAtMs)
    {
        var payload = new byte[1 + 4 + key.Length + 8 + 4 + value.Length];
        var span = payload.AsSpan();
        span[0] = TypePutWithExpiry;
        BinaryPrimitives.WriteInt32LittleEndian(span[1..], key.Length);
        key.CopyTo(span[5..]);
        BinaryPrimitives.WriteInt64LittleEndian(span[(5 + key.Length)..], expiresAtMs);
        BinaryPrimitives.WriteInt32LittleEndian(span[(13 + key.Length)..], value.Length);
        value.CopyTo(span[(17 + key.Length)..]);
        return payload;
    }

    public static byte[] EncodeRangeDelete(byte[] startInclusive, byte[] endExclusive)
    {
        var payload = new byte[1 + 4 + startInclusive.Length + 4 + endExclusive.Length];
        var span = payload.AsSpan();
        span[0] = TypeRangeDelete;
        BinaryPrimitives.WriteInt32LittleEndian(span[1..], startInclusive.Length);
        startInclusive.CopyTo(span[5..]);
        BinaryPrimitives.WriteInt32LittleEndian(span[(5 + startInclusive.Length)..], endExclusive.Length);
        endExclusive.CopyTo(span[(9 + startInclusive.Length)..]);
        return payload;
    }

    public static byte[] EncodeBatch(IReadOnlyList<RecordOp> ops)
    {
        var size = 1 + 4;
        foreach (var op in ops)
            size += OpSize(op);

        var payload = new byte[size];
        var span = payload.AsSpan();
        span[0] = TypeBatch;
        BinaryPrimitives.WriteInt32LittleEndian(span[1..], ops.Count);
        var pos = 5;
        foreach (var op in ops)
        {
            span[pos] = OpType(op);
            pos++;
            WriteOpBody(span, ref pos, op);
        }
        return payload;
    }

    private static byte OpType(RecordOp op) =>
        op.IsRangeDelete ? TypeRangeDelete
        : op.IsDelete ? TypeDelete
        : op.ExpiresAtMs != 0 ? TypePutWithExpiry
        : TypePut;

    private static int OpSize(RecordOp op)
    {
        var size = 1 + 4 + op.Key.Length; // type byte + key length + key
        if (op.IsRangeDelete)
            size += 4 + op.RangeEnd!.Length;
        else if (!op.IsDelete)
            size += (op.ExpiresAtMs != 0 ? 8 : 0) + 4 + op.Value!.Length;
        return size;
    }

    private static void WriteOpBody(Span<byte> span, ref int pos, RecordOp op)
    {
        BinaryPrimitives.WriteInt32LittleEndian(span[pos..], op.Key.Length);
        op.Key.CopyTo(span[(pos + 4)..]);
        pos += 4 + op.Key.Length;
        if (op.IsRangeDelete)
        {
            BinaryPrimitives.WriteInt32LittleEndian(span[pos..], op.RangeEnd!.Length);
            op.RangeEnd.CopyTo(span[(pos + 4)..]);
            pos += 4 + op.RangeEnd.Length;
        }
        else if (!op.IsDelete)
        {
            if (op.ExpiresAtMs != 0)
            {
                BinaryPrimitives.WriteInt64LittleEndian(span[pos..], op.ExpiresAtMs);
                pos += 8;
            }
            BinaryPrimitives.WriteInt32LittleEndian(span[pos..], op.Value!.Length);
            op.Value.CopyTo(span[(pos + 4)..]);
            pos += 4 + op.Value.Length;
        }
    }

    /// <summary>Decodes a payload that already passed its CRC check.</summary>
    public static List<RecordOp> Decode(ReadOnlySpan<byte> payload)
    {
        if (payload.IsEmpty)
            throw new StorageCorruptionException("WAL record payload is empty.");
        if (payload[0] == TypeBatch)
            return DecodeBatch(payload);
        if (payload[0] is TypePut or TypeDelete or TypePutWithExpiry or TypeRangeDelete)
        {
            var pos = 1;
            return new List<RecordOp> { DecodeOp(payload[0], payload, ref pos) };
        }
        throw new StorageCorruptionException($"WAL record payload has unknown type byte 0x{payload[0]:X2}.");
    }

    private static RecordOp DecodeOp(byte opType, ReadOnlySpan<byte> payload, ref int pos)
    {
        var key = ReadBytes(payload, ref pos, "key");
        switch (opType)
        {
            case TypePut:
            {
                var value = ReadBytes(payload, ref pos, "value");
                return new RecordOp(key, value);
            }
            case TypeDelete:
                return new RecordOp(key, null);
            case TypePutWithExpiry:
            {
                if (pos + 8 > payload.Length)
                    throw new StorageCorruptionException("Record ends before the expiry deadline.");
                var expiresAtMs = BinaryPrimitives.ReadInt64LittleEndian(payload[pos..]);
                pos += 8;
                var value = ReadBytes(payload, ref pos, "value");
                return new RecordOp(key, value) { ExpiresAtMs = expiresAtMs };
            }
            case TypeRangeDelete:
            {
                var end = ReadBytes(payload, ref pos, "range end");
                return new RecordOp(key, null) { RangeEnd = end };
            }
            default:
                throw new StorageCorruptionException($"Record carries unknown operation type 0x{opType:X2}.");
        }
    }

    private static List<RecordOp> DecodeBatch(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < 5)
            throw new StorageCorruptionException("Batch record is too short for its header.");
        var count = BinaryPrimitives.ReadInt32LittleEndian(payload[1..]);
        if (count < 0)
            throw new StorageCorruptionException("Batch record carries a negative operation count.");
        var ops = new List<RecordOp>(count);
        var pos = 5;
        for (var i = 0; i < count; i++)
        {
            if (pos >= payload.Length)
                throw new StorageCorruptionException("Batch record ends in the middle of an operation.");
            var opType = payload[pos++];
            if (opType is not (TypePut or TypeDelete or TypePutWithExpiry or TypeRangeDelete))
                throw new StorageCorruptionException($"Batch record contains unknown operation type 0x{opType:X2}.");
            ops.Add(DecodeOp(opType, payload, ref pos));
        }
        return ops;
    }

    private static byte[] ReadBytes(ReadOnlySpan<byte> payload, ref int pos, string what)
    {
        if (pos + 4 > payload.Length)
            throw new StorageCorruptionException($"Record ends before the {what} length.");
        var length = BinaryPrimitives.ReadInt32LittleEndian(payload[pos..]);
        pos += 4;
        if (length < 0 || pos + length > payload.Length)
            throw new StorageCorruptionException($"Record carries an invalid {what} length ({length}).");
        var bytes = payload.Slice(pos, length).ToArray();
        pos += length;
        return bytes;
    }
}
