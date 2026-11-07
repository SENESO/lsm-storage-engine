using System.Buffers.Binary;

namespace StorageEngine;

/// <summary>One decoded WAL payload operation. <see cref="Value"/> is null for a delete.</summary>
internal readonly record struct RecordOp(byte[] Key, byte[]? Value)
{
    public bool IsDelete => Value is null;
}

/// <summary>
/// Encodes and decodes WAL record payloads. Layouts (all integers little-endian):
///   Put:    [0x01][keyLen i32][key][valueLen i32][value]
///   Delete: [0x02][keyLen i32][key]
///   Batch:  [0x03][opCount i32] then per op: [opType u8][keyLen i32][key]
///           plus [valueLen i32][value] when opType is Put.
/// A batch is a single WAL record, so the WAL's CRC makes it atomic: on recovery
/// it is applied whole or (as part of a torn tail) not at all.
/// </summary>
internal static class RecordCodec
{
    private const byte TypePut = 0x01;
    private const byte TypeDelete = 0x02;
    private const byte TypeBatch = 0x03;

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

    public static byte[] EncodeBatch(IReadOnlyList<RecordOp> ops)
    {
        var size = 1 + 4;
        foreach (var op in ops)
            size += 1 + 4 + op.Key.Length + (op.IsDelete ? 0 : 4 + op.Value!.Length);

        var payload = new byte[size];
        var span = payload.AsSpan();
        span[0] = TypeBatch;
        BinaryPrimitives.WriteInt32LittleEndian(span[1..], ops.Count);
        var pos = 5;
        foreach (var op in ops)
        {
            span[pos] = op.IsDelete ? TypeDelete : TypePut;
            BinaryPrimitives.WriteInt32LittleEndian(span[(pos + 1)..], op.Key.Length);
            op.Key.CopyTo(span[(pos + 5)..]);
            pos += 5 + op.Key.Length;
            if (!op.IsDelete)
            {
                BinaryPrimitives.WriteInt32LittleEndian(span[pos..], op.Value!.Length);
                op.Value.CopyTo(span[(pos + 4)..]);
                pos += 4 + op.Value.Length;
            }
        }
        return payload;
    }

    /// <summary>Decodes a payload that already passed its CRC check.</summary>
    public static List<RecordOp> Decode(ReadOnlySpan<byte> payload)
    {
        if (payload.IsEmpty)
            throw new StorageCorruptionException("WAL record payload is empty.");
        return payload[0] switch
        {
            TypePut => new List<RecordOp> { DecodePut(payload) },
            TypeDelete => new List<RecordOp> { DecodeDelete(payload) },
            TypeBatch => DecodeBatch(payload),
            var other => throw new StorageCorruptionException($"WAL record payload has unknown type byte 0x{other:X2}."),
        };
    }

    private static RecordOp DecodePut(ReadOnlySpan<byte> payload)
    {
        var pos = 1;
        var key = ReadBytes(payload, ref pos, "key");
        var value = ReadBytes(payload, ref pos, "value");
        return new RecordOp(key, value);
    }

    private static RecordOp DecodeDelete(ReadOnlySpan<byte> payload)
    {
        var pos = 1;
        var key = ReadBytes(payload, ref pos, "key");
        return new RecordOp(key, null);
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
            var key = ReadBytes(payload, ref pos, "key");
            if (opType == TypePut)
            {
                var value = ReadBytes(payload, ref pos, "value");
                ops.Add(new RecordOp(key, value));
            }
            else if (opType == TypeDelete)
            {
                ops.Add(new RecordOp(key, null));
            }
            else
            {
                throw new StorageCorruptionException($"Batch record contains unknown operation type 0x{opType:X2}.");
            }
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
