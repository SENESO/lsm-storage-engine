using System.Buffers.Binary;

namespace StorageEngine;

/// <summary>
/// Append-only write-ahead log. On-disk record frame:
///   [payloadLength int32][crc32 uint32 of payload][payload]
/// Every acknowledged write is appended (and per the fsync policy, flushed)
/// before the memtable is touched, so recovery can replay the log to rebuild it.
/// </summary>
internal sealed class WriteAheadLog : IDisposable
{
    internal const int HeaderSize = 8;

    /// <summary>Sanity bound for one record's payload; anything larger on disk is damage, not data.</summary>
    internal const int MaxPayloadBytes = 256 * 1024 * 1024;

    private readonly FileStream _stream;
    private readonly FsyncPolicy _policy;
    private readonly object _gate = new();
    private Timer? _timer;
    private bool _closed;

    private WriteAheadLog(FileStream stream, FsyncPolicy policy)
    {
        _stream = stream;
        _policy = policy;
    }

    public static WriteAheadLog OpenForAppend(string path, FsyncPolicy policy, int intervalMilliseconds)
    {
        var stream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite, bufferSize: 4096);
        stream.Seek(0, SeekOrigin.End);
        var log = new WriteAheadLog(stream, policy);
        if (policy == FsyncPolicy.Interval)
            log._timer = new Timer(_ => log.SyncQuietly(), null, intervalMilliseconds, intervalMilliseconds);
        return log;
    }

    public void Append(ReadOnlySpan<byte> payload)
    {
        if (payload.Length > MaxPayloadBytes)
            throw new ArgumentException($"Record payload of {payload.Length} bytes exceeds the {MaxPayloadBytes}-byte limit.", nameof(payload));

        Span<byte> header = stackalloc byte[HeaderSize];
        BinaryPrimitives.WriteInt32LittleEndian(header, payload.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(header[4..], Crc32.Compute(payload));

        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_closed, this);
            _stream.Write(header);
            _stream.Write(payload);
            // The record is now with the OS. Flush(true) additionally forces it
            // to the physical device — that is the fsync policy decision.
            _stream.Flush(flushToDisk: _policy == FsyncPolicy.Always);
        }
    }

    /// <summary>Forces everything written so far to the physical device.</summary>
    public void Sync()
    {
        lock (_gate)
        {
            if (!_closed)
                _stream.Flush(flushToDisk: true);
        }
    }

    private void SyncQuietly()
    {
        try
        {
            Sync();
        }
        catch
        {
            // A background fsync failure cannot be reported to a caller that is
            // not there. The next foreground Sync/Append surfaces stream errors.
        }
    }

    /// <summary>
    /// Clean close: final fsync, then release the file. Used by
    /// <see cref="LsmDb.Dispose"/> only — crash paths use <see cref="Abandon"/>.
    /// </summary>
    public void Dispose()
    {
        _timer?.Dispose();
        lock (_gate)
        {
            if (_closed) return;
            _closed = true;
            _stream.Flush(flushToDisk: true);
            _stream.Dispose();
        }
    }

    /// <summary>
    /// Test hook simulating process death: the stream is dropped with no final
    /// fsync and no cleanup. Bytes already handed to the OS survive (a process
    /// crash does not lose the page cache); bytes never fsynced are exactly as
    /// durable as the policy promised — no more.
    /// </summary>
    internal void Abandon()
    {
        _timer?.Dispose();
        lock (_gate)
        {
            if (_closed) return;
            _closed = true;
            _stream.Dispose();
        }
    }
}

/// <summary>
/// WAL recovery: sequential replay with torn-tail truncation and loud mid-log
/// corruption detection.
///
/// The distinction between the two kinds of damage:
///  - A <b>torn tail</b> is what a crash during append leaves: the final record
///    is partial, or fails its CRC, and nothing valid follows it. Replay stops
///    at the last valid record and the caller truncates the file there.
///  - <b>Mid-log corruption</b> is damage to a record that has intact, valid
///    records after it — a crash cannot produce that (appends are sequential),
///    so it means the file was damaged after being fully written. That is a
///    loud <see cref="StorageCorruptionException"/>, never a silent skip.
/// The resync scan that tells them apart looks for a chain of CRC-valid records
/// reaching exactly to end-of-file after the first bad record.
/// </summary>
/// <summary>Callback for one recovered WAL record payload during replay.</summary>
internal delegate void WalRecordHandler(ReadOnlySpan<byte> payload);

internal static class WalRecovery
{
    /// <summary>
    /// Replays every valid record into <paramref name="apply"/> and returns the
    /// length of the valid prefix of the file (the caller truncates there).
    /// </summary>
    public static long Replay(string path, WalRecordHandler apply)
    {
        if (!File.Exists(path))
            return 0;

        var data = File.ReadAllBytes(path);
        var pos = 0;
        var lastGood = 0;

        while (pos < data.Length)
        {
            var remaining = data.Length - pos;
            if (remaining < WriteAheadLog.HeaderSize)
                break; // Partial header: torn tail.

            var length = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(pos));
            var crc = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(pos + 4));
            var headerSane = length >= 0
                             && length <= WriteAheadLog.MaxPayloadBytes
                             && remaining - WriteAheadLog.HeaderSize >= length;

            if (headerSane)
            {
                var payload = data.AsSpan(pos + WriteAheadLog.HeaderSize, length);
                if (Crc32.Compute(payload) == crc)
                {
                    apply(payload);
                    pos += WriteAheadLog.HeaderSize + length;
                    lastGood = pos;
                    continue;
                }

                // A complete record whose bytes are wrong. If valid records
                // follow it, the log was damaged in the middle: loud failure.
                if (HasValidChainStartingAtOrAfter(data, pos + WriteAheadLog.HeaderSize + length))
                    throw Corrupt(path, pos, "record fails its CRC but valid records follow it");
                break; // Torn tail.
            }

            // The header itself is implausible or the record runs past EOF.
            // A crash mid-append explains that only at the very tail; if a valid
            // record chain exists later in the file, this is mid-log damage.
            if (HasValidChainStartingAtOrAfter(data, pos + 1))
                throw Corrupt(path, pos, "record header is invalid but valid records follow it");
            break; // Torn tail.
        }

        return lastGood;
    }

    private static StorageCorruptionException Corrupt(string path, int offset, string detail) =>
        new($"Write-ahead log '{path}' is corrupt at byte offset {offset}: {detail}. " +
            "Refusing to open; no data was silently discarded.");

    /// <summary>
    /// True when some offset at or after <paramref name="from"/> begins a chain
    /// of one or more CRC-valid records that ends exactly at end-of-file.
    /// </summary>
    private static bool HasValidChainStartingAtOrAfter(byte[] data, int from)
    {
        for (var start = Math.Max(from, 0); start + WriteAheadLog.HeaderSize <= data.Length; start++)
        {
            if (ChainReachesEndOfFile(data, start))
                return true;
        }
        return false;
    }

    private static bool ChainReachesEndOfFile(byte[] data, int start)
    {
        var pos = start;
        var records = 0;
        while (pos < data.Length)
        {
            if (data.Length - pos < WriteAheadLog.HeaderSize)
                return false;
            var length = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(pos));
            if (length < 0 || length > WriteAheadLog.MaxPayloadBytes ||
                data.Length - pos - WriteAheadLog.HeaderSize < length)
                return false;
            var crc = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(pos + 4));
            if (Crc32.Compute(data.AsSpan(pos + WriteAheadLog.HeaderSize, length)) != crc)
                return false;
            pos += WriteAheadLog.HeaderSize + length;
            records++;
        }
        return records > 0 && pos == data.Length;
    }
}
