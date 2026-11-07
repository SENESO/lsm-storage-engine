namespace StorageEngine;

/// <summary>
/// A log-structured merge-tree (LSM) key-value store for byte-array keys and
/// values, in a single directory:
///
///   writes ──► WAL (append + fsync policy) ──► memtable (sorted, in memory)
///   memtable full ──► flush ──► immutable SSTable (sparse index + bloom filter)
///   too many SSTables ──► compaction (strategy-dependent; see StorageOptions)
///
/// Reads check the memtable, then SSTables newest-first (level-major under
/// the leveled strategy: level 0 newest to deepest level). Values may carry
/// a TTL: a deadline is stored with the value, reads treat a reached deadline
/// as absence (without resurrecting older copies), and flush/compaction
/// remove expired data physically. Keys may be deleted one at a time or by
/// range; a range tombstone shadows every older layer for its whole span.
/// One process may open a directory at a time (an OS file lock enforces it);
/// within the process any number of reader threads may run concurrently
/// with the writer.
///
/// Ownership: key and value arrays passed to <see cref="Put(byte[], byte[])"/>
/// and <see cref="Write"/> become the property of the store — do not mutate them
/// afterwards. Arrays returned by <see cref="Get"/> are fresh copies.
/// </summary>
public sealed class LsmDb : IDisposable
{
    private readonly StorageOptions _options;
    private readonly string _directory;
    private readonly ReaderWriterLockSlim _rw = new(LockRecursionPolicy.SupportsRecursion);
    private readonly FileStream _lockStream;
    private readonly Manifest _manifest;
    private readonly List<SSTable.Reader> _retiredTables = new();
    private List<SSTable.Reader> _tables; // ascending id
    private Memtable _memtable;
    private WriteAheadLog _wal;
    private bool _disposed;
    private bool _crashed;
    private long _totalTableBytesWritten;

    // Background flush state. While a flush is in flight, the frozen memtable
    // being written out is _pendingMemtable (newer than every table, older
    // than _memtable) and _pendingFlush is its job record. Both are guarded
    // by _rw like the rest of the state; the job's Done event is what
    // lock-free waiters (Flush, Compact, TableCount, Dispose) rendezvous on.
    private Memtable? _pendingMemtable;
    private FlushJob? _pendingFlush;
    private bool _flushRequested;
    private Exception? _flushFailure;

    // Test hooks, invoked on the background flush thread. They let the suite
    // hold a flush at an exact point (deterministic crash states) and inject
    // an I/O failure. Null in production use.
    internal Action? FlushJobStartingHook;
    internal Action? FlushTableWrittenHook;
    internal Action? FlushFailureHook;

    /// <summary>Total bytes of SSTable files this instance has written (flushes + compactions). Test instrumentation for write-amplification measurement.</summary>
    internal long TotalTableBytesWritten => Interlocked.Read(ref _totalTableBytesWritten);

    /// <summary>One in-flight background flush: a sealed WAL generation being written into its reserved table.</summary>
    private sealed class FlushJob
    {
        public required long Generation { get; init; }
        public required long TableId { get; init; }
        public required IReadOnlyList<StoreEntry> Entries { get; init; }
        public ManualResetEventSlim Done { get; } = new(false);

        /// <summary>Set when the job failed; written before <see cref="Done"/> is set.</summary>
        public Exception? Error;

        /// <summary>Set by SimulateCrash: the worker abandons the job at its next checkpoint, touching nothing.</summary>
        public bool Abandoned;
    }

    private LsmDb(StorageOptions options, string directory, FileStream lockStream, Manifest manifest,
        List<SSTable.Reader> tables, Memtable memtable, WriteAheadLog wal)
    {
        _options = options;
        _directory = directory;
        _lockStream = lockStream;
        _manifest = manifest;
        _tables = tables;
        _memtable = memtable;
        _wal = wal;
    }

    /// <summary>
    /// Opens (creating if needed) the database in <see cref="StorageOptions.DirectoryPath"/>:
    /// takes the directory lock, loads the manifest, removes orphaned files,
    /// opens the live SSTables, and replays the WAL — truncating a torn tail or
    /// throwing <see cref="StorageCorruptionException"/> on mid-log corruption.
    /// Throws <see cref="InvalidOperationException"/> when the directory's
    /// recorded key comparer differs from the one in the options.
    /// </summary>
    public static LsmDb Open(StorageOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        var directory = Path.GetFullPath(options.DirectoryPath);
        Directory.CreateDirectory(directory);

        FileStream lockStream;
        try
        {
            lockStream = new FileStream(Path.Combine(directory, "LOCK"),
                FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        }
        catch (IOException ex)
        {
            throw new InvalidOperationException(
                $"The database directory '{directory}' is already open by another LsmDb instance.", ex);
        }

        var manifestFileExists = File.Exists(Path.Combine(directory, Manifest.FileName));
        try
        {
            var manifest = Manifest.Load(directory);

            // The on-disk order was laid down under the recorded comparer and
            // is meaningless under any other. A brand-new directory adopts
            // the comparer's identity from the options; an existing one must
            // match it exactly.
            if (!manifestFileExists)
            {
                manifest.ComparerName = options.ComparerName;
            }
            else if (!string.Equals(manifest.ComparerName, options.ComparerName, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"The database in '{directory}' was written with key comparer '{manifest.ComparerName}', " +
                    $"but the supplied options name '{options.ComparerName}'. Open with the comparer that wrote the directory.");
            }

            // A WAL generation the manifest names is the only durable copy of
            // its records until a flush commits them into a table. Its file
            // missing is the same class of damage as a missing table: loud,
            // never an empty replay that loses acknowledged writes quietly.
            if (manifestFileExists)
            {
                foreach (var generation in manifest.WalGenerations)
                {
                    if (!File.Exists(WalPath(directory, generation)))
                        throw new StorageCorruptionException(
                            $"The manifest lists WAL generation {generation}, but '{WalPath(directory, generation)}' is missing. The directory was damaged outside the engine.");
                }
            }

            CleanupOrphanFiles(directory, manifest);

            var tables = new List<SSTable.Reader>();
            foreach (var (id, level) in manifest.Tables)
            {
                var path = Path.Combine(directory, SSTable.FileName(id));
                if (!File.Exists(path))
                    throw new StorageCorruptionException(
                        $"The manifest lists SSTable {id}, but '{path}' is missing. The directory was damaged outside the engine.");
                var reader = SSTable.Reader.Open(id, path, options.Comparer);
                reader.Level = level;
                tables.Add(reader);
            }
            tables.Sort((a, b) => a.Id.CompareTo(b.Id));

            // Replay every live WAL generation, oldest first, into a fresh
            // memtable. A generation leaves the manifest's list only at the
            // commit that lands its records in a manifest table, so replay
            // never double-applies a committed flush. A pending flush (see
            // Manifest) is resolved here by discarding it: its generation is
            // replayed like any other, and its reserved table file — never
            // adopted by the manifest — was already removed above as an
            // orphan. The WAL, not the half-landed table, is the record.
            var memtable = new Memtable(options.Comparer);
            foreach (var generation in manifest.WalGenerations)
            {
                var generationPath = WalPath(directory, generation);
                var validLength = WalRecovery.Replay(generationPath, payload =>
                {
                    foreach (var op in RecordCodec.Decode(payload))
                        memtable.Apply(ToEntry(op));
                });
                if (File.Exists(generationPath) && new FileInfo(generationPath).Length > validLength)
                {
                    // Torn tail: cut the log back to the last valid record.
                    using var stream = new FileStream(generationPath, FileMode.Open, FileAccess.Write, FileShare.None);
                    stream.SetLength(validLength);
                    stream.Flush(flushToDisk: true);
                }
            }

            var walPath = WalPath(directory, manifest.WalGeneration);
            var wal = WriteAheadLog.OpenForAppend(walPath, options.FsyncPolicy, options.FsyncIntervalMilliseconds);

            // Make recovery resolutions durable: a v1 manifest is upgraded
            // in place, and a discarded pending flush is cleared, the first
            // time the directory opens cleanly.
            if (manifest.LoadedAsVersion1 || manifest.PendingFlush is not null)
            {
                manifest.PendingFlush = null;
                manifest.Save(directory);
            }

            return new LsmDb(options, directory, lockStream, manifest, tables, memtable, wal);
        }
        catch
        {
            lockStream.Dispose();
            throw;
        }
    }

    /// <summary>Returns a copy of the value for <paramref name="key"/>, or null when absent, deleted, or expired.</summary>
    public byte[]? Get(byte[] key)
    {
        ArgumentNullException.ThrowIfNull(key);
        _rw.EnterReadLock();
        try
        {
            ThrowIfUnavailable();
            var entry = FindNewest(key);
            if (entry is not { } found || found.Kind != EntryKind.Value || found.IsExpiredAt(_options.NowMs()))
                return null;
            return (byte[])found.Value!.Clone();
        }
        finally
        {
            _rw.ExitReadLock();
        }
    }

    /// <summary>Stores <paramref name="value"/> under <paramref name="key"/>, replacing any previous value and clearing any expiry it had.</summary>
    public void Put(byte[] key, byte[] value)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        _rw.EnterWriteLock();
        try
        {
            ThrowIfUnavailable();
            ThrowIfFlushFailed();
            _wal.Append(RecordCodec.EncodePut(key, value));
            _memtable.Put(key, value);
            FlushIfNeededLocked();
        }
        finally
        {
            _rw.ExitWriteLock();
        }
    }

    /// <summary>
    /// Stores <paramref name="value"/> under <paramref name="key"/> with a
    /// time-to-live: once the database clock passes now + <paramref name="ttl"/>
    /// the value reads as absent (lazily — no background work), never
    /// resurrects an older value beneath it, and is removed physically at the
    /// next flush or compaction that touches it. The deadline is persisted as
    /// an absolute timestamp, so it survives crashes and reopens exactly.
    /// </summary>
    public void Put(byte[] key, byte[] value, TimeSpan ttl)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        var deadline = DeadlineFromTtl(ttl);
        _rw.EnterWriteLock();
        try
        {
            ThrowIfUnavailable();
            ThrowIfFlushFailed();
            _wal.Append(RecordCodec.EncodePutWithExpiry(key, value, deadline));
            _memtable.Put(key, value, deadline);
            FlushIfNeededLocked();
        }
        finally
        {
            _rw.ExitWriteLock();
        }
    }

    /// <summary>
    /// Sets an expiry deadline on the live value stored under
    /// <paramref name="key"/>: the value keeps its bytes and gains a deadline
    /// of now + <paramref name="ttl"/> on the database clock. Returns false —
    /// and writes nothing — when the key has no live (unexpired) value.
    /// </summary>
    public bool Expire(byte[] key, TimeSpan ttl)
    {
        ArgumentNullException.ThrowIfNull(key);
        var deadline = DeadlineFromTtl(ttl);
        _rw.EnterWriteLock();
        try
        {
            ThrowIfUnavailable();
            ThrowIfFlushFailed();
            var entry = FindNewest(key);
            if (entry is not { } found || found.Kind != EntryKind.Value || found.IsExpiredAt(_options.NowMs()))
                return false;
            _wal.Append(RecordCodec.EncodePutWithExpiry(key, found.Value!, deadline));
            _memtable.Put(key, found.Value!, deadline);
            FlushIfNeededLocked();
            return true;
        }
        finally
        {
            _rw.ExitWriteLock();
        }
    }

    /// <summary>
    /// Deletes <paramref name="key"/>. Returns true when a live value existed.
    /// Deleting a key that does not exist (or whose value already expired)
    /// writes nothing and returns false.
    /// </summary>
    public bool Delete(byte[] key)
    {
        ArgumentNullException.ThrowIfNull(key);
        _rw.EnterWriteLock();
        try
        {
            ThrowIfUnavailable();
            ThrowIfFlushFailed();
            var entry = FindNewest(key);
            if (entry is not { } found || found.Kind != EntryKind.Value || found.IsExpiredAt(_options.NowMs()))
                return false;
            _wal.Append(RecordCodec.EncodeDelete(key));
            _memtable.Put(key, null);
            FlushIfNeededLocked();
            return true;
        }
        finally
        {
            _rw.ExitWriteLock();
        }
    }

    /// <summary>
    /// Deletes every key in [startInclusive, endExclusive) under the
    /// database's key ordering, as one durable operation: a range tombstone
    /// is appended to the WAL, applied to the memtable, and later flows
    /// through flush and compaction like a point tombstone, shadowing every
    /// older layer for the whole span until a bottom-level merge proves it
    /// can be dropped. Keys put after this call — inside the range included —
    /// are unaffected. An empty range (start equal to end) is a no-op that
    /// writes nothing; a reversed range throws.
    /// </summary>
    public void DeleteRange(byte[] startInclusive, byte[] endExclusive)
    {
        ArgumentNullException.ThrowIfNull(startInclusive);
        ArgumentNullException.ThrowIfNull(endExclusive);
        var cmp = _options.Comparer.Compare(startInclusive, endExclusive);
        if (cmp > 0)
            throw new ArgumentException("Range start must not be greater than range end under the database's key ordering.", nameof(startInclusive));
        if (cmp == 0)
            return;
        _rw.EnterWriteLock();
        try
        {
            ThrowIfUnavailable();
            ThrowIfFlushFailed();
            _wal.Append(RecordCodec.EncodeRangeDelete(startInclusive, endExclusive));
            _memtable.ApplyRangeDelete(startInclusive, endExclusive);
            FlushIfNeededLocked();
        }
        finally
        {
            _rw.ExitWriteLock();
        }
    }

    /// <summary>
    /// Applies a batch atomically: the batch is one WAL record, so after any
    /// crash either all of its operations are visible or none are. Relative
    /// TTLs in the batch become absolute deadlines against the database clock
    /// at the moment of this call.
    /// </summary>
    public void Write(WriteBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (batch.Count == 0)
            return;
        _rw.EnterWriteLock();
        try
        {
            ThrowIfUnavailable();
            ThrowIfFlushFailed();
            var nowMs = _options.NowMs();
            var ops = new List<RecordOp>(batch.Ops.Count);
            foreach (var op in batch.Ops)
            {
                if (op.IsRangeDelete)
                {
                    var cmp = _options.Comparer.Compare(op.Key, op.RangeEnd!);
                    if (cmp > 0)
                        throw new ArgumentException("A batch range delete's start must not be greater than its end under the database's key ordering.");
                    if (cmp == 0)
                        continue; // empty range: no-op
                }
                ops.Add(op.Ttl is { } ttl
                    ? new RecordOp(op.Key, op.Value) { ExpiresAtMs = DeadlineFromTtl(ttl, nowMs) }
                    : op);
            }
            if (ops.Count == 0)
                return;
            _wal.Append(RecordCodec.EncodeBatch(ops));
            foreach (var op in ops)
                _memtable.Apply(ToEntry(op));
            FlushIfNeededLocked();
        }
        finally
        {
            _rw.ExitWriteLock();
        }
    }

    /// <summary>
    /// A forward scan over a snapshot of the database taken at the moment of
    /// this call: later writes do not appear, later deletes do not hide rows.
    /// Entries come out in ascending key order under the database comparer,
    /// newest version per key, with deleted and already-expired keys absent.
    /// (Expiry is judged when the scan is enumerated, against the database
    /// clock: the snapshot freezes versions, not time.) Range bounds are
    /// optional and half-open [fromInclusive, toExclusive).
    /// </summary>
    public IEnumerable<KeyValuePair<byte[], byte[]>> Scan(byte[]? fromInclusive = null, byte[]? toExclusive = null)
    {
        List<IEnumerable<StoreEntry>> sources;
        _rw.EnterReadLock();
        try
        {
            ThrowIfUnavailable();
            sources = new List<IEnumerable<StoreEntry>> { _memtable.SnapshotEntries() };
            if (_pendingMemtable is not null)
                sources.Add(_pendingMemtable.SnapshotEntries());
            foreach (var table in TablesNewestFirst())
                sources.Add(table.EnumerateAll());
        }
        finally
        {
            _rw.ExitReadLock();
        }
        return ScanView(sources, fromInclusive, toExclusive);
    }

    /// <summary>
    /// Flushes the memtable to an SSTable. The rotation itself is instant
    /// (the memtable is sealed and a new one takes over; the table is then
    /// written by a background thread), and this call waits until the flush
    /// — and any flush already in flight ahead of it — has committed, so on
    /// return everything written so far is in a table. When a previous
    /// background flush has failed, this call retries it. A no-op when the
    /// memtable is empty and no flush is in flight.
    /// </summary>
    public void Flush()
    {
        _rw.EnterWriteLock();
        try
        {
            ThrowIfUnavailable();
            if (_flushFailure is not null)
            {
                // Retry the failed job: same generation, same reserved
                // table id, same frozen entries. The durable state never
                // changed, so the retry starts from an identical position.
                var failed = _pendingFlush!;
                _flushFailure = null;
                _pendingFlush = StartFlushJob(failed.Generation, failed.TableId, failed.Entries);
            }
            else if (_pendingFlush is null)
            {
                if (_memtable.Count > 0 || _memtable.RangeCount > 0)
                    RotateLocked();
            }
            else
            {
                // A flush is already running; ask its commit to rotate the
                // current memtable next, and wait for that job too.
                _flushRequested = true;
            }
        }
        finally
        {
            _rw.ExitWriteLock();
        }

        WaitForFlushToSettle();

        _rw.EnterReadLock();
        try
        {
            ThrowIfFlushFailed();
        }
        finally
        {
            _rw.ExitReadLock();
        }
    }

    /// <summary>
    /// Runs compaction until the live table set satisfies the configured
    /// strategy: under the tiered strategy, one full merge of every SSTable
    /// into one (overwritten values dropped, all tombstones purged — safe
    /// because the merge spans everything); under the leveled strategy,
    /// incremental merges until no level is over its budget. Safe to call at
    /// any time; reads and in-flight snapshot scans are unaffected (retired
    /// files are removed lazily). When a background flush is in flight it is
    /// awaited first.
    /// </summary>
    public void Compact()
    {
        _rw.EnterReadLock();
        try
        {
            ThrowIfUnavailable();
        }
        finally
        {
            _rw.ExitReadLock();
        }

        WaitForFlushToSettle();

        _rw.EnterWriteLock();
        try
        {
            ThrowIfUnavailable();
            ThrowIfFlushFailed();
            if (_options.CompactionStrategy == CompactionStrategy.Leveled)
                SettleLeveledLocked();
            else
                CompactLocked();
        }
        finally
        {
            _rw.ExitWriteLock();
        }
    }

    /// <summary>
    /// The number of live SSTables. When a background flush is in flight
    /// this waits for the flush pipeline to settle, so the count is the
    /// committed count, never a mid-flight guess. Reads and writes do not
    /// wait — only this observation point does.
    /// </summary>
    public int TableCount
    {
        get
        {
            WaitForFlushToSettle();
            _rw.EnterReadLock();
            try
            {
                return _tables.Count;
            }
            finally
            {
                _rw.ExitReadLock();
            }
        }
    }

    /// <summary>Approximate size of the current memtable in bytes.</summary>
    public long MemTableSizeBytes
    {
        get
        {
            _rw.EnterReadLock();
            try
            {
                return _memtable.SizeBytes;
            }
            finally
            {
                _rw.ExitReadLock();
            }
        }
    }

    /// <summary>
    /// Clean shutdown: rendezvous with any in-flight background flush (it
    /// commits normally), a final WAL fsync, then all files are released.
    /// Unflushed data is not forced to an SSTable — the WAL is the record,
    /// and the next <see cref="Open"/> replays it.
    /// </summary>
    public void Dispose()
    {
        if (_crashed)
            return; // SimulateCrash already dropped every handle.

        FlushJob? job;
        _rw.EnterWriteLock();
        try
        {
            if (_disposed)
                return;
            _disposed = true;
            job = _pendingFlush;
        }
        finally
        {
            _rw.ExitWriteLock();
        }

        // Rendezvous with the background flush, holding no lock: the
        // in-flight job commits normally (the disposed flag suppresses only
        // chaining a further rotation), so the directory is left in the
        // committed state rather than a half-rotated one, and teardown
        // below can never race the worker's manifest save or file moves.
        // The wait is bounded by the flush itself — one table write.
        job?.Done.Wait();

        _rw.EnterWriteLock();
        try
        {
            _wal.Dispose();
            foreach (var table in _tables)
                table.Dispose();
            foreach (var table in _retiredTables)
            {
                table.Dispose();
                TryDeleteFile(table.FilePath);
            }
            _lockStream.Dispose();
        }
        finally
        {
            _rw.ExitWriteLock();
        }
    }

    /// <summary>
    /// Test hook that fakes sudden process death: handles are dropped with no
    /// final fsync, no memtable flush, and no cleanup — exactly what the OS
    /// leaves behind when a process dies. The instance is unusable afterwards.
    /// What survives is precisely what the WAL had already made durable.
    /// </summary>
    internal void SimulateCrash()
    {
        _rw.EnterWriteLock();
        try
        {
            if (_disposed || _crashed)
                return;
            _crashed = true;
            // Process death does not wait for the flush thread: the job is
            // abandoned, and the worker's checkpoints keep it from touching
            // the directory again. Whatever state it froze mid-step is one
            // of the manifest v2 crash states recovery resolves.
            if (_pendingFlush is not null)
                _pendingFlush.Abandoned = true;
            _wal.Abandon();
            foreach (var table in _tables)
                table.Dispose();
            foreach (var table in _retiredTables)
                table.Dispose();
            _lockStream.Dispose();
        }
        finally
        {
            _rw.ExitWriteLock();
        }
    }

    /// <summary>
    /// The newest verdict for a key across every layer, or null when no layer
    /// mentions it. A returned entry may be a value (possibly expired), a
    /// point tombstone, or the covered-by-range marker — callers interpret.
    /// </summary>
    private StoreEntry? FindNewest(byte[] key)
    {
        if (_memtable.TryGet(key, out var point))
            return point.Value is null
                ? StoreEntry.Delete(key)
                : StoreEntry.Put(key, point.Value, point.ExpiresAtMs);
        if (_memtable.CoversRange(key))
            return StoreEntry.CoveredByRange(key);
        if (_pendingMemtable is not null)
        {
            if (_pendingMemtable.TryGet(key, out point))
                return point.Value is null
                    ? StoreEntry.Delete(key)
                    : StoreEntry.Put(key, point.Value, point.ExpiresAtMs);
            if (_pendingMemtable.CoversRange(key))
                return StoreEntry.CoveredByRange(key);
        }
        foreach (var table in TablesNewestFirst())
        {
            if (table.TryGet(key, out var entry))
                return entry;
        }
        return null;
    }

    /// <summary>
    /// Live tables in read/merge recency order: level ascending (level 0 is
    /// newest), then id descending within a level. Under the tiered strategy
    /// every table is level 0, so this is exactly newest-id-first.
    /// </summary>
    private List<SSTable.Reader> TablesNewestFirst() =>
        _tables.OrderBy(t => t.Level).ThenByDescending(t => t.Id).ToList();

    private IEnumerable<KeyValuePair<byte[], byte[]>> ScanView(
        List<IEnumerable<StoreEntry>> sources, byte[]? fromInclusive, byte[]? toExclusive)
    {
        var comparer = _options.Comparer;
        var nowMs = _options.NowMs();
        foreach (var entry in MergeHelper.MergeByRecency(sources, comparer))
        {
            if (toExclusive is not null && comparer.Compare(entry.Key, toExclusive) >= 0)
                yield break; // the merged stream is key-ordered: nothing later can qualify
            if (entry.Kind != EntryKind.Value || entry.IsExpiredAt(nowMs))
                continue; // tombstones, range tombstones, and expired values are absent
            if (fromInclusive is not null && comparer.Compare(entry.Key, fromInclusive) < 0)
                continue;
            yield return new KeyValuePair<byte[], byte[]>(entry.Key, entry.Value!);
        }
    }

    private long DeadlineFromTtl(TimeSpan ttl) => DeadlineFromTtl(ttl, _options.NowMs());

    private static long DeadlineFromTtl(TimeSpan ttl, long nowMs)
    {
        if (ttl <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ttl), "TTL must be positive.");
        if (ttl.TotalMilliseconds > 9.0e15)
            throw new ArgumentOutOfRangeException(nameof(ttl), "TTL is too large to express as a deadline.");
        return nowMs + (long)ttl.TotalMilliseconds;
    }

    private static StoreEntry ToEntry(RecordOp op)
    {
        if (op.IsRangeDelete)
            return StoreEntry.Range(op.Key, op.RangeEnd!);
        return op.Value is null
            ? StoreEntry.Delete(op.Key)
            : StoreEntry.Put(op.Key, op.Value, op.ExpiresAtMs);
    }

    private void FlushIfNeededLocked()
    {
        // Rotation is cheap and never blocks on the worker: when a flush is
        // already in flight the memtable simply keeps growing, and that
        // flush's commit chains the next rotation itself.
        if (_pendingFlush is null && _flushFailure is null &&
            _memtable.SizeBytes >= _options.MemTableSizeBytes)
            RotateLocked();
    }

    /// <summary>
    /// Seals the live memtable and its WAL generation and starts the
    /// background flush. The write lock is held. The order of steps is the
    /// crash-safety argument:
    ///   1. The next WAL generation's file exists on disk BEFORE the
    ///      manifest names it.
    ///   2. The manifest commits the rotation atomically: the sealed
    ///      generation stays listed and gains a pending-flush slot, the new
    ///      generation becomes the append target. From this instant, a
    ///      crash recovers by replaying both generations — the sealed one's
    ///      records are durable in exactly its WAL until the flush commits.
    ///   3. Only then does the in-memory state rotate (frozen memtable,
    ///      fresh memtable, WAL handle switched) and the worker start.
    /// </summary>
    private void RotateLocked()
    {
        var generation = _manifest.WalGeneration;
        var tableId = _manifest.NextSstId;
        var newGeneration = generation + 1;
        var newWalPath = WalPath(_directory, newGeneration);
        using (var create = new FileStream(newWalPath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            create.Flush(flushToDisk: true);

        _manifest.NextSstId = tableId + 1;
        _manifest.WalGenerations.Add(newGeneration);
        _manifest.PendingFlush = (generation, tableId);
        try
        {
            _manifest.Save(_directory);
        }
        catch
        {
            // The rotation did not commit: roll the in-memory manifest back
            // so the instance stays consistent with the file on disk.
            _manifest.NextSstId = tableId;
            _manifest.WalGenerations.Remove(newGeneration);
            _manifest.PendingFlush = null;
            TryDeleteFile(newWalPath);
            throw;
        }

        var job = StartFlushJob(generation, tableId, _memtable.SnapshotEntries());
        _pendingMemtable = _memtable;
        _memtable = new Memtable(_options.Comparer);

        var oldWal = _wal;
        _wal = WriteAheadLog.OpenForAppend(newWalPath, _options.FsyncPolicy, _options.FsyncIntervalMilliseconds);
        oldWal.Dispose();
    }

    /// <summary>Creates a flush job for the sealed generation, installs it as the pending flush, and starts its worker thread.</summary>
    private FlushJob StartFlushJob(long generation, long tableId,
        IReadOnlyList<StoreEntry> entries)
    {
        var job = new FlushJob { Generation = generation, TableId = tableId, Entries = entries };
        _pendingFlush = job;
        var thread = new Thread(() => RunFlushJob(job))
        {
            IsBackground = true,
            Name = "LsmDb background flush",
        };
        thread.Start();
        return job;
    }

    /// <summary>
    /// The background flush worker. The table write itself takes no locks:
    /// it streams the frozen entries to the reserved temp file, fsyncs, and
    /// renames. A value whose deadline passed by write time is written as a
    /// tombstone instead: the expired bytes leave the live set, but the
    /// shadow over older tables must survive the flush — only a merge that
    /// spans everything beneath may drop it for good. Only the commit
    /// (manifest swap, generation retirement) takes the write lock, briefly.
    /// Abandon checkpoints bracket every step that changes the directory,
    /// so a worker outliving a simulated crash can never adopt, delete, or
    /// overwrite anything afterwards.
    /// </summary>
    private void RunFlushJob(FlushJob job)
    {
        var tempPath = Path.Combine(_directory, SSTable.TempFileName(job.TableId));
        var finalPath = Path.Combine(_directory, SSTable.FileName(job.TableId));
        try
        {
            FlushJobStartingHook?.Invoke();
            FlushFailureHook?.Invoke(); // test-only fault injection

            var nowMs = _options.NowMs();
            using (var writer = new SSTable.Writer(tempPath, _options, job.Entries.Count))
            {
                foreach (var entry in job.Entries)
                {
                    writer.Add(entry.IsExpiredAt(nowMs)
                        ? StoreEntry.Delete(entry.Key)
                        : entry);
                }
                writer.Finish();
            }

            if (job.Abandoned)
            {
                TryDeleteFile(tempPath);
                job.Done.Set();
                return;
            }

            File.Move(tempPath, finalPath);
            Interlocked.Add(ref _totalTableBytesWritten, new FileInfo(finalPath).Length);

            FlushTableWrittenHook?.Invoke();

            if (job.Abandoned)
            {
                // Died after the rename: the table stays on disk unadopted.
                // The next open removes it as an orphan and replays the
                // generation's WAL — exactly the state the v2 recovery
                // tests construct by hand.
                job.Done.Set();
                return;
            }

            CommitFlush(job, finalPath);
        }
        catch (Exception ex)
        {
            if (job.Abandoned)
            {
                job.Done.Set();
                return;
            }
            FailFlush(job, ex);
        }
    }

    /// <summary>
    /// Lands a finished background flush. The write lock is taken briefly.
    /// In one manifest save: the reserved table is adopted, the pending slot
    /// clears, and every generation the flushed memtable covered (the sealed
    /// one, plus any older generations recovery had replayed into the same
    /// memtable) leaves the generation list — their records are in the table
    /// now, and their files are deleted. Before the job reports completion,
    /// a chained rotation is installed when one was requested or the live
    /// memtable crossed its threshold again, so waiters never observe a
    /// false idle pipeline.
    /// </summary>
    private void CommitFlush(FlushJob job, string finalPath)
    {
        _rw.EnterWriteLock();
        try
        {
            if (job.Abandoned || _crashed)
            {
                job.Done.Set();
                return;
            }

            var savedTables = _manifest.Tables.ToList();
            var savedGenerations = _manifest.WalGenerations.ToList();
            List<long> retired;
            try
            {
                retired = _manifest.WalGenerations.Where(g => g <= job.Generation).ToList();
                _manifest.Tables.Add((job.TableId, 0));
                _manifest.WalGenerations.RemoveAll(g => g <= job.Generation);
                _manifest.PendingFlush = null;
                _manifest.Save(_directory);
            }
            catch
            {
                _manifest.Tables.Clear();
                _manifest.Tables.AddRange(savedTables);
                _manifest.WalGenerations.Clear();
                _manifest.WalGenerations.AddRange(savedGenerations);
                _manifest.PendingFlush = (job.Generation, job.TableId);
                throw;
            }

            var reader = SSTable.Reader.Open(job.TableId, finalPath, _options.Comparer);
            reader.Level = 0;
            _tables.Add(reader);
            _pendingMemtable = null;

            foreach (var generation in retired)
                TryDeleteFile(WalPath(_directory, generation));

            if (_options.CompactionStrategy == CompactionStrategy.Leveled)
                SettleLeveledLocked();
            else if (_tables.Count >= _options.CompactionTableThreshold)
                CompactLocked();

            try
            {
                if (!_disposed &&
                    (_flushRequested || _memtable.SizeBytes >= _options.MemTableSizeBytes) &&
                    (_memtable.Count > 0 || _memtable.RangeCount > 0))
                {
                    RotateLocked(); // installs the chained job as _pendingFlush
                }
                else
                {
                    _pendingFlush = null;
                }
            }
            catch (Exception ex)
            {
                // This job committed; only the chained rotation failed. The
                // state on disk and in memory is consistent — the failure
                // surfaces through the same channel as a flush failure.
                _pendingFlush = null;
                _flushFailure = ex;
            }
            _flushRequested = false;
        }
        finally
        {
            _rw.ExitWriteLock();
        }
        job.Done.Set();
    }

    /// <summary>
    /// Records a background flush failure. The durable state is untouched:
    /// the generation is still listed, the pending slot still set, its WAL
    /// file intact, and the frozen memtable stays in the read path — reads
    /// keep working. What changes is only that writes stop (they throw,
    /// naming the failure) until <see cref="Flush"/> retries the job to
    /// completion or the instance is disposed; a reopen recovers every
    /// acknowledged write from the WALs either way. The generation is never
    /// silently dropped.
    /// </summary>
    private void FailFlush(FlushJob job, Exception error)
    {
        _rw.EnterWriteLock();
        try
        {
            if (job.Abandoned || _crashed)
                return;
            job.Error = error;
            _flushFailure = error;
            TryDeleteFile(Path.Combine(_directory, SSTable.TempFileName(job.TableId)));
        }
        finally
        {
            _rw.ExitWriteLock();
            job.Done.Set();
        }
    }

    /// <summary>
    /// Waits until no background flush is in flight — including jobs chained
    /// from a commit — or the in-flight job has failed (its caller surfaces
    /// the failure). Takes no lock while waiting; the worker needs only the
    /// write lock, briefly, so waiting can never deadlock it.
    /// </summary>
    private void WaitForFlushToSettle()
    {
        while (true)
        {
            FlushJob? job;
            _rw.EnterReadLock();
            try
            {
                if (_crashed)
                    return;
                job = _pendingFlush;
            }
            finally
            {
                _rw.ExitReadLock();
            }
            if (job is null)
                return;
            job.Done.Wait();
            if (job.Error is not null)
                return;
        }
    }

    /// <summary>
    /// The tiered strategy's compaction: a merge of EVERY live table into
    /// one. Because the merge spans every table, there is no older data
    /// beneath it for a tombstone to shadow: point tombstones, range
    /// tombstones, and expired values are all purged, and overwritten copies
    /// collapse to the newest value.
    ///
    /// The merge streams end to end. MergeByRecency holds one entry per
    /// input table in its queue; each surviving entry is written straight
    /// into the output table before the next is read. No merged copy of the
    /// data is ever held in memory: peak merge memory is one entry per
    /// input, plus the writer's sampled index (one key per
    /// IndexSampleInterval entries) and its bloom filter — proportional to
    /// the key count, never to the number of bytes the tables hold.
    ///
    /// A bloom filter must be sized before the first key is added, and the
    /// one count known at that point is the inputs' combined entry count —
    /// a strict upper bound on the merged count, since duplicates and
    /// tombstones only shrink it. The merged table can therefore carry a
    /// larger filter than an exact count would give, never a smaller one.
    /// </summary>
    private void CompactLocked()
    {
        if (_tables.Count == 0)
            return;

        var sources = new List<IEnumerable<StoreEntry>>();
        foreach (var table in TablesNewestFirst())
            sources.Add(table.EnumerateAll());

        long inputEntries = 0;
        foreach (var table in _tables)
            inputEntries += table.EntryCount;
        var bloomBound = inputEntries > int.MaxValue ? int.MaxValue : (int)inputEntries;

        var id = _manifest.NextSstId;
        var tempPath = Path.Combine(_directory, SSTable.TempFileName(id));
        var finalPath = Path.Combine(_directory, SSTable.FileName(id));
        var nowMs = _options.NowMs();
        long written = 0;
        using (var writer = new SSTable.Writer(tempPath, _options, Math.Max(bloomBound, 1)))
        {
            foreach (var entry in MergeHelper.MergeByRecency(sources, _options.Comparer))
            {
                if (entry.Kind != EntryKind.Value)
                    continue; // tombstones (point and range): purged by the full merge
                if (entry.IsExpiredAt(nowMs))
                    continue; // the expired newest version: dropped; older copies were suppressed by the merge
                writer.Add(entry);
                written++;
            }
            if (written > 0)
                writer.Finish();
        }

        var retired = _tables;
        if (written == 0)
        {
            // Every input entry was a tombstone or a shadowed/expired copy:
            // the merged result is empty. No table is created and no table
            // id is consumed — the (empty) temp file is simply discarded.
            TryDeleteFile(tempPath);
            _manifest.Tables.Clear();
            _manifest.Save(_directory);
            _tables = new List<SSTable.Reader>();
            _retiredTables.AddRange(retired);
            return;
        }

        File.Move(tempPath, finalPath);
        Interlocked.Add(ref _totalTableBytesWritten, new FileInfo(finalPath).Length);

        _manifest.Tables.Clear();
        _manifest.Tables.Add((id, 0));
        _manifest.NextSstId = id + 1;
        _manifest.Save(_directory);

        var reader = SSTable.Reader.Open(id, finalPath, _options.Comparer);
        reader.Level = 0;
        _tables = new List<SSTable.Reader> { reader };
        _retiredTables.AddRange(retired);
        // Retired readers stay open and their files stay on disk until Dispose
        // (or the next Open's orphan cleanup), so snapshot scans captured
        // before this compaction keep reading a consistent view.
    }

    /// <summary>One leveled compaction step: merge <see cref="Inputs"/> into a new set of tables at <see cref="TargetLevel"/>.</summary>
    private sealed record LeveledPick(List<SSTable.Reader> Inputs, int TargetLevel);

    /// <summary>
    /// The leveled strategy's settle loop: while any level is over its
    /// budget (level 0 by table count, deeper levels by bytes), run one
    /// incremental merge and re-evaluate. Each merge drains the condition
    /// that triggered it — level 0 empties into level 1; an over-budget
    /// level sheds a table's span into the level below — so the loop
    /// terminates; the iteration cap is only a safety valve.
    /// </summary>
    private void SettleLeveledLocked()
    {
        for (var iteration = 0; iteration < 256; iteration++)
        {
            var pick = PickLeveledLocked();
            if (pick is null)
                return;
            ExecuteLeveledCompactionLocked(pick);
        }
    }

    private LeveledPick? PickLeveledLocked()
    {
        var level0 = _tables.Where(t => t.Level == 0).ToList();
        if (level0.Count >= _options.CompactionTableThreshold)
        {
            // All of level 0 drains into level 1, plus every level-1 table
            // whose span the level-0 union touches (partition invariant:
            // the outputs must not overlap a surviving level-1 table).
            var inputs = new List<SSTable.Reader>(level0);
            var span = SpanOf(level0);
            inputs.AddRange(_tables.Where(t => t.Level == 1 && SpansOverlap(t, span)));
            return new LeveledPick(inputs, 1);
        }

        var maxLevel = _tables.Count == 0 ? 0 : _tables.Max(t => t.Level);
        for (var level = 1; level <= maxLevel; level++)
        {
            var levelTables = _tables.Where(t => t.Level == level).ToList();
            long size = 0;
            foreach (var table in levelTables)
                size += table.SizeBytes;
            if (size <= LevelBudget(level))
                continue;
            // Deterministic pick: the table with the smallest starting key.
            var picked = levelTables
                .OrderBy(t => t.MinKey, _options.Comparer)
                .ThenBy(t => t.Id)
                .First();
            var inputs = new List<SSTable.Reader> { picked };
            var span = SpanOf(picked);
            inputs.AddRange(_tables.Where(t => t.Level == level + 1 && SpansOverlap(t, span)));
            return new LeveledPick(inputs, level + 1);
        }
        return null;
    }

    private long LevelBudget(int level)
    {
        double budget = _options.LeveledBaseLevelSizeBytes;
        for (var i = 1; i < level; i++)
            budget *= _options.LeveledSizeRatio;
        return budget >= long.MaxValue ? long.MaxValue : (long)budget;
    }

    /// <summary>The key span a table can affect: [MinKey, MaxCoveredKey] (the end is a range tombstone's end when that reaches further).</summary>
    private (byte[] Start, byte[] End) SpanOf(SSTable.Reader table) => (table.MinKey, table.MaxCoveredKey);

    private (byte[] Start, byte[] End) SpanOf(List<SSTable.Reader> tables)
    {
        var comparer = _options.Comparer;
        var start = tables[0].MinKey;
        var end = tables[0].MaxCoveredKey;
        foreach (var table in tables)
        {
            if (comparer.Compare(table.MinKey, start) < 0)
                start = table.MinKey;
            if (comparer.Compare(table.MaxCoveredKey, end) > 0)
                end = table.MaxCoveredKey;
        }
        return (start, end);
    }

    private bool SpansOverlap(SSTable.Reader table, (byte[] Start, byte[] End) span)
    {
        var comparer = _options.Comparer;
        return comparer.Compare(table.MinKey, span.End) <= 0 &&
               comparer.Compare(span.Start, table.MaxCoveredKey) <= 0;
    }

    /// <summary>
    /// Executes one leveled merge. Inputs are consumed newest-first (upper
    /// level before the target level, newest id first within a level) and
    /// the merged stream is written to fresh tables at the target level,
    /// split at the configured target size.
    ///
    /// Tombstone discipline (the leveled difference from CompactLocked): a
    /// point or range tombstone may be dropped only when it is <i>provably</i>
    /// unshadowed — no live table outside the merge has a span that could
    /// hold an older copy it suppresses. Anything else is carried into the
    /// output, so a leveled merge never resurrects data. Expired values
    /// follow the same rule, except that when they may not be dropped they
    /// are converted to point tombstones (same shadow, no bytes kept).
    ///
    /// Crash safety mirrors the full merge: outputs are written to temp
    /// files, fsynced, renamed, and only then does one manifest save swap
    /// inputs for outputs. A crash before the save leaves the inputs live
    /// and the outputs orphans; after it, the reverse.
    /// </summary>
    private void ExecuteLeveledCompactionLocked(LeveledPick pick)
    {
        var comparer = _options.Comparer;
        var inputs = pick.Inputs
            .OrderBy(t => t.Level)
            .ThenByDescending(t => t.Id)
            .ToList();
        var inputSet = new HashSet<SSTable.Reader>(inputs);
        var nonInputs = _tables.Where(t => !inputSet.Contains(t)).ToList();

        bool PointDroppable(byte[] key) =>
            !nonInputs.Any(t => comparer.Compare(t.MinKey, key) <= 0 &&
                                comparer.Compare(key, t.MaxCoveredKey) <= 0);
        bool RangeDroppable(byte[] start, byte[] end) =>
            !nonInputs.Any(t => comparer.Compare(t.MinKey, end) < 0 &&
                                comparer.Compare(start, t.MaxCoveredKey) <= 0);

        var sources = new List<IEnumerable<StoreEntry>>();
        foreach (var table in inputs)
            sources.Add(table.EnumerateAll());
        long inputEntries = 0;
        foreach (var table in inputs)
            inputEntries += table.EntryCount;
        var bloomBound = inputEntries > int.MaxValue ? int.MaxValue : (int)Math.Max(inputEntries, 1);

        var nowMs = _options.NowMs();
        var outputs = new List<(long Id, string FinalPath)>();
        var nextId = _manifest.NextSstId;
        SSTable.Writer? writer = null;
        string? tempPath = null;
        long currentId = 0;

        void StartOutput()
        {
            currentId = nextId++;
            tempPath = Path.Combine(_directory, SSTable.TempFileName(currentId));
            writer = new SSTable.Writer(tempPath, _options, bloomBound);
        }

        void FinishOutput()
        {
            writer!.Finish();
            writer.Dispose();
            writer = null;
            var finalPath = Path.Combine(_directory, SSTable.FileName(currentId));
            File.Move(tempPath!, finalPath);
            Interlocked.Add(ref _totalTableBytesWritten, new FileInfo(finalPath).Length);
            outputs.Add((currentId, finalPath));
        }

        try
        {
            foreach (var entry in MergeHelper.MergeByRecency(sources, comparer))
            {
                StoreEntry? emit = entry.Kind switch
                {
                    EntryKind.Value when entry.IsExpiredAt(nowMs) =>
                        PointDroppable(entry.Key) ? null : StoreEntry.Delete(entry.Key),
                    EntryKind.Value => entry,
                    EntryKind.Tombstone =>
                        PointDroppable(entry.Key) ? null : entry,
                    EntryKind.RangeTombstone =>
                        RangeDroppable(entry.Key, entry.RangeEnd!) ? null : entry,
                    _ => entry,
                };
                if (emit is not { } output)
                    continue;
                if (writer is null)
                    StartOutput();
                writer!.Add(output);
                if (writer.BytesWritten >= _options.LeveledTargetTableSizeBytes)
                    FinishOutput();
            }
            if (writer is not null)
                FinishOutput();
        }
        catch
        {
            writer?.Dispose();
            if (tempPath is not null)
                TryDeleteFile(tempPath);
            throw;
        }

        // One manifest save swaps inputs for outputs atomically.
        var savedTables = _manifest.Tables.ToList();
        var savedNextId = _manifest.NextSstId;
        var inputIds = new HashSet<long>(inputs.Select(t => t.Id));
        try
        {
            _manifest.Tables.RemoveAll(t => inputIds.Contains(t.Id));
            foreach (var (id, _) in outputs)
                _manifest.Tables.Add((id, pick.TargetLevel));
            _manifest.NextSstId = nextId;
            _manifest.Save(_directory);
        }
        catch
        {
            _manifest.Tables.Clear();
            _manifest.Tables.AddRange(savedTables);
            _manifest.NextSstId = savedNextId;
            throw;
        }

        _tables.RemoveAll(t => inputIds.Contains(t.Id));
        foreach (var (id, finalPath) in outputs)
        {
            var reader = SSTable.Reader.Open(id, finalPath, comparer);
            reader.Level = pick.TargetLevel;
            _tables.Add(reader);
        }
        _tables.Sort((a, b) => a.Id.CompareTo(b.Id));
        _retiredTables.AddRange(inputs);
    }

    private void ThrowIfUnavailable()
    {
        if (_crashed)
            throw new InvalidOperationException(
                "This instance simulated a crash and is dead; open the directory again for a working instance.");
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    /// <summary>
    /// The write-side half of the background flush failure semantics (see
    /// FailFlush): while a failed flush is outstanding, writes throw —
    /// naming the original failure — instead of piling onto a pipeline
    /// whose oldest generation cannot land. Reads are not gated: the frozen
    /// memtable is intact and part of the read path.
    /// </summary>
    private void ThrowIfFlushFailed()
    {
        if (_flushFailure is not null)
            throw new InvalidOperationException(
                "A background flush failed, so writes are suspended: the generation it was flushing is " +
                "still fully durable in its WAL and nothing was lost. Call Flush() to retry the flush, " +
                "or Dispose and reopen.", _flushFailure);
    }

    private static string WalPath(string directory, long generation) =>
        Path.Combine(directory, $"wal-{generation}.log");

    private static void CleanupOrphanFiles(string directory, Manifest manifest)
    {
        foreach (var file in Directory.EnumerateFiles(directory))
        {
            var name = Path.GetFileName(file);
            if (name == Manifest.TempFileName)
            {
                TryDeleteFile(file);
            }
            else if (name.StartsWith("sst-", StringComparison.Ordinal) &&
                     name.EndsWith(".tmp", StringComparison.Ordinal))
            {
                TryDeleteFile(file);
            }
            else if (name.StartsWith("sst-", StringComparison.Ordinal) &&
                     name.EndsWith(".lsm", StringComparison.Ordinal) &&
                     TryParseId(name, out var sstId) && !manifest.HasTable(sstId))
            {
                TryDeleteFile(file);
            }
            else if (name.StartsWith("wal-", StringComparison.Ordinal) &&
                     name.EndsWith(".log", StringComparison.Ordinal) &&
                     TryParseId(name, out var walGen) && !manifest.WalGenerations.Contains(walGen))
            {
                TryDeleteFile(file);
            }
        }
    }

    private static bool TryParseId(string fileName, out long id)
    {
        id = 0;
        var core = fileName.Split('.')[0]; // "sst-000123" or "wal-3"
        var dash = core.IndexOf('-');
        return dash >= 0 && long.TryParse(core.AsSpan(dash + 1), out id);
    }

    private static void TryDeleteFile(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (IOException)
        {
            // Best effort: a later Open retries orphan cleanup.
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}
