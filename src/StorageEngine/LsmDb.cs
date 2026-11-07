namespace StorageEngine;

/// <summary>
/// A log-structured merge-tree (LSM) key-value store for byte-array keys and
/// values, in a single directory:
///
///   writes ──► WAL (append + fsync policy) ──► memtable (sorted, in memory)
///   memtable full ──► flush ──► immutable SSTable (sparse index + bloom filter)
///   too many SSTables ──► full-merge compaction (newest value wins,
///   tombstones purged — a full merge has nothing older beneath it)
///
/// Reads check the memtable, then SSTables newest-first. One process may open
/// a directory at a time (an OS file lock enforces it); within the process any
/// number of reader threads may run concurrently with the writer.
///
/// Ownership: key and value arrays passed to <see cref="Put"/> and
/// <see cref="Write"/> become the property of the store — do not mutate them
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
    private List<SSTable.Reader> _tables; // ascending id; the newest table is last
    private Memtable _memtable;
    private WriteAheadLog _wal;
    private bool _disposed;
    private bool _crashed;

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

    /// <summary>One in-flight background flush: a sealed WAL generation being written into its reserved table.</summary>
    private sealed class FlushJob
    {
        public required long Generation { get; init; }
        public required long TableId { get; init; }
        public required IReadOnlyList<KeyValuePair<byte[], byte[]?>> Entries { get; init; }
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
            foreach (var id in manifest.SstIds)
            {
                var path = Path.Combine(directory, SSTable.FileName(id));
                if (!File.Exists(path))
                    throw new StorageCorruptionException(
                        $"The manifest lists SSTable {id}, but '{path}' is missing. The directory was damaged outside the engine.");
                tables.Add(SSTable.Reader.Open(id, path));
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
            var memtable = new Memtable();
            foreach (var generation in manifest.WalGenerations)
            {
                var generationPath = WalPath(directory, generation);
                var validLength = WalRecovery.Replay(generationPath, payload =>
                {
                    foreach (var op in RecordCodec.Decode(payload))
                        memtable.Put(op.Key, op.Value);
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

    /// <summary>Returns a copy of the value for <paramref name="key"/>, or null when absent (or deleted).</summary>
    public byte[]? Get(byte[] key)
    {
        ArgumentNullException.ThrowIfNull(key);
        _rw.EnterReadLock();
        try
        {
            ThrowIfUnavailable();
            var value = GetInternal(key);
            return value is null ? null : (byte[])value.Clone();
        }
        finally
        {
            _rw.ExitReadLock();
        }
    }

    /// <summary>Stores <paramref name="value"/> under <paramref name="key"/>, replacing any previous value.</summary>
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
    /// Deletes <paramref name="key"/>. Returns true when a live value existed.
    /// Deleting a key that does not exist writes nothing and returns false.
    /// </summary>
    public bool Delete(byte[] key)
    {
        ArgumentNullException.ThrowIfNull(key);
        _rw.EnterWriteLock();
        try
        {
            ThrowIfUnavailable();
            ThrowIfFlushFailed();
            if (GetInternal(key) is null)
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
    /// Applies a batch atomically: the batch is one WAL record, so after any
    /// crash either all of its operations are visible or none are.
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
            _wal.Append(RecordCodec.EncodeBatch(batch.Ops));
            foreach (var op in batch.Ops)
                _memtable.Put(op.Key, op.Value);
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
    /// Entries come out in ascending key order, newest version per key, with
    /// deleted keys absent. Range bounds are optional and half-open
    /// [fromInclusive, toExclusive).
    /// </summary>
    public IEnumerable<KeyValuePair<byte[], byte[]>> Scan(byte[]? fromInclusive = null, byte[]? toExclusive = null)
    {
        List<IEnumerable<KeyValuePair<byte[], byte[]?>>> sources;
        _rw.EnterReadLock();
        try
        {
            ThrowIfUnavailable();
            sources = new List<IEnumerable<KeyValuePair<byte[], byte[]?>>> { _memtable.SnapshotEntries() };
            if (_pendingMemtable is not null)
                sources.Add(_pendingMemtable.SnapshotEntries());
            for (var i = _tables.Count - 1; i >= 0; i--)
                sources.Add(_tables[i].EnumerateAll());
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
                if (_memtable.Count > 0)
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
    /// Merges every SSTable into one, dropping overwritten values and purging
    /// all tombstones. Safe to call at any time; reads and in-flight snapshot
    /// scans are unaffected (retired files are removed lazily). When a
    /// background flush is in flight it is awaited first, so the merge spans
    /// every table flushed so far.
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

    private byte[]? GetInternal(byte[] key)
    {
        if (_memtable.TryGet(key, out var value))
            return value;
        if (_pendingMemtable is not null && _pendingMemtable.TryGet(key, out value))
            return value;
        for (var i = _tables.Count - 1; i >= 0; i--)
        {
            if (_tables[i].TryGet(key, out var tableValue))
                return tableValue;
        }
        return null;
    }

    private static IEnumerable<KeyValuePair<byte[], byte[]>> ScanView(
        List<IEnumerable<KeyValuePair<byte[], byte[]?>>> sources, byte[]? fromInclusive, byte[]? toExclusive)
    {
        foreach (var entry in MergeHelper.MergeByRecency(sources))
        {
            if (entry.Value is null)
                continue; // tombstone: the key is deleted in this snapshot
            if (fromInclusive is not null && ByteArrayComparer.Instance.Compare(entry.Key, fromInclusive) < 0)
                continue;
            if (toExclusive is not null && ByteArrayComparer.Instance.Compare(entry.Key, toExclusive) >= 0)
                yield break;
            yield return new KeyValuePair<byte[], byte[]>(entry.Key, entry.Value);
        }
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
        _memtable = new Memtable();

        var oldWal = _wal;
        _wal = WriteAheadLog.OpenForAppend(newWalPath, _options.FsyncPolicy, _options.FsyncIntervalMilliseconds);
        oldWal.Dispose();
    }

    /// <summary>Creates a flush job for the sealed generation, installs it as the pending flush, and starts its worker thread.</summary>
    private FlushJob StartFlushJob(long generation, long tableId,
        IReadOnlyList<KeyValuePair<byte[], byte[]?>> entries)
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
    /// renames. Only the commit (manifest swap, generation retirement)
    /// takes the write lock, briefly. Abandon checkpoints bracket every
    /// step that changes the directory, so a worker outliving a simulated
    /// crash can never adopt, delete, or overwrite anything afterwards.
    /// </summary>
    private void RunFlushJob(FlushJob job)
    {
        var tempPath = Path.Combine(_directory, SSTable.TempFileName(job.TableId));
        var finalPath = Path.Combine(_directory, SSTable.FileName(job.TableId));
        try
        {
            FlushJobStartingHook?.Invoke();
            FlushFailureHook?.Invoke(); // test-only fault injection

            using (var writer = new SSTable.Writer(tempPath, _options, job.Entries.Count))
            {
                foreach (var (key, value) in job.Entries)
                    writer.Add(key, value);
                writer.Finish();
            }

            if (job.Abandoned)
            {
                TryDeleteFile(tempPath);
                job.Done.Set();
                return;
            }

            File.Move(tempPath, finalPath);

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

            var savedIds = _manifest.SstIds.ToList();
            var savedGenerations = _manifest.WalGenerations.ToList();
            List<long> retired;
            try
            {
                retired = _manifest.WalGenerations.Where(g => g <= job.Generation).ToList();
                _manifest.SstIds.Add(job.TableId);
                _manifest.WalGenerations.RemoveAll(g => g <= job.Generation);
                _manifest.PendingFlush = null;
                _manifest.Save(_directory);
            }
            catch
            {
                _manifest.SstIds.Clear();
                _manifest.SstIds.AddRange(savedIds);
                _manifest.WalGenerations.Clear();
                _manifest.WalGenerations.AddRange(savedGenerations);
                _manifest.PendingFlush = (job.Generation, job.TableId);
                throw;
            }

            _tables.Add(SSTable.Reader.Open(job.TableId, finalPath));
            _pendingMemtable = null;

            foreach (var generation in retired)
                TryDeleteFile(WalPath(_directory, generation));

            if (_tables.Count >= _options.CompactionTableThreshold)
                CompactLocked();

            try
            {
                if (!_disposed &&
                    (_flushRequested || _memtable.SizeBytes >= _options.MemTableSizeBytes) &&
                    _memtable.Count > 0)
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

    private void CompactLocked()
    {
        if (_tables.Count == 0)
            return;

        var sources = new List<IEnumerable<KeyValuePair<byte[], byte[]?>>>();
        for (var i = _tables.Count - 1; i >= 0; i--)
            sources.Add(_tables[i].EnumerateAll());

        // A compaction here always spans EVERY table, so there is no older data
        // beneath the merge for a tombstone to shadow: tombstones are purged,
        // and overwritten copies collapse to the newest value.
        //
        // The merge streams end to end. MergeByRecency holds one entry per
        // input table in its queue; each surviving entry is written straight
        // into the output table before the next is read. No merged copy of the
        // data is ever held in memory: peak merge memory is one entry per
        // input, plus the writer's sampled index (one key per
        // IndexSampleInterval entries) and its bloom filter — proportional to
        // the key count, never to the number of bytes the tables hold.
        //
        // A bloom filter must be sized before the first key is added, and the
        // one count known at that point is the inputs' combined entry count —
        // a strict upper bound on the merged count, since duplicates and
        // tombstones only shrink it. The merged table can therefore carry a
        // larger filter than an exact count would give, never a smaller one.
        long inputEntries = 0;
        foreach (var table in _tables)
            inputEntries += table.EntryCount;
        var bloomBound = inputEntries > int.MaxValue ? int.MaxValue : (int)inputEntries;

        var id = _manifest.NextSstId;
        var tempPath = Path.Combine(_directory, SSTable.TempFileName(id));
        var finalPath = Path.Combine(_directory, SSTable.FileName(id));
        long written = 0;
        using (var writer = new SSTable.Writer(tempPath, _options, Math.Max(bloomBound, 1)))
        {
            foreach (var entry in MergeHelper.MergeByRecency(sources))
            {
                if (entry.Value is null)
                    continue; // tombstone: purged by the full merge (see above)
                writer.Add(entry.Key, entry.Value);
                written++;
            }
            if (written > 0)
                writer.Finish();
        }

        var retired = _tables;
        if (written == 0)
        {
            // Every input entry was a tombstone or a shadowed copy: the merged
            // result is empty. No table is created and no table id is consumed
            // — the (empty) temp file is simply discarded.
            TryDeleteFile(tempPath);
            _manifest.SstIds.Clear();
            _manifest.Save(_directory);
            _tables = new List<SSTable.Reader>();
            _retiredTables.AddRange(retired);
            return;
        }

        File.Move(tempPath, finalPath);

        _manifest.SstIds.Clear();
        _manifest.SstIds.Add(id);
        _manifest.NextSstId = id + 1;
        _manifest.Save(_directory);

        _tables = new List<SSTable.Reader> { SSTable.Reader.Open(id, finalPath) };
        _retiredTables.AddRange(retired);
        // Retired readers stay open and their files stay on disk until Dispose
        // (or the next Open's orphan cleanup), so snapshot scans captured
        // before this compaction keep reading a consistent view.
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
                     TryParseId(name, out var sstId) && !manifest.SstIds.Contains(sstId))
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
