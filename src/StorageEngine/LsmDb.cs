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

        try
        {
            var manifest = Manifest.Load(directory);
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

            // Replay the WAL into a fresh memtable. Records already reflected
            // in a manifest-listed table never share a WAL generation with the
            // table (see Flush), so replay cannot double-apply a flush.
            var memtable = new Memtable();
            var walPath = WalPath(directory, manifest.WalGeneration);
            var validLength = WalRecovery.Replay(walPath, payload =>
            {
                foreach (var op in RecordCodec.Decode(payload))
                    memtable.Put(op.Key, op.Value);
            });
            if (File.Exists(walPath) && new FileInfo(walPath).Length > validLength)
            {
                // Torn tail: cut the log back to the last valid record.
                using var stream = new FileStream(walPath, FileMode.Open, FileAccess.Write, FileShare.None);
                stream.SetLength(validLength);
                stream.Flush(flushToDisk: true);
            }

            var wal = WriteAheadLog.OpenForAppend(walPath, options.FsyncPolicy, options.FsyncIntervalMilliseconds);
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
            for (var i = _tables.Count - 1; i >= 0; i--)
                sources.Add(_tables[i].EnumerateAll());
        }
        finally
        {
            _rw.ExitReadLock();
        }
        return ScanView(sources, fromInclusive, toExclusive);
    }

    /// <summary>Flushes the memtable to an SSTable now (a no-op when the memtable is empty).</summary>
    public void Flush()
    {
        _rw.EnterWriteLock();
        try
        {
            ThrowIfUnavailable();
            FlushLocked();
        }
        finally
        {
            _rw.ExitWriteLock();
        }
    }

    /// <summary>
    /// Merges every SSTable into one, dropping overwritten values and purging
    /// all tombstones. Safe to call at any time; reads and in-flight snapshot
    /// scans are unaffected (retired files are removed lazily).
    /// </summary>
    public void Compact()
    {
        _rw.EnterWriteLock();
        try
        {
            ThrowIfUnavailable();
            CompactLocked();
        }
        finally
        {
            _rw.ExitWriteLock();
        }
    }

    /// <summary>The number of live SSTables.</summary>
    public int TableCount
    {
        get
        {
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
    /// Clean shutdown: a final WAL fsync, then all files are released. Data is
    /// not flushed to an SSTable — the WAL is the record, and the next
    /// <see cref="Open"/> replays it.
    /// </summary>
    public void Dispose()
    {
        if (_crashed)
            return; // SimulateCrash already dropped every handle.
        _rw.EnterWriteLock();
        try
        {
            if (_disposed)
                return;
            _disposed = true;
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
        if (_memtable.SizeBytes >= _options.MemTableSizeBytes)
            FlushLocked();
    }

    private void FlushLocked()
    {
        if (_memtable.Count == 0)
            return;

        var entries = _memtable.SnapshotEntries();
        var id = _manifest.NextSstId;
        var tempPath = Path.Combine(_directory, SSTable.TempFileName(id));
        var finalPath = Path.Combine(_directory, SSTable.FileName(id));
        using (var writer = new SSTable.Writer(tempPath, _options, entries.Count))
        {
            foreach (var (key, value) in entries)
                writer.Add(key, value);
            writer.Finish();
        }
        File.Move(tempPath, finalPath);

        // The next WAL generation must exist on disk BEFORE the manifest points
        // at it; after the manifest moves, the old generation's records live in
        // the new table and its file becomes an orphan.
        var newGeneration = _manifest.WalGeneration + 1;
        var newWalPath = WalPath(_directory, newGeneration);
        using (var create = new FileStream(newWalPath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            create.Flush(flushToDisk: true);

        _manifest.SstIds.Add(id);
        _manifest.NextSstId = id + 1;
        _manifest.WalGeneration = newGeneration;
        _manifest.Save(_directory);

        var oldWal = _wal;
        var oldWalPath = WalPath(_directory, newGeneration - 1);
        _wal = WriteAheadLog.OpenForAppend(newWalPath, _options.FsyncPolicy, _options.FsyncIntervalMilliseconds);
        oldWal.Dispose();
        TryDeleteFile(oldWalPath);

        _tables.Add(SSTable.Reader.Open(id, finalPath));
        _memtable = new Memtable();

        if (_tables.Count >= _options.CompactionTableThreshold)
            CompactLocked();
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
                     TryParseId(name, out var walGen) && walGen != manifest.WalGeneration)
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
