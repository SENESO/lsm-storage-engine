using static StorageEngine.Tests.TestSupport;

namespace StorageEngine.Tests;

/// <summary>
/// Write-path semantics that only appear when operations interact across
/// storage layers: a batch arguing with itself, a tombstone meeting a later
/// reinsert, compaction running while the memtable holds the newest state,
/// and values whose size dwarfs the memtable they flush through.
/// </summary>
public class WriteSemanticsTests : DbTestBase
{
    [Fact]
    public void DeleteThenReinsert_AcrossFlushes_NewestValueWins()
    {
        using var db = LsmDb.Open(Options(compactionThreshold: 1000));
        db.Put(B("k"), B("v1"));
        db.Flush();

        Assert.True(db.Delete(B("k")));
        Assert.False(db.Delete(B("k"))); // already gone: no second tombstone
        db.Flush();                       // the tombstone itself lands in a table
        Assert.Null(db.Get(B("k")));

        db.Put(B("k"), B("v2"));          // reinsert over the tombstone table
        Assert.Equal("v2", S(db.Get(B("k"))!));
        db.Flush();
        db.Compact();
        Assert.Equal("v2", S(db.Get(B("k"))!));
        db.Dispose();

        using var reopened = LsmDb.Open(Options(compactionThreshold: 1000));
        Assert.Equal("v2", S(reopened.Get(B("k"))!));
        Assert.Single(reopened.Scan());
    }

    [Fact]
    public void Batch_DuplicateKeys_LastOperationWins_EvenAfterCrash()
    {
        var db = LsmDb.Open(Options());
        var batch = new WriteBatch()
            .Put(B("k"), B("first"))
            .Put(B("k"), B("second"))   // same key twice: the later put wins
            .Put(B("temp"), B("x"))
            .Delete(B("temp"))          // written and deleted by one batch
            .Delete(B("back"))
            .Put(B("back"), B("alive")); // deleted then rewritten by one batch
        db.Write(batch);

        Assert.Equal("second", S(db.Get(B("k"))!));
        Assert.Null(db.Get(B("temp")));
        Assert.Equal("alive", S(db.Get(B("back"))!));
        db.SimulateCrash();

        // Replay applies the batch's operations in order, so the recovered
        // state is identical — not just the same set of keys.
        using var reopened = LsmDb.Open(Options());
        Assert.Equal("second", S(reopened.Get(B("k"))!));
        Assert.Null(reopened.Get(B("temp")));
        Assert.Equal("alive", S(reopened.Get(B("back"))!));
    }

    [Fact]
    public void Batch_LargerThanMemtable_FlushesAndSurvivesReopen()
    {
        using var db = LsmDb.Open(Options(memTableBytes: 1024, compactionThreshold: 1000));
        var batch = new WriteBatch();
        for (var i = 0; i < 30; i++)
            batch.Put(B($"b-{i:D3}"), B(new string('z', 100)));
        db.Write(batch);

        Assert.True(db.TableCount >= 1, "a batch that overfills the memtable flushes it");
        for (var i = 0; i < 30; i++)
            Assert.Equal(new string('z', 100), S(db.Get(B($"b-{i:D3}"))!));
        db.Dispose();

        using var reopened = LsmDb.Open(Options(memTableBytes: 1024, compactionThreshold: 1000));
        for (var i = 0; i < 30; i++)
            Assert.Equal(new string('z', 100), S(reopened.Get(B($"b-{i:D3}"))!));
    }

    [Fact]
    public void Flush_OnEmptyMemtable_IsANoOp()
    {
        using var db = LsmDb.Open(Options(compactionThreshold: 1000));
        db.Flush();
        Assert.Equal(0, db.TableCount);
        Assert.Empty(Directory.GetFiles(Dir, "sst-*.lsm"));
        Assert.Equal(0, db.MemTableSizeBytes);

        db.Put(B("k"), B("v"));
        Assert.True(db.MemTableSizeBytes > 0);
        db.Flush();
        Assert.Equal(1, db.TableCount);
        Assert.Equal(0, db.MemTableSizeBytes);

        db.Flush(); // nothing new to flush: still exactly one table
        Assert.Equal(1, db.TableCount);

        Assert.False(db.Delete(B("ghost"))); // writes nothing...
        db.Flush();                          // ...so this flush is a no-op too
        Assert.Equal(1, db.TableCount);
    }

    [Fact]
    public void Compact_OnEmptyOrSingleTable_IsSafe()
    {
        using var db = LsmDb.Open(Options(compactionThreshold: 1000));
        db.Compact(); // no tables at all
        Assert.Equal(0, db.TableCount);

        db.Put(B("k"), B("v"));
        db.Flush();
        db.Compact(); // one table: merges into an equivalent single table
        Assert.Equal(1, db.TableCount);
        Assert.Equal("v", S(db.Get(B("k"))!));
        db.Dispose();

        using var reopened = LsmDb.Open(Options(compactionThreshold: 1000));
        Assert.Equal(1, reopened.TableCount);
        Assert.Equal("v", S(reopened.Get(B("k"))!));
    }

    [Fact]
    public void Compact_WhenEverythingIsDeleted_LeavesZeroTables()
    {
        using var db = LsmDb.Open(Options(compactionThreshold: 1000));
        db.Put(B("k1"), B("v1"));
        db.Put(B("k2"), B("v2"));
        db.Flush();
        Assert.True(db.Delete(B("k1")));
        Assert.True(db.Delete(B("k2")));
        db.Flush();

        // The merge's output is empty: no table is written at all, and the
        // keys are gone rather than shadowed by a surviving tombstone table.
        db.Compact();
        Assert.Equal(0, db.TableCount);
        Assert.Null(db.Get(B("k1")));
        Assert.Null(db.Get(B("k2")));
        Assert.Empty(db.Scan());

        db.Put(B("k3"), B("v3")); // the emptied database keeps working
        db.Dispose();

        using var reopened = LsmDb.Open(Options(compactionThreshold: 1000));
        Assert.Equal(0, reopened.TableCount);
        Assert.Equal("v3", S(reopened.Get(B("k3"))!));
        Assert.Null(reopened.Get(B("k1")));
    }

    [Fact]
    public void Compact_DoesNotDisturbLiveMemtable()
    {
        using var db = LsmDb.Open(Options(compactionThreshold: 1000));
        db.Put(B("k"), B("old"));
        db.Flush();

        db.Put(B("k"), B("new")); // newest version lives only in the memtable
        db.Compact();             // merges tables only — the memtable is not part of it
        Assert.Equal(1, db.TableCount);
        Assert.Equal("new", S(db.Get(B("k"))!));

        Assert.True(db.Delete(B("k"))); // a memtable tombstone over the merged table
        db.Compact();
        Assert.Null(db.Get(B("k")));
        db.Flush();
        db.Dispose();

        using var reopened = LsmDb.Open(Options(compactionThreshold: 1000));
        Assert.Null(reopened.Get(B("k")));
    }

    [Fact]
    public void Get_ReturnsACopy_CallerMutationDoesNotCorruptTheStore()
    {
        using var db = LsmDb.Open(Options());
        db.Put(B("k"), new byte[] { 1, 2, 3 });

        var first = db.Get(B("k"))!;
        first[0] = 99;

        Assert.Equal(new byte[] { 1, 2, 3 }, db.Get(B("k")));
    }

    [Fact]
    public void EmptyKey_RoundTripsThroughFlushAndReopen()
    {
        using var db = LsmDb.Open(Options(compactionThreshold: 1000));
        db.Put(B(""), B("under the empty key"));
        db.Put(B("a"), B("1"));
        Assert.Equal("under the empty key", S(db.Get(B(""))!));

        db.Flush();
        Assert.Equal("under the empty key", S(db.Get(B(""))!));
        // The empty key is the smallest possible key: it scans first.
        Assert.Equal("", S(db.Scan().First().Key));
        db.Dispose();

        using var reopened = LsmDb.Open(Options(compactionThreshold: 1000));
        Assert.Equal("under the empty key", S(reopened.Get(B(""))!));
        Assert.True(reopened.Delete(B("")));
        Assert.Null(reopened.Get(B("")));
    }

    [Fact]
    public void OversizedValue_AndLargeKey_RoundTripByteExact()
    {
        // One entry far larger than the memtable threshold: the put itself
        // flushes, and every later layer must carry it without truncation.
        var bigValue = new byte[3_000_000];
        var bigKey = new byte[32_768];
        for (var i = 0; i < bigValue.Length; i++)
            bigValue[i] = (byte)(i % 251);
        for (var i = 0; i < bigKey.Length; i++)
            bigKey[i] = (byte)(i % 253);

        using var db = LsmDb.Open(Options(memTableBytes: 64 * 1024, compactionThreshold: 1000));
        db.Put(bigKey, bigValue);
        Assert.True(db.TableCount >= 1, "an oversized entry flushes on its own put");
        Assert.Equal(bigValue, db.Get(bigKey));
        db.Compact();
        Assert.Equal(bigValue, db.Get(bigKey));
        db.Dispose();

        using var reopened = LsmDb.Open(Options(memTableBytes: 64 * 1024, compactionThreshold: 1000));
        Assert.Equal(bigValue, reopened.Get(bigKey));
    }

    [Fact]
    public void AbsentKeys_InsideAFlushedTablesRange_ReturnNull()
    {
        using var db = LsmDb.Open(Options(compactionThreshold: 1000));
        for (var i = 0; i < 200; i += 2)
            db.Put(B($"k-{i:D3}"), B($"v{i}"));
        db.Put(B("empty-in-table"), Array.Empty<byte>());
        db.Flush();

        // In-range misses walk the bloom filter, the sparse index, and the
        // forward scan — and must still answer "absent".
        for (var i = 1; i < 200; i += 2)
            Assert.Null(db.Get(B($"k-{i:D3}")));
        Assert.Null(db.Get(B("a-below-min")));
        Assert.Null(db.Get(B("z-above-max")));

        // An empty value stored in the table is present, not absent.
        var empty = db.Get(B("empty-in-table"));
        Assert.NotNull(empty);
        Assert.Empty(empty);
    }
}
