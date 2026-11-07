using static StorageEngine.Tests.TestSupport;

namespace StorageEngine.Tests;

public class FlushAndCompactionTests : DbTestBase
{
    [Fact]
    public void Flush_WritesSSTable_AndReadsBackFromDisk()
    {
        using var db = LsmDb.Open(Options(memTableBytes: 1024, compactionThreshold: 1000));
        for (var i = 0; i < 60; i++)
            db.Put(B($"f-{i:D3}"), B(new string('x', 90)));

        Assert.True(db.TableCount >= 1, "expected the small memtable to flush at least once");
        for (var i = 0; i < 60; i++)
            Assert.Equal(new string('x', 90), S(db.Get(B($"f-{i:D3}"))!));
    }

    [Fact]
    public void Overwrite_AcrossFlushes_NewestVersionWins()
    {
        using var db = LsmDb.Open(Options(compactionThreshold: 1000));
        db.Put(B("k"), B("v1"));
        db.Flush();
        db.Put(B("k"), B("v2"));
        Assert.Equal("v2", S(db.Get(B("k"))!));
        db.Flush();
        Assert.Equal("v2", S(db.Get(B("k"))!));
        db.Dispose();

        using var reopened = LsmDb.Open(Options(compactionThreshold: 1000));
        Assert.Equal("v2", S(reopened.Get(B("k"))!));
    }

    [Fact]
    public void Delete_AcrossFlushes_TombstoneShadowsOlderTable()
    {
        using var db = LsmDb.Open(Options(compactionThreshold: 1000));
        db.Put(B("k"), B("v"));
        db.Flush();
        Assert.True(db.Delete(B("k")));
        Assert.Null(db.Get(B("k")));
        db.Flush(); // the tombstone itself lands in a table
        Assert.Null(db.Get(B("k")));
        db.Compact(); // full merge: the tombstone is purged, the value is gone for good
        Assert.Null(db.Get(B("k")));
        db.Dispose();

        using var reopened = LsmDb.Open(Options(compactionThreshold: 1000));
        Assert.Null(reopened.Get(B("k")));
    }

    [Fact]
    public void Compaction_PreservesTheExactFinalState()
    {
        using var db = LsmDb.Open(Options(memTableBytes: 512, compactionThreshold: 1000));
        var model = new SortedDictionary<string, string>(StringComparer.Ordinal);

        void Apply(string key, string? value)
        {
            if (value is null)
            {
                db.Delete(B(key));
                model.Remove(key);
            }
            else
            {
                db.Put(B(key), B(value));
                model[key] = value;
            }
        }

        // Several rounds of writes, overwrites, and deletes over ~60 keys —
        // enough to stack up many tables at a 512-byte memtable.
        for (var round = 0; round < 5; round++)
        {
            for (var i = 0; i < 60; i++)
            {
                var key = $"user-{i:D3}";
                if ((i + round) % 7 == 0)
                    Apply(key, null);
                else
                    Apply(key, $"round{round}-value{i}");
            }
        }
        Assert.True(db.TableCount > 1, "the test is meaningless without several tables to merge");

        db.Compact();
        Assert.Equal(1, db.TableCount);

        foreach (var (key, value) in model)
            Assert.Equal(value, S(db.Get(B(key))!));
        for (var i = 0; i < 60; i++)
        {
            var key = $"user-{i:D3}";
            if (!model.ContainsKey(key))
                Assert.Null(db.Get(B(key)));
        }

        var scanned = db.Scan().Select(p => (S(p.Key), S(p.Value))).ToList();
        Assert.Equal(model.Select(p => (p.Key, p.Value)).ToList(), scanned);
        db.Dispose();

        // The merged table is durable: reopening shows the identical state.
        using var reopened = LsmDb.Open(Options(memTableBytes: 512, compactionThreshold: 1000));
        Assert.Equal(1, reopened.TableCount);
        var rescanned = reopened.Scan().Select(p => (S(p.Key), S(p.Value))).ToList();
        Assert.Equal(model.Select(p => (p.Key, p.Value)).ToList(), rescanned);
    }

    [Fact]
    public void AutoCompaction_KeepsTableCountAtTheThreshold()
    {
        using var db = LsmDb.Open(Options(memTableBytes: 1024, compactionThreshold: 2));
        for (var i = 0; i < 200; i++)
            db.Put(B($"a-{i:D4}"), B(new string('y', 100)));

        // Every flush past the first compacts back down to a single table.
        Assert.Equal(1, db.TableCount);
        for (var i = 0; i < 200; i++)
            Assert.Equal(new string('y', 100), S(db.Get(B($"a-{i:D4}"))!));
    }
}
