using static StorageEngine.Tests.TestSupport;

namespace StorageEngine.Tests;

public class ScanTests : DbTestBase
{
    [Fact]
    public void Scan_MergesMemtableAndTables_InKeyOrder()
    {
        using var db = LsmDb.Open(Options(compactionThreshold: 1000));
        db.Put(B("delta"), B("4"));
        db.Put(B("bravo"), B("2"));
        db.Flush();
        db.Put(B("alpha"), B("1"));
        db.Put(B("bravo"), B("2-new")); // overwrite of a flushed key
        db.Put(B("charlie"), B("3"));
        db.Delete(B("delta"));          // delete of a flushed key

        var scanned = db.Scan().Select(p => (S(p.Key), S(p.Value))).ToList();
        Assert.Equal(
            new List<(string, string)> { ("alpha", "1"), ("bravo", "2-new"), ("charlie", "3") },
            scanned);
    }

    [Fact]
    public void Scan_Snapshot_DoesNotSeeLaterWrites()
    {
        using var db = LsmDb.Open(Options(compactionThreshold: 1000));
        db.Put(B("a"), B("1"));
        db.Put(B("b"), B("2"));
        db.Flush();
        db.Put(B("c"), B("3"));

        var snapshot = db.Scan(); // snapshot is taken HERE
        db.Put(B("d"), B("4"));
        db.Delete(B("a"));
        db.Put(B("b"), B("2-changed"));

        var fromSnapshot = snapshot.Select(p => (S(p.Key), S(p.Value))).ToList();
        Assert.Equal(
            new List<(string, string)> { ("a", "1"), ("b", "2"), ("c", "3") },
            fromSnapshot);

        var live = db.Scan().Select(p => (S(p.Key), S(p.Value))).ToList();
        Assert.Equal(
            new List<(string, string)> { ("b", "2-changed"), ("c", "3"), ("d", "4") },
            live);
    }

    [Fact]
    public void Scan_Range_RespectsHalfOpenBounds()
    {
        using var db = LsmDb.Open(Options());
        foreach (var key in new[] { "a", "b", "c", "d", "e" })
            db.Put(B(key), B(key.ToUpperInvariant()));

        var middle = db.Scan(B("b"), B("d")).Select(p => S(p.Key)).ToList();
        Assert.Equal(new List<string> { "b", "c" }, middle);

        var tail = db.Scan(B("d")).Select(p => S(p.Key)).ToList();
        Assert.Equal(new List<string> { "d", "e" }, tail);
    }

    [Fact]
    public void Scan_EmptyDatabase_ReturnsNothing()
    {
        using var db = LsmDb.Open(Options());
        Assert.Empty(db.Scan());
    }
}
