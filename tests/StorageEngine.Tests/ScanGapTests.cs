using static StorageEngine.Tests.TestSupport;

namespace StorageEngine.Tests;

/// <summary>
/// Scan edges the first suite did not pin: one-sided ranges, ranges that
/// cannot contain anything, and a snapshot taken across a compaction —
/// DESIGN.md promises the snapshot keeps reading its consistent view while
/// the compaction retires the tables underneath it.
/// </summary>
public class ScanGapTests : DbTestBase
{
    [Fact]
    public void Scan_ToExclusiveOnly_ReturnsThePrefix()
    {
        using var db = LsmDb.Open(Options());
        foreach (var key in new[] { "a", "b", "c", "d", "e" })
            db.Put(B(key), B(key));

        var prefix = db.Scan(null, B("c")).Select(p => S(p.Key)).ToList();
        Assert.Equal(new List<string> { "a", "b" }, prefix);
    }

    [Fact]
    public void Scan_EmptyOrInvertedRange_ReturnsNothing()
    {
        using var db = LsmDb.Open(Options());
        foreach (var key in new[] { "a", "b", "c", "d", "e" })
            db.Put(B(key), B(key));

        Assert.Empty(db.Scan(B("c"), B("c"))); // from == to: half-open means empty
        Assert.Empty(db.Scan(B("d"), B("b"))); // from > to
        Assert.Empty(db.Scan(B("x"), B("z"))); // a well-formed range past the data
    }

    [Fact]
    public void Scan_Snapshot_SurvivesCompaction()
    {
        using var db = LsmDb.Open(Options(compactionThreshold: 1000));
        db.Put(B("a"), B("1"));
        db.Put(B("b"), B("2"));
        db.Flush();
        db.Put(B("c"), B("3"));

        var snapshot = db.Scan(); // captured before the compaction
        db.Compact();             // retires the table the snapshot reads from
        db.Put(B("d"), B("4"));
        db.Delete(B("a"));

        var fromSnapshot = snapshot.Select(p => (S(p.Key), S(p.Value))).ToList();
        Assert.Equal(
            new List<(string, string)> { ("a", "1"), ("b", "2"), ("c", "3") },
            fromSnapshot);

        var live = db.Scan().Select(p => (S(p.Key), S(p.Value))).ToList();
        Assert.Equal(
            new List<(string, string)> { ("b", "2"), ("c", "3"), ("d", "4") },
            live);
    }
}
