using static StorageEngine.Tests.TestSupport;

namespace StorageEngine.Tests;

/// <summary>
/// Custom key comparators: the ordering is pluggable, its identity is
/// recorded in the manifest, and opening a directory under a different
/// identity than the recorded one is refused loudly — the on-disk order
/// would be meaningless under another ordering.
/// </summary>
public class ComparatorTests : DbTestBase
{
    /// <summary>Descending byte order: the exact reverse of the built-in ordering.</summary>
    private sealed class ReverseComparer : IComparer<byte[]>
    {
        public int Compare(byte[]? x, byte[]? y) =>
            ByteArrayComparer.Instance.Compare(y, x);
    }

    /// <summary>ASCII case-insensitive order: keys differing only in letter case are the same key.</summary>
    private sealed class CaseInsensitiveComparer : IComparer<byte[]>
    {
        public int Compare(byte[]? x, byte[]? y)
        {
            if (x is null || y is null)
                return ByteArrayComparer.Instance.Compare(x, y);
            var foldedX = x.Select(b => (byte)char.ToUpperInvariant((char)b)).ToArray();
            var foldedY = y.Select(b => (byte)char.ToUpperInvariant((char)b)).ToArray();
            return ByteArrayComparer.Instance.Compare(foldedX, foldedY);
        }
    }

    private StorageOptions ReverseOptions() =>
        Options(comparer: new ReverseComparer(), comparerName: "reverse-test");

    [Fact]
    public void ReverseComparer_OrderAppliesToScansReadsAndTables()
    {
        using (var db = LsmDb.Open(ReverseOptions()))
        {
            foreach (var key in new[] { "b", "d", "a", "e", "c" })
                db.Put(B(key), B($"v-{key}"));
            // Scan order is the comparer's: descending.
            Assert.Equal(new[] { "e", "d", "c", "b", "a" },
                db.Scan().Select(kv => S(kv.Key)).ToArray());
            Assert.Equal("v-c", S(db.Get(B("c"))!));
            db.Flush();
            Assert.Equal(new[] { "e", "d", "c", "b", "a" },
                db.Scan().Select(kv => S(kv.Key)).ToArray());
            Assert.Equal("v-a", S(db.Get(B("a"))!));
            db.Compact();
            Assert.Equal("v-e", S(db.Get(B("e"))!));
        }
        using (var db = LsmDb.Open(ReverseOptions()))
        {
            Assert.Equal(new[] { "e", "d", "c", "b", "a" },
                db.Scan().Select(kv => S(kv.Key)).ToArray());
        }
    }

    [Fact]
    public void ReverseComparer_ScanBounds_AreInComparerOrder()
    {
        using var db = LsmDb.Open(ReverseOptions());
        foreach (var key in new[] { "a", "b", "c", "d", "e" })
            db.Put(B(key), B("v"));
        // [from, to) under descending order: from "d" down to (excluding) "b".
        var keys = db.Scan(B("d"), B("b")).Select(kv => S(kv.Key)).ToArray();
        Assert.Equal(new[] { "d", "c" }, keys);
    }

    [Fact]
    public void Open_WithADifferentComparerNameThanRecorded_IsRefused()
    {
        using (var db = LsmDb.Open(ReverseOptions()))
        {
            db.Put(B("k"), B("v"));
        }
        // The default (bytewise) identity does not match the recorded one.
        var ex = Assert.Throws<InvalidOperationException>(() => LsmDb.Open(Options()));
        Assert.Contains("reverse-test", ex.Message);
        Assert.Contains("bytewise", ex.Message);
        // Neither does a different custom name over the same comparer object.
        Assert.Throws<InvalidOperationException>(() =>
            LsmDb.Open(Options(comparer: new ReverseComparer(), comparerName: "someone-else")));
        // The recorded identity opens fine.
        using (var db = LsmDb.Open(ReverseOptions()))
        {
            Assert.Equal("v", S(db.Get(B("k"))!));
        }
    }

    [Fact]
    public void Open_BytewiseDirectory_WithACustomComparer_IsRefused()
    {
        using (var db = LsmDb.Open(Options()))
        {
            db.Put(B("k"), B("v"));
        }
        // The directory recorded "bytewise" (the pre-existing default).
        Assert.Throws<InvalidOperationException>(() => LsmDb.Open(ReverseOptions()));
    }

    [Fact]
    public void HandWrittenManifest_ComparatorLine_GovernsTheOpen()
    {
        using (var db = LsmDb.Open(Options()))
        {
            db.Put(B("k"), B("v"));
            db.Flush();
        }
        var manifestPath = Path.Combine(Dir, "MANIFEST");
        var lines = File.ReadAllLines(manifestPath).ToList();
        var at = lines.FindIndex(l => l.StartsWith("comparator", StringComparison.Ordinal));
        Assert.True(at >= 0, "the engine records its comparer identity in the manifest");
        lines[at] = "comparator something-else";
        File.WriteAllLines(manifestPath, lines);

        Assert.Throws<InvalidOperationException>(() => LsmDb.Open(Options()));
        using (var db = LsmDb.Open(Options(comparer: new ReverseComparer(), comparerName: "something-else")))
        {
            // The identity matches; the data was laid down bytewise, so under
            // a one-key database the read still resolves — the refusal is
            // about identity, and identity is what was restored.
            Assert.Equal("v", S(db.Get(B("k"))!));
        }
    }

    [Fact]
    public void CustomComparer_WithoutAName_IsRejected()
    {
        Assert.Throws<ArgumentException>(() =>
            LsmDb.Open(Options(comparer: new ReverseComparer())));
        Assert.Throws<ArgumentException>(() =>
            LsmDb.Open(Options(comparerName: "name-without-comparer")));
    }

    [Fact]
    public void CaseInsensitiveComparer_KeysDifferingOnlyInCase_AreTheSameKey()
    {
        var options = Options(comparer: new CaseInsensitiveComparer(), comparerName: "ci-ascii-test");
        using (var db = LsmDb.Open(options))
        {
            db.Put(B("User:42"), B("eslam"));
            Assert.Equal("eslam", S(db.Get(B("USER:42"))!));
            Assert.Equal("eslam", S(db.Get(B("user:42"))!));
            db.Put(B("uSER:42"), B("overwritten"));
            Assert.Equal("overwritten", S(db.Get(B("User:42"))!));
            Assert.Single(db.Scan());
            db.Flush();
            Assert.Equal("overwritten", S(db.Get(B("USER:42"))!));
            Assert.True(db.Delete(B("user:42")));
            Assert.Null(db.Get(B("User:42")));
        }
        using (var db = LsmDb.Open(options))
        {
            Assert.Null(db.Get(B("USER:42")));
        }
    }

    [Fact]
    public void ReverseComparer_RangeDelete_FollowsComparerOrder()
    {
        using var db = LsmDb.Open(ReverseOptions());
        foreach (var key in new[] { "a", "b", "c", "m", "z" })
            db.Put(B(key), B("v"));
        // Under descending order, [m, a) spans the keys ordered from m down
        // to just above a: bytewise, that is {b, c, m}.
        db.DeleteRange(B("m"), B("a"));
        Assert.Null(db.Get(B("m")));
        Assert.Null(db.Get(B("c")));
        Assert.Null(db.Get(B("b")));
        Assert.Equal("v", S(db.Get(B("a"))!));
        Assert.Equal("v", S(db.Get(B("z"))!));
        db.Flush();
        Assert.Null(db.Get(B("c")));
        Assert.Equal(new[] { "z", "a" }, db.Scan().Select(kv => S(kv.Key)).ToArray());
    }
}
