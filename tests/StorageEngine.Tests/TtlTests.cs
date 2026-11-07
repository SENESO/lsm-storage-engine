using static StorageEngine.Tests.TestSupport;

namespace StorageEngine.Tests;

/// <summary>
/// Per-key TTL: values carry an absolute deadline (persisted in the WAL and
/// the tables), reads honor it lazily against the database clock, an expired
/// newest version never resurrects an older one, and flush/compaction remove
/// expired data physically. All time movement is a manually advanced fake
/// clock — no wall-clock sleeps.
/// </summary>
public class TtlTests : DbTestBase
{
    private readonly FakeClock _clock = new();

    private StorageOptions TtlOptions(
        long memTableBytes = 4 * 1024 * 1024,
        int compactionThreshold = 4) =>
        Options(memTableBytes, clock: _clock.Func, compactionThreshold: compactionThreshold);

    [Fact]
    public void PutWithTtl_VisibleBeforeDeadline_AbsentAfter_Lazily()
    {
        using var db = LsmDb.Open(TtlOptions());
        db.Put(B("k"), B("v"), TimeSpan.FromSeconds(30));
        Assert.Equal("v", S(db.Get(B("k"))!));

        _clock.Advance(TimeSpan.FromSeconds(29));
        Assert.Equal("v", S(db.Get(B("k"))!));

        _clock.Advance(TimeSpan.FromSeconds(2)); // past the deadline
        Assert.Null(db.Get(B("k")));
        Assert.Empty(db.Scan());
    }

    [Fact]
    public void ExpiredNewestValue_NeverResurrectsTheOlderTableValue()
    {
        using (var db = LsmDb.Open(TtlOptions()))
        {
            db.Put(B("k"), B("old"));
            db.Flush(); // "old" now lives in a table
            db.Put(B("k"), B("new"), TimeSpan.FromSeconds(10));

            _clock.Advance(TimeSpan.FromSeconds(11));
            // Lazy expiry in the memtable already shadows the table copy.
            Assert.Null(db.Get(B("k")));

            db.Flush(); // the worker converts the expired value to a tombstone
            Assert.Null(db.Get(B("k")));
        }
        using (var db = LsmDb.Open(TtlOptions()))
        {
            Assert.Null(db.Get(B("k")));
            db.Compact(); // bottom merge: tombstone + shadowed value both purged
            Assert.Null(db.Get(B("k")));
            Assert.Equal(0, db.TableCount); // nothing physical remains of either version
        }
        using (var db = LsmDb.Open(TtlOptions()))
        {
            Assert.Null(db.Get(B("k")));
        }
    }

    [Fact]
    public void PlainPut_ClearsExpiry()
    {
        using var db = LsmDb.Open(TtlOptions());
        db.Put(B("k"), B("v1"), TimeSpan.FromSeconds(10));
        db.Put(B("k"), B("v2"));
        _clock.Advance(TimeSpan.FromHours(1));
        Assert.Equal("v2", S(db.Get(B("k"))!));
    }

    [Fact]
    public void PutWithTtl_OverwriteWithLaterDeadline_TheLaterDeadlineWins()
    {
        using var db = LsmDb.Open(TtlOptions());
        db.Put(B("k"), B("v1"), TimeSpan.FromSeconds(10));
        _clock.Advance(TimeSpan.FromSeconds(5));
        db.Put(B("k"), B("v2"), TimeSpan.FromSeconds(100));
        _clock.Advance(TimeSpan.FromSeconds(50)); // past the first, before the second
        Assert.Equal("v2", S(db.Get(B("k"))!));
        _clock.Advance(TimeSpan.FromSeconds(60));
        Assert.Null(db.Get(B("k")));
    }

    [Fact]
    public void Expire_SetsADeadlineOnALiveValue_AndPersistsIt()
    {
        using (var db = LsmDb.Open(TtlOptions()))
        {
            db.Put(B("k"), B("v"));
            Assert.True(db.Expire(B("k"), TimeSpan.FromSeconds(20)));
            Assert.False(db.Expire(B("missing"), TimeSpan.FromSeconds(20)));
            _clock.Advance(TimeSpan.FromSeconds(19));
            Assert.Equal("v", S(db.Get(B("k"))!));
        }
        using (var db = LsmDb.Open(TtlOptions()))
        {
            // The deadline was persisted as an absolute timestamp.
            Assert.Equal("v", S(db.Get(B("k"))!));
            _clock.Advance(TimeSpan.FromSeconds(2));
            Assert.Null(db.Get(B("k")));
            // Expire on an expired value is a miss, not a resurrection.
            Assert.False(db.Expire(B("k"), TimeSpan.FromSeconds(20)));
            Assert.Null(db.Get(B("k")));
        }
    }

    [Fact]
    public void Ttl_SurvivesCrashWithTheOriginalDeadline()
    {
        var db = LsmDb.Open(TtlOptions());
        db.Put(B("a"), B("1"), TimeSpan.FromSeconds(60));
        db.Put(B("b"), B("2"));
        db.SimulateCrash();

        using (var db2 = LsmDb.Open(TtlOptions()))
        {
            Assert.Equal("1", S(db2.Get(B("a"))!));
            _clock.Advance(TimeSpan.FromSeconds(61));
            Assert.Null(db2.Get(B("a")));
            Assert.Equal("2", S(db2.Get(B("b"))!));
        }
    }

    [Fact]
    public void Ttl_InABatch_AppliesWithTheBatch()
    {
        var db = LsmDb.Open(TtlOptions());
        db.Put(B("gone"), B("x"));
        db.Write(new WriteBatch()
            .Put(B("timed"), B("t"), TimeSpan.FromSeconds(15))
            .Put(B("plain"), B("p"))
            .Delete(B("gone")));
        db.SimulateCrash();

        using (var db2 = LsmDb.Open(TtlOptions()))
        {
            Assert.Equal("t", S(db2.Get(B("timed"))!));
            Assert.Equal("p", S(db2.Get(B("plain"))!));
            Assert.Null(db2.Get(B("gone")));
            _clock.Advance(TimeSpan.FromSeconds(16));
            Assert.Null(db2.Get(B("timed")));
            Assert.Equal("p", S(db2.Get(B("plain"))!));
        }
    }

    [Fact]
    public void Delete_OnAnExpiredValue_WritesNothingAndReturnsFalse()
    {
        using var db = LsmDb.Open(TtlOptions());
        db.Put(B("k"), B("v"), TimeSpan.FromSeconds(5));
        _clock.Advance(TimeSpan.FromSeconds(6));
        Assert.False(db.Delete(B("k")));
        Assert.Null(db.Get(B("k")));
    }

    [Fact]
    public void NonPositiveTtl_Throws()
    {
        using var db = LsmDb.Open(TtlOptions());
        Assert.Throws<ArgumentOutOfRangeException>(() => db.Put(B("k"), B("v"), TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => db.Put(B("k"), B("v"), TimeSpan.FromSeconds(-1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => db.Expire(B("k"), TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new WriteBatch().Put(B("k"), B("v"), TimeSpan.Zero));
    }

    [Fact]
    public void Ttl_FlushedValue_ExpiresInsideTheTable()
    {
        using (var db = LsmDb.Open(TtlOptions()))
        {
            db.Put(B("k"), B("v"), TimeSpan.FromSeconds(30));
            db.Flush(); // the value lands in a table WITH its deadline
            _clock.Advance(TimeSpan.FromSeconds(29));
            Assert.Equal("v", S(db.Get(B("k"))!));
            _clock.Advance(TimeSpan.FromSeconds(2));
            Assert.Null(db.Get(B("k")));
        }
        using (var db = LsmDb.Open(TtlOptions()))
        {
            Assert.Null(db.Get(B("k")));
            db.Compact();
            Assert.Equal(0, db.TableCount); // physically dropped by the bottom merge
        }
    }

    [Fact]
    public void Model_RandomOperations_WithTtl_AdvancingClock_AndCrashes()
    {
        var rng = new Random(20261107);
        // Model: key -> (value, absolute deadline ms; 0 = none). Liveness is
        // judged against the same fake clock the database uses.
        var model = new Dictionary<string, (string Value, long DeadlineMs)>();
        var keys = Enumerable.Range(0, 40).Select(i => $"key-{i:D2}").ToArray();

        bool Live((string Value, long DeadlineMs) e) => e.DeadlineMs == 0 || e.DeadlineMs > _clock.NowMs;

        void Verify(LsmDb db)
        {
            foreach (var key in keys)
            {
                var expected = model.TryGetValue(key, out var e) && Live(e) ? e.Value : null;
                var actual = db.Get(B(key));
                Assert.Equal(expected, actual is null ? null : S(actual));
            }
            var expectedScan = model
                .Where(kv => Live(kv.Value))
                .OrderBy(kv => kv.Key, StringComparer.Ordinal)
                .Select(kv => (kv.Key, kv.Value.Value))
                .ToList();
            var actualScan = db.Scan().Select(kv => (S(kv.Key), S(kv.Value))).ToList();
            Assert.Equal(expectedScan, actualScan);
        }

        var db = LsmDb.Open(TtlOptions(memTableBytes: 1024, compactionThreshold: 3));
        for (var step = 0; step < 300; step++)
        {
            var key = keys[rng.Next(keys.Length)];
            switch (rng.Next(8))
            {
                case 0:
                case 1:
                case 2:
                {
                    var value = $"v{step}";
                    db.Put(B(key), B(value));
                    model[key] = (value, 0);
                    break;
                }
                case 3:
                {
                    var value = $"v{step}";
                    var ttl = TimeSpan.FromSeconds(rng.Next(1, 40));
                    db.Put(B(key), B(value), ttl);
                    model[key] = (value, _clock.NowMs + (long)ttl.TotalMilliseconds);
                    break;
                }
                case 4:
                {
                    var ttl = TimeSpan.FromSeconds(rng.Next(1, 40));
                    var existed = model.TryGetValue(key, out var e) && Live(e);
                    Assert.Equal(existed, db.Expire(B(key), ttl));
                    if (existed)
                        model[key] = (e.Value, _clock.NowMs + (long)ttl.TotalMilliseconds);
                    break;
                }
                case 5:
                {
                    var existed = model.TryGetValue(key, out var e) && Live(e);
                    Assert.Equal(existed, db.Delete(B(key)));
                    model.Remove(key);
                    break;
                }
                case 6:
                    _clock.Advance(TimeSpan.FromSeconds(rng.Next(1, 25)));
                    break;
                case 7:
                    db.Flush();
                    break;
            }
            if (step % 60 == 59)
            {
                db.Compact();
                Verify(db);
                db.SimulateCrash();
                db = LsmDb.Open(TtlOptions(memTableBytes: 1024, compactionThreshold: 3));
                Verify(db);
            }
        }
        Verify(db);
        db.Dispose();
    }
}
