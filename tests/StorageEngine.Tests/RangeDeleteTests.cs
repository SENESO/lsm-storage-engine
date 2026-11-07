using static StorageEngine.Tests.TestSupport;

namespace StorageEngine.Tests;

/// <summary>
/// Range deletes: one range tombstone per call, with the same durability and
/// shadowing guarantees as a point tombstone — it flows WAL → memtable →
/// flush → compaction, shadows every older layer for its whole span, and
/// never touches keys written after it.
/// </summary>
public class RangeDeleteTests : DbTestBase
{
    private void PutRange(LsmDb db, string from, string to, string valuePrefix = "v")
    {
        for (var c = from[0]; c <= to[0]; c++)
            db.Put(B($"{(char)c}"), B($"{valuePrefix}-{(char)c}"));
    }

    [Fact]
    public void RangeDelete_InMemtable_RemovesTheSpan_KeepsTheBoundaries()
    {
        using var db = LsmDb.Open(Options());
        PutRange(db, "a", "f");
        db.DeleteRange(B("b"), B("e")); // [b, e): b, c, d die; e is the exclusive end
        Assert.Equal("v-a", S(db.Get(B("a"))!));
        Assert.Null(db.Get(B("b")));
        Assert.Null(db.Get(B("c")));
        Assert.Null(db.Get(B("d")));
        Assert.Equal("v-e", S(db.Get(B("e"))!));
        Assert.Equal("v-f", S(db.Get(B("f"))!));
        Assert.Equal(new[] { "a", "e", "f" }, db.Scan().Select(kv => S(kv.Key)).ToArray());
    }

    [Fact]
    public void RangeDelete_ShadowsFlushedTables_AndSurvivesReopen()
    {
        using (var db = LsmDb.Open(Options()))
        {
            PutRange(db, "a", "f");
            db.Flush(); // the values live in a table now
            db.DeleteRange(B("b"), B("e"));
            Assert.Null(db.Get(B("c")));
            db.Flush(); // the tombstone lands in its own, newer table
            Assert.Null(db.Get(B("c")));
            Assert.Equal("v-a", S(db.Get(B("a"))!));
        }
        using (var db = LsmDb.Open(Options()))
        {
            Assert.Null(db.Get(B("b")));
            Assert.Null(db.Get(B("d")));
            Assert.Equal("v-e", S(db.Get(B("e"))!));
            Assert.Equal(new[] { "a", "e", "f" }, db.Scan().Select(kv => S(kv.Key)).ToArray());
        }
    }

    [Fact]
    public void RangeDelete_ThenPutInsideTheRange_ThePutSurvives()
    {
        using (var db = LsmDb.Open(Options()))
        {
            PutRange(db, "a", "f");
            db.Flush();
            db.DeleteRange(B("b"), B("e"));
            db.Put(B("c"), B("resurrected"));
            Assert.Equal("resurrected", S(db.Get(B("c"))!));
            db.Flush();
            Assert.Equal("resurrected", S(db.Get(B("c"))!));
            Assert.Null(db.Get(B("b")));
        }
        using (var db = LsmDb.Open(Options()))
        {
            Assert.Equal("resurrected", S(db.Get(B("c"))!));
            Assert.Null(db.Get(B("d")));
        }
    }

    [Fact]
    public void OverlappingRanges_DeleteTheUnion()
    {
        using var db = LsmDb.Open(Options());
        PutRange(db, "a", "h");
        db.DeleteRange(B("b"), B("d"));
        db.DeleteRange(B("c"), B("g")); // overlaps the first; union is [b, g)
        Assert.Equal("v-a", S(db.Get(B("a"))!));
        foreach (var key in new[] { "b", "c", "d", "e", "f" })
            Assert.Null(db.Get(B(key)));
        Assert.Equal("v-g", S(db.Get(B("g"))!));
        Assert.Equal("v-h", S(db.Get(B("h"))!));
        db.Flush();
        Assert.Equal(new[] { "a", "g", "h" }, db.Scan().Select(kv => S(kv.Key)).ToArray());
    }

    [Fact]
    public void RangeDelete_CrashBetweenWalAppendAndFlush_RecoversFromTheWal()
    {
        var db = LsmDb.Open(Options());
        PutRange(db, "a", "f");
        db.Flush();
        db.DeleteRange(B("b"), B("e")); // durable in the WAL, never flushed
        db.SimulateCrash();

        using (var db2 = LsmDb.Open(Options()))
        {
            Assert.Null(db2.Get(B("c")));
            Assert.Equal("v-a", S(db2.Get(B("a"))!));
            db2.Flush(); // now the tombstone is in a table as well
            Assert.Null(db2.Get(B("d")));
        }
        using (var db3 = LsmDb.Open(Options()))
        {
            Assert.Null(db3.Get(B("c")));
            Assert.Equal(new[] { "a", "e", "f" }, db3.Scan().Select(kv => S(kv.Key)).ToArray());
        }
    }

    [Fact]
    public void RangeDelete_FullCompaction_PurgesTheTombstone_WithoutResurrection()
    {
        using (var db = LsmDb.Open(Options(compactionThreshold: 1000)))
        {
            PutRange(db, "a", "f");
            db.Flush();
            db.DeleteRange(B("b"), B("e"));
            db.Flush();
            db.Compact(); // tiered bottom merge: values and tombstone all purge
            Assert.Null(db.Get(B("c")));
            Assert.Equal("v-a", S(db.Get(B("a"))!));
        }
        using (var db = LsmDb.Open(Options(compactionThreshold: 1000)))
        {
            Assert.Null(db.Get(B("b")));
            Assert.Null(db.Get(B("d")));
            Assert.Equal(new[] { "a", "e", "f" }, db.Scan().Select(kv => S(kv.Key)).ToArray());
        }
    }

    [Fact]
    public void RangeDelete_InABatch_AppliesInOrder_Atomically()
    {
        var db = LsmDb.Open(Options());
        PutRange(db, "a", "f");
        db.Flush();
        db.Write(new WriteBatch()
            .DeleteRange(B("b"), B("e"))
            .Put(B("c"), B("after-range"))); // put AFTER the range in the same batch: survives
        Assert.Equal("after-range", S(db.Get(B("c"))!));
        Assert.Null(db.Get(B("d")));

        db.Write(new WriteBatch()
            .Put(B("d"), B("before-range"))
            .DeleteRange(B("b"), B("e"))); // range AFTER the put in the same batch: kills it —
        Assert.Null(db.Get(B("d")));       // and re-covers "c" from the earlier batch, too
        Assert.Null(db.Get(B("c")));
        db.Put(B("c"), B("final")); // a put after every range delete: survives
        db.SimulateCrash();

        using (var db2 = LsmDb.Open(Options()))
        {
            Assert.Equal("final", S(db2.Get(B("c"))!));
            Assert.Null(db2.Get(B("d")));
            Assert.Equal("v-a", S(db2.Get(B("a"))!));
        }
    }

    [Fact]
    public void RangeDelete_EmptyRangeIsANoOp_ReversedRangeThrows()
    {
        using var db = LsmDb.Open(Options());
        PutRange(db, "a", "c");
        var sizeBefore = db.MemTableSizeBytes;
        db.DeleteRange(B("b"), B("b")); // empty: writes nothing
        Assert.Equal(sizeBefore, db.MemTableSizeBytes);
        Assert.Equal("v-b", S(db.Get(B("b"))!));
        Assert.Throws<ArgumentException>(() => db.DeleteRange(B("c"), B("a")));
        Assert.Throws<ArgumentException>(() =>
            db.Write(new WriteBatch().DeleteRange(B("c"), B("a"))));
    }

    [Fact]
    public void RangeDelete_CoveringEverything_EmptiesTheDatabase()
    {
        using var db = LsmDb.Open(Options());
        PutRange(db, "a", "f");
        db.Flush();
        PutRange(db, "g", "j", "w");
        db.DeleteRange(B("a"), B("z"));
        Assert.Empty(db.Scan());
        db.Flush();
        Assert.Empty(db.Scan());
        db.Compact();
        Assert.Equal(0, db.TableCount);
    }

    [Fact]
    public void Model_RandomOperations_WithRangeDeletes_AndCrashes()
    {
        var rng = new Random(424242);
        // Single-version model: operations apply in order; a range delete
        // removes the span's keys at its moment in the order, exactly the
        // final-state semantics layering must reproduce.
        var model = new SortedDictionary<string, string>(StringComparer.Ordinal);
        var keys = Enumerable.Range(0, 30).Select(i => $"k{i:D2}").ToArray();

        void Verify(LsmDb db)
        {
            var expected = model.Select(kv => (kv.Key, kv.Value)).ToList();
            var actual = db.Scan().Select(kv => (S(kv.Key), S(kv.Value))).ToList();
            Assert.Equal(expected, actual);
            foreach (var key in keys)
            {
                var expectedValue = model.TryGetValue(key, out var v) ? v : null;
                var actualValue = db.Get(B(key));
                Assert.Equal(expectedValue, actualValue is null ? null : S(actualValue));
            }
        }

        var db = LsmDb.Open(Options(memTableBytes: 900, compactionThreshold: 3));
        for (var step = 0; step < 250; step++)
        {
            switch (rng.Next(6))
            {
                case 0:
                case 1:
                case 2:
                {
                    var key = keys[rng.Next(keys.Length)];
                    var value = $"v{step}";
                    db.Put(B(key), B(value));
                    model[key] = value;
                    break;
                }
                case 3:
                {
                    var key = keys[rng.Next(keys.Length)];
                    Assert.Equal(model.Remove(key), db.Delete(B(key)));
                    break;
                }
                case 4:
                {
                    var i = rng.Next(keys.Length);
                    var j = rng.Next(keys.Length);
                    if (i > j) (i, j) = (j, i);
                    var start = keys[i];
                    var end = j + 1 < keys.Length ? keys[j + 1] : "k99";
                    db.DeleteRange(B(start), B(end));
                    foreach (var key in model.Keys
                                 .Where(k => string.CompareOrdinal(k, start) >= 0 &&
                                             string.CompareOrdinal(k, end) < 0)
                                 .ToList())
                        model.Remove(key);
                    break;
                }
                case 5:
                    db.Flush();
                    break;
            }
            if (step % 50 == 49)
            {
                db.Compact();
                Verify(db);
                db.SimulateCrash();
                db = LsmDb.Open(Options(memTableBytes: 900, compactionThreshold: 3));
                Verify(db);
            }
        }
        Verify(db);
        db.Dispose();
    }
}
