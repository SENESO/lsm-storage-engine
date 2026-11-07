using static StorageEngine.Tests.TestSupport;

namespace StorageEngine.Tests;

[Collection("Serial")]
public class ScaleTests : DbTestBase
{
    /// <summary>
    /// Correctness at a size where the machinery, not the data volume per
    /// operation, is what gets exercised: 300,000 keys are filled, then put
    /// through a mixed pass of deletes (every 7th key) and rewrites (every
    /// 3rd of the rest), then half the deleted keys are resurrected. The
    /// workload stacks ~18 tables, a full compaction merges them into one,
    /// and the result is verified byte-for-byte against an independent
    /// in-test model — full scan equality in order, a stride sample of point
    /// reads, deleted keys staying deleted — and then verified again after a
    /// close/reopen cycle, so the merged table on disk, not a memory image,
    /// is what passes.
    ///
    /// FsyncPolicy.Interval is used deliberately: this test proves logical
    /// correctness at scale, and the durability policies have their own
    /// crash-simulation tests. The interval policy keeps ~450,000 writes
    /// inside a sane runtime on a shared sandbox machine.
    /// </summary>
    [Fact]
    public void Scale_MixedWorkload_ByteExactAfterCompactionAndReopen()
    {
        const int keyCount = 300_000;

        var options = Options(memTableBytes: 4 * 1024 * 1024, policy: FsyncPolicy.Interval, compactionThreshold: 64);
        var model = new SortedDictionary<string, string>(StringComparer.Ordinal);

        static string Value(int i, char round) => $"{round}-{i:D6}-" + new string((char)('a' + i % 26), 88);

        var db = LsmDb.Open(options);

        for (var i = 0; i < keyCount; i++)
        {
            var value = Value(i, 'a');
            db.Put(B(Key(i)), B(value));
            model[Key(i)] = value;
        }

        for (var i = 0; i < keyCount; i++)
        {
            if (i % 7 == 0)
            {
                db.Delete(B(Key(i)));
                model.Remove(Key(i));
            }
            else if (i % 3 == 0)
            {
                var value = Value(i, 'b');
                db.Put(B(Key(i)), B(value));
                model[Key(i)] = value;
            }
        }

        // Resurrect every deleted key whose index is a multiple of 14 (the
        // other half of the deleted set, i % 14 == 7, stays deleted).
        for (var i = 0; i < keyCount; i += 14)
        {
            var value = Value(i, 'c');
            db.Put(B(Key(i)), B(value));
            model[Key(i)] = value;
        }

        Assert.True(db.TableCount >= 8, $"expected a stack of tables to merge, got {db.TableCount}");

        db.Compact();
        Assert.Equal(1, db.TableCount);
        VerifyExact(db, model, keyCount);
        db.Dispose();

        using var reopened = LsmDb.Open(options);
        Assert.Equal(1, reopened.TableCount);
        VerifyExact(reopened, model, keyCount);
    }

    private static string Key(int i) => $"user-{i:D6}";

    private static void VerifyExact(LsmDb db, SortedDictionary<string, string> model, int keyCount)
    {
        // Full scan: byte-exact against the model, in ascending order.
        var count = 0;
        using var expected = model.GetEnumerator();
        foreach (var pair in db.Scan())
        {
            Assert.True(expected.MoveNext(), "scan returned more entries than the model holds");
            Assert.Equal(expected.Current.Key, S(pair.Key));
            Assert.Equal(expected.Current.Value, S(pair.Value));
            count++;
        }
        Assert.Equal(model.Count, count);

        // Point reads on a stride sample: live keys return their value,
        // absent keys return null.
        for (var i = 0; i < keyCount; i += 499)
        {
            var key = Key(i);
            if (model.TryGetValue(key, out var value))
                Assert.Equal(value, S(db.Get(B(key))!));
            else
                Assert.Null(db.Get(B(key)));
        }

        // A sample of keys that were deleted and never resurrected stays
        // deleted — their tombstones were purged by the full merge, and the
        // values beneath them went with them.
        var checkedDeleted = 0;
        for (var i = 7; i < keyCount && checkedDeleted < 500; i += 14)
        {
            Assert.Null(db.Get(B(Key(i))));
            checkedDeleted++;
        }
        Assert.Equal(500, checkedDeleted);
    }
}
