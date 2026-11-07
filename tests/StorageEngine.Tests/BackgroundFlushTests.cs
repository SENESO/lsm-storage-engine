using static StorageEngine.Tests.TestSupport;

namespace StorageEngine.Tests;

/// <summary>
/// The background flush: rotation seals the memtable and its WAL
/// generation instantly, a worker thread writes the table, and the commit
/// lands it in the manifest. These tests pin the semantics DESIGN.md
/// states — reads and writes continue against the new generation while a
/// flush is in flight; a write is durable in exactly one place at all
/// times; Dispose rendezvouses with the worker; SimulateCrash abandons it
/// wherever it stands; and a failed flush loses nothing, suspends writes
/// loudly, and is retried by Flush().
/// </summary>
public class BackgroundFlushTests : DbTestBase
{
    private static void WaitUntil(Func<bool> condition, string because)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return;
            Thread.Sleep(10);
        }
        Assert.Fail($"Timed out waiting for: {because}");
    }

    /// <summary>A hook that parks the flush worker until the test releases it.</summary>
    private sealed class Gate
    {
        public ManualResetEventSlim Entered { get; } = new(false);
        public ManualResetEventSlim Release { get; } = new(false);

        public Action Hook => () =>
        {
            Entered.Set();
            Release.Wait(TimeSpan.FromSeconds(60));
        };
    }

    private static Dictionary<string, string> Snapshot(LsmDb db)
    {
        var result = new Dictionary<string, string>();
        foreach (var (key, value) in db.Scan())
            result[S(key)] = S(value);
        return result;
    }

    [Fact]
    public async Task Flush_RunsInBackground_ReadsAndWritesContinueAgainstTheNewGeneration()
    {
        using var db = LsmDb.Open(Options(compactionThreshold: 1000));
        db.Put(B("sealed-1"), B("old"));
        db.Put(B("sealed-2"), B("old"));
        db.Put(B("sealed-3"), B("old"));

        var gate = new Gate();
        db.FlushTableWrittenHook = gate.Hook;
        var flush = Task.Run(() => db.Flush());
        try
        {
            // The worker has written the table but not committed it: the
            // flush is genuinely in flight while we work.
            Assert.True(gate.Entered.Wait(TimeSpan.FromSeconds(15)), "the flush worker never reached the written point");

            // Reads see the frozen memtable (it is part of the read path).
            Assert.Equal("old", S(db.Get(B("sealed-1"))!));
            // Writes land in the new generation and read back.
            db.Put(B("live-1"), B("new"));
            Assert.Equal("new", S(db.Get(B("live-1"))!));
            // The new generation shadows the frozen one: overwrite and
            // delete of sealed keys take effect immediately.
            db.Put(B("sealed-2"), B("shadow"));
            db.Delete(B("sealed-3"));
            Assert.Equal("shadow", S(db.Get(B("sealed-2"))!));
            Assert.Null(db.Get(B("sealed-3")));
            Assert.Equal(
                new Dictionary<string, string>
                {
                    ["sealed-1"] = "old",
                    ["sealed-2"] = "shadow",
                    ["live-1"] = "new",
                },
                Snapshot(db));
        }
        finally
        {
            gate.Release.Set();
        }
        Assert.Same(flush, await Task.WhenAny(flush, Task.Delay(TimeSpan.FromSeconds(15))));
        db.FlushTableWrittenHook = null;

        Assert.Equal(1, db.TableCount);
        db.Dispose();
        using var reopened = LsmDb.Open(Options(compactionThreshold: 1000));
        Assert.Equal(
            new Dictionary<string, string>
            {
                ["sealed-1"] = "old",
                ["sealed-2"] = "shadow",
                ["live-1"] = "new",
            },
            Snapshot(reopened));
    }

    [Fact]
    public void Crash_AfterRotation_BeforeTableWrite_RecoversEverythingFromTheWals()
    {
        var db = LsmDb.Open(Options(compactionThreshold: 1000));
        db.Put(B("a"), B("1"));
        db.Put(B("b"), B("2"));

        var gate = new Gate();
        db.FlushJobStartingHook = gate.Hook;
        var flush = Task.Run(() => db.Flush());
        Assert.True(gate.Entered.Wait(TimeSpan.FromSeconds(15)), "the flush worker never started");
        // More acknowledged writes, into the new generation, pre-crash.
        db.Put(B("c"), B("3"));

        db.SimulateCrash();
        gate.Release.Set();
        // The abandoned worker finishes its step and deletes its temp file
        // without ever committing; wait for the directory to go quiet.
        WaitUntil(() => !Directory.GetFiles(Dir, "sst-*").Any(), "the abandoned worker to leave no table behind");
        _ = flush.Exception; // the parked Flush() call observes the dead instance; irrelevant here

        using var reopened = LsmDb.Open(Options(compactionThreshold: 1000));
        Assert.Equal(0, reopened.TableCount);
        Assert.Equal(
            new Dictionary<string, string> { ["a"] = "1", ["b"] = "2", ["c"] = "3" },
            Snapshot(reopened));
    }

    [Fact]
    public void Crash_AfterTableRename_BeforeCommit_RecoversFromWal_AndDiscardsTheTable()
    {
        var db = LsmDb.Open(Options(compactionThreshold: 1000));
        db.Put(B("a"), B("1"));
        db.Put(B("b"), B("2"));

        var gate = new Gate();
        db.FlushTableWrittenHook = gate.Hook;
        var flush = Task.Run(() => db.Flush());
        Assert.True(gate.Entered.Wait(TimeSpan.FromSeconds(15)), "the flush worker never reached the written point");

        db.SimulateCrash();
        gate.Release.Set();
        _ = flush.Exception;

        // The renamed-but-unadopted table is an orphan: open discards it
        // and the WAL generations replay to the exact pre-crash state.
        using var reopened = LsmDb.Open(Options(compactionThreshold: 1000));
        Assert.Equal(0, reopened.TableCount);
        Assert.Empty(Directory.GetFiles(Dir, "sst-*.lsm"));
        Assert.Equal(
            new Dictionary<string, string> { ["a"] = "1", ["b"] = "2" },
            Snapshot(reopened));
    }

    [Fact]
    public async Task Dispose_RendezvousesWithTheInFlightFlush()
    {
        var db = LsmDb.Open(Options(compactionThreshold: 1000));
        db.Put(B("a"), B("1"));

        var gate = new Gate();
        db.FlushTableWrittenHook = gate.Hook;
        var flush = Task.Run(() => db.Flush());
        Assert.True(gate.Entered.Wait(TimeSpan.FromSeconds(15)), "the flush worker never reached the written point");

        var dispose = Task.Run(() => db.Dispose());
        try
        {
            // The flush is parked before its commit; Dispose must not
            // return — and must not deadlock — while it is.
            Assert.NotSame(dispose, await Task.WhenAny(dispose, Task.Delay(TimeSpan.FromMilliseconds(300))));
        }
        finally
        {
            gate.Release.Set();
        }
        Assert.Same(dispose, await Task.WhenAny(dispose, Task.Delay(TimeSpan.FromSeconds(15))));
        Assert.Same(flush, await Task.WhenAny(flush, Task.Delay(TimeSpan.FromSeconds(15))));

        // The rendezvous left the committed state behind, not a rotation.
        using var reopened = LsmDb.Open(Options(compactionThreshold: 1000));
        Assert.Equal(1, reopened.TableCount);
        Assert.Equal("1", S(reopened.Get(B("a"))!));
    }

    [Fact]
    public async Task Flush_WaitsForAChainedRotation_RequestedWhileAFlushWasInFlight()
    {
        using var db = LsmDb.Open(Options(compactionThreshold: 1000));
        db.Put(B("first"), B("1"));

        var gate = new Gate();
        db.FlushTableWrittenHook = gate.Hook;
        var flush1 = Task.Run(() => db.Flush());
        Assert.True(gate.Entered.Wait(TimeSpan.FromSeconds(15)), "the flush worker never reached the written point");

        db.Put(B("second"), B("2"));
        var flush2 = Task.Run(() => db.Flush());
        try
        {
            var both = Task.WhenAll(flush1, flush2);
            Assert.NotSame(both, await Task.WhenAny(both, Task.Delay(TimeSpan.FromMilliseconds(300))));
        }
        finally
        {
            gate.Release.Set();
        }
        await Task.WhenAll(flush1, flush2);
        db.FlushTableWrittenHook = null;

        // Both generations landed as their own tables: the second Flush()
        // waited for the chained rotation, not just the first job.
        Assert.Equal(2, db.TableCount);
        db.Dispose();
        using var reopened = LsmDb.Open(Options(compactionThreshold: 1000));
        Assert.Equal("1", S(reopened.Get(B("first"))!));
        Assert.Equal("2", S(reopened.Get(B("second"))!));
    }

    [Fact]
    public void BackgroundFlushFailure_SuspendsWrites_KeepsReads_FlushRetriesAndHeals()
    {
        using var db = LsmDb.Open(Options(memTableBytes: 1024, compactionThreshold: 1000));
        db.Put(B("stable"), B("value"));

        var fail = true;
        db.FlushFailureHook = () =>
        {
            if (fail)
                throw new IOException("injected flush failure");
        };

        // Fill the memtable with distinct bulky keys until a rotation
        // fires; the worker then fails, and the failure latches: further
        // writes throw. Every fill that returned is acknowledged and must
        // survive to the end of the test.
        var fills = new List<string>();
        WaitUntil(() =>
        {
            try
            {
                var key = $"fill-{fills.Count:D3}";
                db.Put(B(key), new byte[256]);
                fills.Add(key);
                return false;
            }
            catch (InvalidOperationException)
            {
                return true;
            }
        }, "the injected flush failure to suspend writes");
        Assert.NotEmpty(fills);

        // Reads keep working throughout: the stable key and the frozen
        // generation behind the failed flush are both served.
        Assert.Equal("value", S(db.Get(B("stable"))!));
        Assert.NotNull(db.Get(B(fills[0])));

        // A retry while the fault persists surfaces the original failure.
        var thrown = Assert.Throws<InvalidOperationException>(() => db.Flush());
        Assert.IsType<IOException>(thrown.InnerException);

        // Fault cleared: Flush() retries the same job and the store heals.
        fail = false;
        db.Flush();
        db.Put(B("after"), B("healed"));
        db.FlushFailureHook = null;
        db.Dispose();

        using var reopened = LsmDb.Open(Options(memTableBytes: 1024, compactionThreshold: 1000));
        Assert.Equal("value", S(reopened.Get(B("stable"))!));
        Assert.Equal("healed", S(reopened.Get(B("after"))!));
        foreach (var key in fills)
            Assert.NotNull(reopened.Get(B(key)));
    }

    [Fact]
    public void SeededRandomCrashes_DuringBackgroundFlushes_RecoverTheAcknowledgedModel()
    {
        for (var seed = 0; seed < 12; seed++)
        {
            var dir = Path.Combine(Dir, $"seed-{seed}");
            Directory.CreateDirectory(dir);
            var options = TestSupport.Options(dir, memTableBytes: 1024, compactionThreshold: 1000);
            var rng = new Random(seed);
            var model = new Dictionary<string, string>();

            var db = LsmDb.Open(options);
            var opCount = rng.Next(60, 140);
            for (var i = 0; i < opCount; i++)
            {
                var roll = rng.Next(100);
                if (roll < 70)
                {
                    var key = $"k-{rng.Next(40):D2}";
                    var value = $"s{seed}-op{i}";
                    db.Put(B(key), B(value));
                    model[key] = value;
                }
                else if (roll < 85)
                {
                    var key = $"k-{rng.Next(40):D2}";
                    if (db.Delete(B(key)))
                        model.Remove(key);
                }
                else
                {
                    var batch = new WriteBatch();
                    var staged = new List<(string Key, string Value)>();
                    for (var j = 0; j < rng.Next(2, 4); j++)
                    {
                        var key = $"k-{rng.Next(40):D2}";
                        var value = $"s{seed}-op{i}-{j}";
                        batch.Put(B(key), B(value));
                        staged.Add((key, value));
                    }
                    db.Write(batch);
                    foreach (var (key, value) in staged)
                        model[key] = value;
                }
                if (rng.Next(100) < 10)
                    Thread.Sleep(1); // let workers land at random points
                if (rng.Next(100) < 5)
                    db.Flush();
            }
            // Death lands wherever the pipeline happens to be: rotating,
            // writing, committing, or idle. Every call that returned was
            // acknowledged, so the model is exact.
            db.SimulateCrash();

            using var reopened = LsmDb.Open(options);
            Assert.Equal(model, Snapshot(reopened));
        }
    }

    [Fact]
    public async Task ConcurrentWriters_DuringBackgroundFlushes_LandEveryWrite()
    {
        using var db = LsmDb.Open(Options(memTableBytes: 8 * 1024, compactionThreshold: 1000));
        const int writers = 4;
        const int writesPerWriter = 250;

        var tasks = Enumerable.Range(0, writers).Select(w => Task.Run(() =>
        {
            for (var i = 0; i < writesPerWriter; i++)
                db.Put(B($"w{w}-{i:D4}"), B($"value-{w}-{i}"));
        })).ToArray();
        // Settle the pipeline from the main thread while writers rotate it.
        var all = Task.WhenAll(tasks);
        while (!all.IsCompleted)
        {
            db.Flush();
            await Task.Delay(20);
        }
        await all;
        db.Flush();

        for (var w = 0; w < writers; w++)
            for (var i = 0; i < writesPerWriter; i++)
                Assert.Equal($"value-{w}-{i}", S(db.Get(B($"w{w}-{i:D4}"))!));
        db.Dispose();

        using var reopened = LsmDb.Open(Options(memTableBytes: 8 * 1024, compactionThreshold: 1000));
        Assert.Equal(writers * writesPerWriter, Snapshot(reopened).Count);
    }
}
