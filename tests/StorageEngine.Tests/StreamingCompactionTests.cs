using System.Diagnostics;
using static StorageEngine.Tests.TestSupport;

namespace StorageEngine.Tests;

/// <summary>
/// Tests in this collection never run in parallel with any other collection:
/// the compaction boundedness test measures the process working set, which a
/// neighbouring test's allocations would corrupt.
/// </summary>
[CollectionDefinition("Serial", DisableParallelization = true)]
public sealed class SerialCollectionDefinition
{
}

[Collection("Serial")]
public class StreamingCompactionTests : DbTestBase
{
    /// <summary>
    /// The merge must not hold the data it merges. This test builds ~76 MB of
    /// input tables across 80+ tables (~61 MB of it surviving into the merged
    /// table) and compacts them while a sampler thread watches the process
    /// working set.
    ///
    /// A merge that materializes its output first — the pre-hardening
    /// implementation collected every surviving entry into a list before
    /// writing — would grow the working set by roughly the surviving data
    /// size (~61 MB here, more with list slack). The streaming merge holds
    /// one entry per input table plus the writer's sampled index and bloom
    /// filter, so its growth is a few megabytes of stream buffers and
    /// transient entry garbage: 5–9 MB across the hardening run's measurement
    /// probes. The 32 MB limit sits several times above that observed growth
    /// and well under half of what a materializing merge would need — the
    /// margin is deliberate, so the test fails on the algorithm, never on
    /// machine noise.
    /// </summary>
    [Fact]
    public void Compaction_Streams_PeakMemoryStaysFarBelowTheDataSize()
    {
        const int keySpace = 120_000;
        const int valueBytes = 512;
        const long growthLimitBytes = 32L * 1024 * 1024;

        var options = Options(memTableBytes: 1024 * 1024, policy: FsyncPolicy.Interval, compactionThreshold: 100_000);
        var model = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);

        static string Key(int i) => $"k-{i:D6}";
        static byte[] Value(int i, int version)
        {
            var value = new byte[valueBytes];
            for (var j = 0; j < value.Length; j++)
                value[j] = (byte)(i * 31 + version * 7 + j);
            return value;
        }

        var db = LsmDb.Open(options);
        for (var i = 0; i < keySpace; i++)
        {
            var value = Value(i, 1);
            db.Put(B(Key(i)), value);
            model[Key(i)] = value;
        }
        for (var i = 0; i < 30_000; i++)
        {
            var value = Value(i, 2);
            db.Put(B(Key(i)), value);
            model[Key(i)] = value;
        }
        for (var i = 30_000; i < 42_000; i++)
        {
            Assert.True(db.Delete(B(Key(i))));
            model.Remove(Key(i));
        }
        Assert.True(db.TableCount >= 8, $"expected many input tables, got {db.TableCount}");

        // Baseline: a settled heap, sampled a few times so a lazy GC does not
        // masquerade as compaction growth.
        using var process = Process.GetCurrentProcess();
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var baseline = 0L;
        for (var i = 0; i < 3; i++)
        {
            Thread.Sleep(50);
            process.Refresh();
            baseline = Math.Max(baseline, process.WorkingSet64);
        }

        long peak = baseline;
        var stop = false;
        var sampler = new Thread(() =>
        {
            while (!Volatile.Read(ref stop))
            {
                process.Refresh();
                if (process.WorkingSet64 > peak)
                    peak = process.WorkingSet64;
                Thread.Sleep(2);
            }
        })
        { IsBackground = true };
        sampler.Start();
        db.Compact();
        Volatile.Write(ref stop, true);
        sampler.Join();
        process.Refresh();
        peak = Math.Max(peak, process.WorkingSet64);

        var growth = peak - baseline;
        Assert.True(growth < growthLimitBytes,
            $"peak working-set growth during compaction was {growth / (1024.0 * 1024):F1} MB; " +
            $"the limit is {growthLimitBytes / (1024 * 1024)} MB against ~57 MB of live data");

        Assert.Equal(1, db.TableCount);
        AssertExactState(db, model);
        db.Dispose();

        using var reopened = LsmDb.Open(options);
        Assert.Equal(1, reopened.TableCount);
        AssertExactState(reopened, model);
    }

    private static void AssertExactState(LsmDb db, SortedDictionary<string, byte[]> model)
    {
        // Span comparisons rather than Assert.Equal on byte arrays: xUnit's
        // collection-equality path costs ~0.6 ms per 512-byte value, which at
        // this entry count would take a minute per verification pass. The
        // check itself is identical — byte-for-byte, in order.
        var count = 0;
        using var expected = model.GetEnumerator();
        foreach (var pair in db.Scan())
        {
            Assert.True(expected.MoveNext(), "scan returned more entries than the model holds");
            Assert.True(string.Equals(expected.Current.Key, S(pair.Key), StringComparison.Ordinal),
                $"key mismatch at scanned entry {count}");
            Assert.True(expected.Current.Value.AsSpan().SequenceEqual(pair.Value),
                $"value mismatch for key {expected.Current.Key}");
            count++;
        }
        Assert.Equal(model.Count, count);
    }
}
