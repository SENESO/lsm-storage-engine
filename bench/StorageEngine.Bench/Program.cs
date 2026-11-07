using System.Diagnostics;
using System.Text;
using StorageEngine;

// A small, honest benchmark: single process, single thread, one sandbox VM.
// Every scenario states its durability policy, because fsync dominates write
// numbers and hiding it would make the numbers meaningless. See README.md.

var root = Path.Combine(Path.GetTempPath(), "lsm-bench-" + Guid.NewGuid().ToString("N"));
Directory.CreateDirectory(root);

static byte[] Key(int i) => Encoding.UTF8.GetBytes($"key-{i:D8}");
var value = new byte[100];
new Random(42).NextBytes(value);

double Measure(string label, Action work)
{
    var sw = Stopwatch.StartNew();
    work();
    sw.Stop();
    Console.WriteLine($"{label}: {sw.Elapsed.TotalSeconds:F2}s");
    return sw.Elapsed.TotalSeconds;
}

void Report(string label, int ops, double seconds) =>
    Console.WriteLine($"  -> {label}: {ops / seconds:N0} ops/sec ({ops:N0} ops)");

string Scenario(string name) => Directory.CreateDirectory(Path.Combine(root, name)).FullName;

try
{
    // --- Writes, fully durable: fsync per record ---
    {
        var dir = Scenario("write-always");
        using var db = LsmDb.Open(new StorageOptions { DirectoryPath = dir, FsyncPolicy = FsyncPolicy.Always });
        const int ops = 20_000;
        var seconds = Measure($"write {ops:N0} keys, 100B values, fsync=always", () =>
        {
            for (var i = 0; i < ops; i++)
                db.Put(Key(i), value);
        });
        Report("writes/sec (always)", ops, seconds);
    }

    // --- Writes, interval fsync (100 ms) ---
    {
        var dir = Scenario("write-interval");
        using var db = LsmDb.Open(new StorageOptions
        {
            DirectoryPath = dir,
            FsyncPolicy = FsyncPolicy.Interval,
            FsyncIntervalMilliseconds = 100,
        });
        const int ops = 100_000;
        var seconds = Measure($"write {ops:N0} keys, 100B values, fsync=interval(100ms)", () =>
        {
            for (var i = 0; i < ops; i++)
                db.Put(Key(i), value);
        });
        Report("writes/sec (interval)", ops, seconds);
    }

    // --- Point reads at two population sizes (data flushed + compacted to SSTables) ---
    foreach (var population in new[] { 10_000, 100_000 })
    {
        var dir = Scenario($"reads-{population}");
        using (var db = LsmDb.Open(new StorageOptions { DirectoryPath = dir, FsyncPolicy = FsyncPolicy.Interval }))
        {
            for (var i = 0; i < population; i++)
                db.Put(Key(i), value);
            db.Flush();
            db.Compact();
        }
        using var reopened = LsmDb.Open(new StorageOptions { DirectoryPath = dir });
        var random = new Random(1234);
        const int reads = 50_000;
        var hits = 0;
        var seconds = Measure($"read {reads:N0} random keys at {population:N0} population (SSTable path)", () =>
        {
            for (var i = 0; i < reads; i++)
            {
                if (reopened.Get(Key(random.Next(population))) is not null)
                    hits++;
            }
        });
        Report($"point reads/sec @{population:N0}", reads, seconds);
        Console.WriteLine($"     (hits: {hits:N0}/{reads:N0})");
    }

    // --- Full scan over the 100k population ---
    {
        var dir = Path.Combine(root, "reads-100000");
        using var db = LsmDb.Open(new StorageOptions { DirectoryPath = dir });
        long scanned = 0;
        var seconds = Measure("scan all entries at 100,000 population", () =>
        {
            foreach (var _ in db.Scan())
                scanned++;
        });
        Report("scan entries/sec", (int)scanned, seconds);
    }
}
finally
{
    try
    {
        Directory.Delete(root, recursive: true);
    }
    catch (IOException)
    {
        // Temp directory; the OS will reclaim it.
    }
}
