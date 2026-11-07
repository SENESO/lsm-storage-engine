using System.Text;

namespace StorageEngine.Tests;

internal static class TestSupport
{
    public static string NewDirectory()
    {
        var dir = Path.Combine(Path.GetTempPath(), "lsm-db-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    public static void DeleteDirectory(string dir)
    {
        try
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
        }
        catch (IOException)
        {
            // Best effort: temp directories are transient anyway.
        }
    }

    public static byte[] B(string text) => Encoding.UTF8.GetBytes(text);

    public static string S(byte[] bytes) => Encoding.UTF8.GetString(bytes);

    public static StorageOptions Options(
        string dir,
        long memTableBytes = 4 * 1024 * 1024,
        FsyncPolicy policy = FsyncPolicy.Always,
        int compactionThreshold = 4,
        IComparer<byte[]>? comparer = null,
        string? comparerName = null,
        Func<DateTimeOffset>? clock = null,
        CompactionStrategy strategy = CompactionStrategy.Tiered,
        long leveledBaseLevelSizeBytes = 10 * 1024 * 1024,
        int leveledSizeRatio = 10,
        long leveledTargetTableSizeBytes = 2 * 1024 * 1024) =>
        new()
        {
            DirectoryPath = dir,
            MemTableSizeBytes = memTableBytes,
            FsyncPolicy = policy,
            CompactionTableThreshold = compactionThreshold,
            KeyComparer = comparer,
            KeyComparerName = comparerName,
            Clock = clock,
            CompactionStrategy = strategy,
            LeveledBaseLevelSizeBytes = leveledBaseLevelSizeBytes,
            LeveledSizeRatio = leveledSizeRatio,
            LeveledTargetTableSizeBytes = leveledTargetTableSizeBytes,
        };
}

/// <summary>A manually advanced clock: TTL tests move time explicitly instead of sleeping.</summary>
internal sealed class FakeClock
{
    private DateTimeOffset _now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    public DateTimeOffset Now() => _now;

    public Func<DateTimeOffset> Func => Now;

    public void Advance(TimeSpan by) => _now += by;

    public long NowMs => _now.ToUnixTimeMilliseconds();
}

/// <summary>Base class: one fresh database directory per test.</summary>
public abstract class DbTestBase : IDisposable
{
    protected string Dir { get; } = TestSupport.NewDirectory();

    protected StorageOptions Options(
        long memTableBytes = 4 * 1024 * 1024,
        FsyncPolicy policy = FsyncPolicy.Always,
        int compactionThreshold = 4,
        IComparer<byte[]>? comparer = null,
        string? comparerName = null,
        Func<DateTimeOffset>? clock = null,
        CompactionStrategy strategy = CompactionStrategy.Tiered,
        long leveledBaseLevelSizeBytes = 10 * 1024 * 1024,
        int leveledSizeRatio = 10,
        long leveledTargetTableSizeBytes = 2 * 1024 * 1024) =>
        TestSupport.Options(Dir, memTableBytes, policy, compactionThreshold, comparer, comparerName,
            clock, strategy, leveledBaseLevelSizeBytes, leveledSizeRatio, leveledTargetTableSizeBytes);

    public void Dispose()
    {
        TestSupport.DeleteDirectory(Dir);
        GC.SuppressFinalize(this);
    }
}
