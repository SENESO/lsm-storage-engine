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
        int compactionThreshold = 4) =>
        new()
        {
            DirectoryPath = dir,
            MemTableSizeBytes = memTableBytes,
            FsyncPolicy = policy,
            CompactionTableThreshold = compactionThreshold,
        };
}

/// <summary>Base class: one fresh database directory per test.</summary>
public abstract class DbTestBase : IDisposable
{
    protected string Dir { get; } = TestSupport.NewDirectory();

    protected StorageOptions Options(
        long memTableBytes = 4 * 1024 * 1024,
        FsyncPolicy policy = FsyncPolicy.Always,
        int compactionThreshold = 4) =>
        TestSupport.Options(Dir, memTableBytes, policy, compactionThreshold);

    public void Dispose()
    {
        TestSupport.DeleteDirectory(Dir);
        GC.SuppressFinalize(this);
    }
}
