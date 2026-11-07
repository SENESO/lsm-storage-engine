using static StorageEngine.Tests.TestSupport;

namespace StorageEngine.Tests;

/// <summary>
/// The public surface's argument contract: invalid options are rejected
/// before a directory is touched, null arguments are refused by name, and a
/// disposed or crash-killed instance says so instead of corrupting state.
/// </summary>
public class ValidationTests : DbTestBase
{
    [Fact]
    public void InvalidOptions_AreRejectedBeforeOpening()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => LsmDb.Open(new StorageOptions
        {
            DirectoryPath = Dir, MemTableSizeBytes = 0,
        }));
        Assert.Throws<ArgumentOutOfRangeException>(() => LsmDb.Open(new StorageOptions
        {
            DirectoryPath = Dir, FsyncIntervalMilliseconds = 0,
        }));
        Assert.Throws<ArgumentOutOfRangeException>(() => LsmDb.Open(new StorageOptions
        {
            DirectoryPath = Dir, CompactionTableThreshold = 1,
        }));
        Assert.Throws<ArgumentOutOfRangeException>(() => LsmDb.Open(new StorageOptions
        {
            DirectoryPath = Dir, BloomBitsPerKey = 0,
        }));
        Assert.Throws<ArgumentOutOfRangeException>(() => LsmDb.Open(new StorageOptions
        {
            DirectoryPath = Dir, IndexSampleInterval = 0,
        }));
        Assert.Throws<ArgumentException>(() => LsmDb.Open(new StorageOptions
        {
            DirectoryPath = "",
        }));

        // None of the rejections took the directory lock or left state:
        // a valid open of the same directory succeeds immediately.
        using var db = LsmDb.Open(Options());
        db.Put(B("k"), B("v"));
        Assert.Equal("v", S(db.Get(B("k"))!));
    }

    [Fact]
    public void NullArguments_AreRejected()
    {
        Assert.Throws<ArgumentNullException>(() => LsmDb.Open(null!));

        using var db = LsmDb.Open(Options());
        Assert.Throws<ArgumentNullException>(() => db.Get(null!));
        Assert.Throws<ArgumentNullException>(() => db.Put(null!, B("v")));
        Assert.Throws<ArgumentNullException>(() => db.Put(B("k"), null!));
        Assert.Throws<ArgumentNullException>(() => db.Delete(null!));
        Assert.Throws<ArgumentNullException>(() => db.Write(null!));
        Assert.Throws<ArgumentNullException>(() => new WriteBatch().Put(null!, B("v")));
        Assert.Throws<ArgumentNullException>(() => new WriteBatch().Put(B("k"), null!));
        Assert.Throws<ArgumentNullException>(() => new WriteBatch().Delete(null!));
    }

    [Fact]
    public void UseAfterDispose_ThrowsObjectDisposed()
    {
        var db = LsmDb.Open(Options());
        db.Put(B("k"), B("v"));
        db.Dispose();

        Assert.Throws<ObjectDisposedException>(() => db.Get(B("k")));
        Assert.Throws<ObjectDisposedException>(() => db.Put(B("k"), B("v")));
        Assert.Throws<ObjectDisposedException>(() => db.Flush());
        Assert.Throws<ObjectDisposedException>(() => db.Scan());

        db.Dispose(); // disposing twice is explicitly safe
    }

    [Fact]
    public void UseAfterSimulatedCrash_ThrowsInvalidOperation()
    {
        var db = LsmDb.Open(Options());
        db.Put(B("k"), B("v"));
        db.SimulateCrash();

        Assert.Throws<InvalidOperationException>(() => db.Get(B("k")));
        Assert.Throws<InvalidOperationException>(() => db.Put(B("k"), B("v")));
        db.Dispose(); // a crashed instance disposes silently: its handles are gone
    }
}
