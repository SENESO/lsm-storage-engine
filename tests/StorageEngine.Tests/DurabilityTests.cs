using static StorageEngine.Tests.TestSupport;

namespace StorageEngine.Tests;

/// <summary>
/// Durability is the point of this engine, so these are the load-bearing tests:
/// a process that dies mid-work must reopen to exactly the state its
/// acknowledged writes promised — every one of them, and never a partial one.
/// </summary>
public class DurabilityTests : DbTestBase
{
    [Fact]
    public void Crash_PreservesEveryAcknowledgedWrite()
    {
        var db = LsmDb.Open(Options());
        for (var i = 0; i < 200; i++)
            db.Put(B($"key-{i:D4}"), B($"value-{i}"));
        db.SimulateCrash();

        using var reopened = LsmDb.Open(Options());
        for (var i = 0; i < 200; i++)
            Assert.Equal($"value-{i}", S(reopened.Get(B($"key-{i:D4}"))!));
    }

    [Fact]
    public void Crash_BatchIsVisibleWholeOrNotAtAll()
    {
        var db = LsmDb.Open(Options());
        db.Put(B("victim"), B("before"));
        var batch = new WriteBatch();
        for (var i = 0; i < 50; i++)
            batch.Put(B($"batch-{i:D3}"), B($"b{i}"));
        batch.Delete(B("victim"));
        db.Write(batch);
        db.SimulateCrash();

        using var reopened = LsmDb.Open(Options());
        for (var i = 0; i < 50; i++)
            Assert.Equal($"b{i}", S(reopened.Get(B($"batch-{i:D3}"))!));
        Assert.Null(reopened.Get(B("victim")));
    }

    [Fact]
    public void Crash_AfterFlushesAndMoreWrites_PreservesEverything()
    {
        // 1 KB memtable with ~110-byte entries: several automatic flushes,
        // then more writes that live only in the WAL + memtable at crash time.
        var db = LsmDb.Open(Options(memTableBytes: 1024));
        for (var i = 0; i < 300; i++)
            db.Put(B($"k-{i:D4}"), B(new string('v', 100)));
        Assert.True(db.TableCount >= 1, "expected at least one automatic flush before the crash");
        db.SimulateCrash();

        using var reopened = LsmDb.Open(Options(memTableBytes: 1024));
        for (var i = 0; i < 300; i++)
            Assert.Equal(new string('v', 100), S(reopened.Get(B($"k-{i:D4}"))!));
    }

    [Fact]
    public void Crash_DeleteSurvives_ReplayedFromWal()
    {
        var db = LsmDb.Open(Options());
        db.Put(B("stay"), B("1"));
        db.Put(B("go"), B("2"));
        Assert.True(db.Delete(B("go")));
        db.SimulateCrash();

        using var reopened = LsmDb.Open(Options());
        Assert.Equal("1", S(reopened.Get(B("stay"))!));
        Assert.Null(reopened.Get(B("go")));
    }

    [Fact]
    public void TornTail_GarbageAppendedToWal_RecoversCleanly_AndTruncates()
    {
        string walPath;
        long cleanLength;
        using (var db = LsmDb.Open(Options()))
        {
            for (var i = 0; i < 20; i++)
                db.Put(B($"t-{i:D2}"), B($"v{i}"));
        }
        walPath = Directory.GetFiles(Dir, "wal-*.log").Single();
        cleanLength = new FileInfo(walPath).Length;

        // What a crash mid-append leaves: garbage bytes and a partial record.
        using (var stream = new FileStream(walPath, FileMode.Append, FileAccess.Write))
        {
            stream.Write(B("GARBAGEGARBAGE"));
            stream.Write(new byte[] { 0x40, 0x00, 0x00 }); // half of a record header
        }
        Assert.True(new FileInfo(walPath).Length > cleanLength);

        using (var db = LsmDb.Open(Options()))
        {
            for (var i = 0; i < 20; i++)
                Assert.Equal($"v{i}", S(db.Get(B($"t-{i:D2}"))!));
            // The torn bytes were cut: the log is back to its valid length...
            Assert.Equal(cleanLength, new FileInfo(walPath).Length);
            // ...and new appends land exactly where the valid records end.
            db.Put(B("after"), B("repair"));
        }
        using (var db = LsmDb.Open(Options()))
        {
            Assert.Equal("repair", S(db.Get(B("after"))!));
            Assert.Equal("v7", S(db.Get(B("t-07"))!));
        }
    }

    [Fact]
    public void MidLogCorruption_FlippedByteInMiddleRecord_FailsLoudly()
    {
        using (var db = LsmDb.Open(Options()))
        {
            for (var i = 0; i < 10; i++)
                db.Put(B($"c-{i}"), B($"payload-{i}"));
        }

        var walPath = Directory.GetFiles(Dir, "wal-*.log").Single();
        var bytes = File.ReadAllBytes(walPath);
        // Walk to the 5th record and flip one byte inside its payload.
        var pos = 0;
        for (var record = 0; record < 10; record++)
        {
            var length = BitConverter.ToInt32(bytes, pos);
            if (record == 4)
                bytes[pos + 8 + 3] ^= 0xFF;
            pos += 8 + length;
        }
        File.WriteAllBytes(walPath, bytes);

        // Records 6..10 are intact after the damaged one, so this cannot be a
        // torn tail: opening must refuse, loudly, and lose nothing silently.
        Assert.Throws<StorageCorruptionException>(() => LsmDb.Open(Options()));
    }

    [Fact]
    public void SecondOpen_WhileFirstIsAlive_IsRejected()
    {
        using var db = LsmDb.Open(Options());
        Assert.Throws<InvalidOperationException>(() => LsmDb.Open(Options()));
    }

    [Fact]
    public void Crash_ThenReopen_ReleasesTheDirectoryLock()
    {
        var db = LsmDb.Open(Options());
        db.Put(B("x"), B("y"));
        db.SimulateCrash();

        // A dead process holds no lock: a fresh instance opens immediately.
        using var reopened = LsmDb.Open(Options());
        Assert.Equal("y", S(reopened.Get(B("x"))!));
    }

    [Fact]
    public void IntervalPolicy_Crash_PreservesWritesToo()
    {
        // Process death does not lose the OS page cache, so even interval-fsynced
        // writes survive a *process* crash. (Power loss is the case the policy
        // honestly gives up — see DESIGN.md.)
        var db = LsmDb.Open(Options(policy: FsyncPolicy.Interval));
        for (var i = 0; i < 50; i++)
            db.Put(B($"i-{i}"), B($"v{i}"));
        db.SimulateCrash();

        using var reopened = LsmDb.Open(Options(policy: FsyncPolicy.Interval));
        for (var i = 0; i < 50; i++)
            Assert.Equal($"v{i}", S(reopened.Get(B($"i-{i}"))!));
    }
}
