using System.Buffers.Binary;
using static StorageEngine.Tests.TestSupport;

namespace StorageEngine.Tests;

/// <summary>
/// The states a crash leaves behind on disk: files written but never adopted
/// by the manifest, files the manifest adopted whose inputs were never
/// deleted, and outright damage to adopted files. DESIGN.md promises each of
/// these recovers in a specific way — orphans are deleted unread, adopted
/// data survives, damage to adopted data fails loudly. These tests build
/// those exact on-disk states and open them.
/// </summary>
public class RecoveryStateTests : DbTestBase
{
    private static byte[] Frame(byte[] payload)
    {
        var frame = new byte[8 + payload.Length];
        BinaryPrimitives.WriteInt32LittleEndian(frame, payload.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(4), Crc32.Compute(payload));
        payload.CopyTo(frame.AsSpan(8));
        return frame;
    }

    private static (string WalPath, int LastRecordStart) FindWalAndLastRecord(string dir)
    {
        var walPath = Directory.GetFiles(dir, "wal-*.log").Single();
        var bytes = File.ReadAllBytes(walPath);
        var pos = 0;
        var lastStart = 0;
        while (pos < bytes.Length)
        {
            lastStart = pos;
            pos += 8 + BitConverter.ToInt32(bytes, pos);
        }
        return (walPath, lastStart);
    }

    [Fact]
    public void CrashDuringFlush_TableRenamedButManifestNotUpdated_RecoversFromWal_AndDeletesOrphan()
    {
        // Flush protocol step 1's crash window: the new table file is fully
        // written and renamed into place, but the manifest never learned
        // about it — the WAL still holds every record it contains.
        using (var db = LsmDb.Open(Options(compactionThreshold: 1000)))
        {
            db.Put(B("alpha"), B("1"));
            db.Put(B("beta"), B("2"));
        }

        // A table holding exactly those entries, produced by a second
        // database, transplanted in as the never-adopted flush output.
        var scratch = NewDirectory();
        try
        {
            using (var db = LsmDb.Open(TestSupport.Options(scratch, compactionThreshold: 1000)))
            {
                db.Put(B("alpha"), B("1"));
                db.Put(B("beta"), B("2"));
                db.Flush();
            }
            var produced = Directory.GetFiles(scratch, "sst-*.lsm").Single();
            File.Copy(produced, Path.Combine(Dir, Path.GetFileName(produced)));
        }
        finally
        {
            DeleteDirectory(scratch);
        }

        using (var db = LsmDb.Open(Options(compactionThreshold: 1000)))
        {
            // The orphan was deleted unread; the data came back from the WAL.
            Assert.Equal(0, db.TableCount);
            Assert.Empty(Directory.GetFiles(Dir, "sst-*.lsm"));
            Assert.Equal("1", S(db.Get(B("alpha"))!));
            Assert.Equal("2", S(db.Get(B("beta"))!));

            // The directory is fully healthy: a flush now adopts id 1 for real.
            db.Flush();
            Assert.Equal(1, db.TableCount);
            Assert.Equal("1", S(db.Get(B("alpha"))!));
        }
    }

    [Fact]
    public void StaleWalGenerationFile_IsIgnoredAndRemovedOnOpen()
    {
        // Flush protocol step 3's crash window: the manifest already points
        // at the new WAL generation, but the old generation's file was never
        // deleted. Its records must not be replayed — they are already in
        // the table, and anything else in it is not live data.
        string stalePath;
        using (var db = LsmDb.Open(Options(compactionThreshold: 1000)))
        {
            db.Put(B("real"), B("1"));
            db.Flush(); // manifest now on WAL generation 2
        }
        stalePath = Path.Combine(Dir, "wal-1.log");
        File.WriteAllBytes(stalePath, Frame(RecordCodec.EncodePut(B("phantom"), B("boo"))));

        using (var db = LsmDb.Open(Options(compactionThreshold: 1000)))
        {
            Assert.False(File.Exists(stalePath));
            Assert.Null(db.Get(B("phantom")));
            Assert.Equal("1", S(db.Get(B("real"))!));
        }
    }

    [Fact]
    public void LeftoverTempFiles_AreRemovedOnOpen()
    {
        using (var db = LsmDb.Open(Options()))
        {
            db.Put(B("k"), B("v"));
        }
        // Debris of an interrupted manifest save and an interrupted flush.
        File.WriteAllBytes(Path.Combine(Dir, "MANIFEST.tmp"), B("next-sst-id bogus"));
        File.WriteAllBytes(Path.Combine(Dir, "sst-000042.tmp"), B("half a table"));

        using (var db = LsmDb.Open(Options()))
        {
            Assert.False(File.Exists(Path.Combine(Dir, "MANIFEST.tmp")));
            Assert.False(File.Exists(Path.Combine(Dir, "sst-000042.tmp")));
            Assert.Equal("v", S(db.Get(B("k"))!));
        }
    }

    [Fact]
    public void CrashAfterCompactionSwap_ReopenCleansRetiredInputs_AndKeepsMergedData()
    {
        // Compaction swaps the manifest to the merged table and deletes its
        // inputs lazily. A crash in between leaves the inputs on disk as
        // orphans of the new manifest.
        var db = LsmDb.Open(Options(compactionThreshold: 1000));
        db.Put(B("a"), B("1"));
        db.Flush();
        db.Put(B("b"), B("2"));
        db.Flush();
        db.Compact();
        Assert.Equal(3, Directory.GetFiles(Dir, "sst-*.lsm").Length);
        db.SimulateCrash();

        using var reopened = LsmDb.Open(Options(compactionThreshold: 1000));
        Assert.Equal(1, reopened.TableCount);
        Assert.Single(Directory.GetFiles(Dir, "sst-*.lsm"));
        Assert.Equal("1", S(reopened.Get(B("a"))!));
        Assert.Equal("2", S(reopened.Get(B("b"))!));
    }

    [Fact]
    public void MissingTableFile_ListedInManifest_FailsLoudly()
    {
        using (var db = LsmDb.Open(Options()))
        {
            db.Put(B("k"), B("v"));
            db.Flush();
        }
        File.Delete(Directory.GetFiles(Dir, "sst-*.lsm").Single());

        // The manifest names data that is not there: that is damage outside
        // the engine, and opening must refuse rather than lose it silently.
        Assert.Throws<StorageCorruptionException>(() => LsmDb.Open(Options()));
    }

    [Fact]
    public void CorruptedTableFooter_FailsLoudlyOnOpen()
    {
        string tablePath;
        using (var db = LsmDb.Open(Options()))
        {
            db.Put(B("k"), B("v"));
            db.Flush();
        }
        tablePath = Directory.GetFiles(Dir, "sst-*.lsm").Single();
        var bytes = File.ReadAllBytes(tablePath);
        bytes[^1] ^= 0xFF; // inside the footer's magic
        File.WriteAllBytes(tablePath, bytes);

        Assert.Throws<StorageCorruptionException>(() => LsmDb.Open(Options()));
    }

    [Fact]
    public void WalRecord_WithValidCrcButUndecodablePayload_FailsLoudly()
    {
        string walPath;
        using (var db = LsmDb.Open(Options()))
        {
            db.Put(B("a"), B("1"));
        }
        walPath = Directory.GetFiles(Dir, "wal-*.log").Single();

        // A frame the CRC vouches for, carrying a payload type the engine
        // never writes: the frame is intact, the content is not data.
        using (var stream = new FileStream(walPath, FileMode.Append, FileAccess.Write))
            stream.Write(Frame(new byte[] { 0x7F, 0xAA }));

        Assert.Throws<StorageCorruptionException>(() => LsmDb.Open(Options()));
    }

    [Fact]
    public void TornTail_LastRecordFailsItsCrc_RecoversEarlierRecords_AndTruncates()
    {
        using (var db = LsmDb.Open(Options()))
        {
            for (var i = 0; i < 5; i++)
                db.Put(B($"t-{i}"), B($"payload-{i}"));
        }
        var (walPath, lastStart) = FindWalAndLastRecord(Dir);
        var bytes = File.ReadAllBytes(walPath);
        bytes[lastStart + 8 + 2] ^= 0xFF; // inside the last record's payload
        File.WriteAllBytes(walPath, bytes);

        using (var db = LsmDb.Open(Options()))
        {
            for (var i = 0; i < 4; i++)
                Assert.Equal($"payload-{i}", S(db.Get(B($"t-{i}"))!));
            // The damaged record was never validly acknowledged: it is cut,
            // not served, and the log ends exactly at the last good record.
            Assert.Null(db.Get(B("t-4")));
            Assert.Equal(lastStart, new FileInfo(walPath).Length);
            db.Put(B("after"), B("repair"));
        }
        using (var db = LsmDb.Open(Options()))
        {
            Assert.Equal("repair", S(db.Get(B("after"))!));
        }
    }

    [Fact]
    public void TornTail_LastRecordTruncatedMidPayload_RecoversEarlierRecords()
    {
        using (var db = LsmDb.Open(Options()))
        {
            for (var i = 0; i < 5; i++)
                db.Put(B($"t-{i}"), B($"payload-{i}"));
        }
        var (walPath, lastStart) = FindWalAndLastRecord(Dir);
        // The classic torn write: header complete, payload cut short.
        using (var stream = new FileStream(walPath, FileMode.Open, FileAccess.Write))
            stream.SetLength(lastStart + 8 + 2);

        using (var db = LsmDb.Open(Options()))
        {
            for (var i = 0; i < 4; i++)
                Assert.Equal($"payload-{i}", S(db.Get(B($"t-{i}"))!));
            Assert.Null(db.Get(B("t-4")));
            Assert.Equal(lastStart, new FileInfo(walPath).Length);
        }
    }
}
