using System.Buffers.Binary;
using static StorageEngine.Tests.TestSupport;

namespace StorageEngine.Tests;

/// <summary>
/// The manifest format v2 states: several live WAL generations (a rotation
/// makes two generations live at once) and the pending-flush slot a
/// background flush leaves behind when it dies mid-flight. DESIGN.md states
/// the resolution rule — recovery replays every listed generation in order,
/// discards a pending flush (its table was never adopted; the WAL is the
/// record), and upgrades a v1 manifest in place. These tests hand-build the
/// exact on-disk states and open them.
/// </summary>
public class ManifestV2RecoveryTests : DbTestBase
{
    private string ManifestPath => Path.Combine(Dir, "MANIFEST");

    private static byte[] Frame(byte[] payload)
    {
        var frame = new byte[8 + payload.Length];
        BinaryPrimitives.WriteInt32LittleEndian(frame, payload.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(4), Crc32.Compute(payload));
        payload.CopyTo(frame.AsSpan(8));
        return frame;
    }

    private void WriteManifest(params string[] lines) =>
        File.WriteAllLines(ManifestPath,
            lines.Prepend("# LSM storage engine manifest. Replaced atomically; do not edit while a database is open."));

    private void WriteWal(long generation, params byte[][] payloads)
    {
        using var stream = new FileStream(Path.Combine(Dir, $"wal-{generation}.log"),
            FileMode.Create, FileAccess.Write);
        foreach (var payload in payloads)
            stream.Write(Frame(payload));
    }

    [Fact]
    public void Version1Manifest_OpensAndUpgradesInPlace()
    {
        using (var db = LsmDb.Open(Options(compactionThreshold: 1000)))
        {
            db.Put(B("a"), B("1"));
            db.Flush(); // table 1, WAL generation 2
            db.Put(B("b"), B("2"));
        }
        // Rewrite the manifest exactly as the v1 engine wrote it: no format
        // line, a single wal-gen line, no pending slot.
        WriteManifest("next-sst-id 2", "wal-gen 2", "sst 1");

        using (var db = LsmDb.Open(Options(compactionThreshold: 1000)))
        {
            Assert.Equal("1", S(db.Get(B("a"))!));
            Assert.Equal("2", S(db.Get(B("b"))!));
            Assert.Equal(1, db.TableCount);
        }

        // The open upgraded the file in place: same meaning, v2 layout.
        var upgraded = File.ReadAllText(ManifestPath);
        Assert.Contains("format 2", upgraded);
        Assert.Contains("wal-gen 2", upgraded);
        Assert.Contains("sst 1", upgraded);
        Assert.DoesNotContain("pending-flush", upgraded);
    }

    [Fact]
    public void MultipleGenerations_ReplayInOrder_AndCollapseAtTheNextFlush()
    {
        // Two live generations, neither in a table: generation 2 is newer,
        // so its overwrite and its delete win; a key only in generation 1
        // survives.
        WriteWal(1,
            RecordCodec.EncodePut(B("a"), B("old")),
            RecordCodec.EncodePut(B("b"), B("gone")),
            RecordCodec.EncodePut(B("c"), B("kept")));
        WriteWal(2,
            RecordCodec.EncodePut(B("a"), B("new")),
            RecordCodec.EncodeDelete(B("b")));
        WriteManifest("format 2", "next-sst-id 1", "wal-gen 1", "wal-gen 2");

        using (var db = LsmDb.Open(Options(compactionThreshold: 1000)))
        {
            Assert.Equal("new", S(db.Get(B("a"))!));
            Assert.Null(db.Get(B("b")));
            Assert.Equal("kept", S(db.Get(B("c"))!));

            // The flush commits the merged generations into one table and
            // the generation files leave with it.
            db.Flush();
            Assert.Equal(1, db.TableCount);
            Assert.False(File.Exists(Path.Combine(Dir, "wal-1.log")));
            Assert.False(File.Exists(Path.Combine(Dir, "wal-2.log")));
        }
        using (var db = LsmDb.Open(Options(compactionThreshold: 1000)))
        {
            Assert.Equal("new", S(db.Get(B("a"))!));
            Assert.Null(db.Get(B("b")));
            Assert.Equal("kept", S(db.Get(B("c"))!));
        }
    }

    /// <summary>
    /// Builds the state a rotation leaves the instant after it commits to
    /// the manifest: table 1 live, generation 2 sealed with one record in
    /// its WAL and named by the pending slot (reserved table 2), generation
    /// 3 the empty append target.
    /// </summary>
    private void BuildRotatedState()
    {
        using (var db = LsmDb.Open(Options(compactionThreshold: 1000)))
        {
            db.Put(B("base"), B("table"));
            db.Flush(); // table 1, WAL generation 2 live
            db.Put(B("pending-key"), B("from-wal"));
        }
        File.WriteAllBytes(Path.Combine(Dir, "wal-3.log"), Array.Empty<byte>());
        WriteManifest("format 2", "next-sst-id 3", "wal-gen 2", "wal-gen 3", "pending-flush 2 2", "sst 1");
    }

    [Fact]
    public void CrashAfterRotation_BeforeFlush_RecoversFromThePendingGenerationsWal()
    {
        // The rotation committed; the background flush never wrote a byte.
        BuildRotatedState();

        using (var db = LsmDb.Open(Options(compactionThreshold: 1000)))
        {
            Assert.Equal("table", S(db.Get(B("base"))!));
            Assert.Equal("from-wal", S(db.Get(B("pending-key"))!));
            Assert.Equal(1, db.TableCount);

            // The pending slot was resolved and the resolution saved:
            // no pending entry remains, both generations stay listed
            // (their records are durable only in those files).
            var resolved = File.ReadAllText(ManifestPath);
            Assert.DoesNotContain("pending-flush", resolved);
            Assert.Contains("wal-gen 2", resolved);
            Assert.Contains("wal-gen 3", resolved);

            // A flush now lands the recovered records for real.
            db.Flush();
            Assert.Equal(2, db.TableCount);
            Assert.False(File.Exists(Path.Combine(Dir, "wal-2.log")));
            Assert.False(File.Exists(Path.Combine(Dir, "wal-3.log")));
        }
        using (var db = LsmDb.Open(Options(compactionThreshold: 1000)))
        {
            Assert.Equal("from-wal", S(db.Get(B("pending-key"))!));
        }
    }

    [Fact]
    public void CrashMidFlush_TableRenamedButNotAdopted_PendingFlushIsDiscarded()
    {
        // The flush wrote and renamed its table, but the manifest swap that
        // adopts it never happened. The table is an orphan: recovery deletes
        // it unread and replays the generation's WAL instead — so a key the
        // table carries but the WAL does not ("ghost") must NOT appear.
        BuildRotatedState();
        var scratch = NewDirectory();
        try
        {
            using (var db = LsmDb.Open(TestSupport.Options(scratch, compactionThreshold: 1000)))
            {
                db.Put(B("pending-key"), B("from-wal"));
                db.Put(B("ghost"), B("never-acknowledged-in-this-db"));
                db.Flush();
            }
            var produced = Directory.GetFiles(scratch, "sst-*.lsm").Single();
            File.Copy(produced, Path.Combine(Dir, "sst-000002.lsm"));
        }
        finally
        {
            DeleteDirectory(scratch);
        }

        using (var db = LsmDb.Open(Options(compactionThreshold: 1000)))
        {
            Assert.Equal(1, db.TableCount);
            Assert.False(File.Exists(Path.Combine(Dir, "sst-000002.lsm")));
            Assert.Null(db.Get(B("ghost")));
            Assert.Equal("from-wal", S(db.Get(B("pending-key"))!));
        }
    }

    [Fact]
    public void CrashMidFlush_PartialTempTable_PendingFlushIsDiscarded()
    {
        // The flush died while its table was still a temp file.
        BuildRotatedState();
        File.WriteAllBytes(Path.Combine(Dir, "sst-000002.tmp"), B("half a table"));

        using (var db = LsmDb.Open(Options(compactionThreshold: 1000)))
        {
            Assert.False(File.Exists(Path.Combine(Dir, "sst-000002.tmp")));
            Assert.Equal(1, db.TableCount);
            Assert.Equal("from-wal", S(db.Get(B("pending-key"))!));
        }
    }

    [Fact]
    public void CrashAfterPendingCleared_BeforeWalDeletion_StaleGenerationFileIsIgnored()
    {
        // The flush committed (table adopted, slot cleared, generations
        // collapsed) but the sealed generation's file was never deleted.
        // It is unlisted, so it is removed unread: the phantom record in it
        // must not resurrect.
        using (var db = LsmDb.Open(Options(compactionThreshold: 1000)))
        {
            db.Put(B("k1"), B("v1"));
            db.Flush();
            db.Put(B("k2"), B("v2"));
            db.Flush(); // tables 1 and 2, generation 3 live
        }
        WriteWal(2,
            RecordCodec.EncodePut(B("k2"), B("v2")),
            RecordCodec.EncodePut(B("phantom"), B("boo")));

        using (var db = LsmDb.Open(Options(compactionThreshold: 1000)))
        {
            Assert.False(File.Exists(Path.Combine(Dir, "wal-2.log")));
            Assert.Null(db.Get(B("phantom")));
            Assert.Equal("v2", S(db.Get(B("k2"))!));
            Assert.Equal(2, db.TableCount);
        }
    }

    [Fact]
    public void PendingFlush_NamingASealedGenerationThatIsNotLive_FailsLoudly()
    {
        using (var db = LsmDb.Open(Options()))
        {
            db.Put(B("k"), B("v"));
        }
        // Generation 9 is not in the generation list: no engine state
        // produces this, so the manifest is damaged, not a crash state.
        WriteManifest("format 2", "next-sst-id 1", "wal-gen 1", "pending-flush 9 2");

        Assert.Throws<StorageCorruptionException>(() => LsmDb.Open(Options()));
    }

    [Fact]
    public void PendingFlush_NamingAnAlreadyAdoptedTable_FailsLoudly()
    {
        using (var db = LsmDb.Open(Options(compactionThreshold: 1000)))
        {
            db.Put(B("k"), B("v"));
            db.Flush(); // table 1 adopted, generation 2 live
        }
        File.WriteAllBytes(Path.Combine(Dir, "wal-1.log"), Array.Empty<byte>());
        WriteManifest("format 2", "next-sst-id 2", "wal-gen 1", "wal-gen 2", "pending-flush 1 1", "sst 1");

        Assert.Throws<StorageCorruptionException>(() => LsmDb.Open(Options()));
    }

    [Fact]
    public void MissingListedWalGeneration_FailsLoudly()
    {
        using (var db = LsmDb.Open(Options()))
        {
            db.Put(B("k"), B("v"));
        }
        // The manifest names generation 1; its file — the only durable copy
        // of the acknowledged write above — is gone. Opening must refuse
        // rather than replay nothing and call the loss a fresh database.
        File.Delete(Path.Combine(Dir, "wal-1.log"));

        Assert.Throws<StorageCorruptionException>(() => LsmDb.Open(Options()));
    }
}
