using static StorageEngine.Tests.TestSupport;

namespace StorageEngine.Tests;

/// <summary>
/// Leveled compaction as the alternative strategy: L0 drains into L1, each
/// level above 0 is key-range partitioned (non-overlapping tables) under a
/// size-ratio budget, and outputs split at a target table size. Tombstones
/// are dropped only when the merge's inputs provably contain every older
/// copy they could shadow. Crash windows around the manifest swap behave
/// exactly like the tiered strategy's: unlisted files are orphans and are
/// deleted unread.
/// </summary>
public class LeveledCompactionTests : DbTestBase
{
    private StorageOptions Leveled(
        long memTableBytes = 64 * 1024,
        int threshold = 2,
        long baseBytes = 1024 * 1024,
        int ratio = 10,
        long targetBytes = 1024 * 1024) =>
        Options(memTableBytes, compactionThreshold: threshold,
            strategy: CompactionStrategy.Leveled,
            leveledBaseLevelSizeBytes: baseBytes,
            leveledSizeRatio: ratio,
            leveledTargetTableSizeBytes: targetBytes);

    private List<(long Id, int Level)> ManifestTables()
    {
        var result = new List<(long, int)>();
        foreach (var line in File.ReadAllLines(Path.Combine(Dir, "MANIFEST")))
        {
            var parts = line.Split(' ');
            if (parts.Length >= 2 && parts[0] == "sst")
                result.Add((long.Parse(parts[1]), parts.Length >= 3 ? int.Parse(parts[2]) : 0));
        }
        return result;
    }

    private void EditManifestLevel(long id, int newLevel)
    {
        var path = Path.Combine(Dir, "MANIFEST");
        var lines = File.ReadAllLines(path);
        for (var i = 0; i < lines.Length; i++)
        {
            var parts = lines[i].Split(' ');
            if (parts.Length >= 2 && parts[0] == "sst" && long.Parse(parts[1]) == id)
                lines[i] = $"sst {id} {newLevel}";
        }
        File.WriteAllLines(path, lines);
    }

    private int TotalEntryCount()
    {
        var total = 0;
        foreach (var (id, _) in ManifestTables())
        {
            using var reader = SSTable.Reader.Open(id, Path.Combine(Dir, SSTable.FileName(id)));
            total += reader.EntryCount;
        }
        return total;
    }

    [Fact]
    public void LevelsForm_DataDrainsFromL0IntoL1_AndReadsBack()
    {
        using (var db = LsmDb.Open(Leveled()))
        {
            for (var chunk = 0; chunk < 3; chunk++)
            {
                for (var i = 0; i < 10; i++)
                    db.Put(B($"k{chunk * 10 + i:D2}"), B($"v{chunk}-{i}"));
                db.Flush();
            }
            db.Compact();
            var tables = ManifestTables();
            Assert.NotEmpty(tables);
            Assert.Contains(tables, t => t.Level >= 1);
            Assert.Equal("v1-3", S(db.Get(B("k13"))!));
            Assert.Equal(30, db.Scan().Count());
        }
        using (var db = LsmDb.Open(Leveled()))
        {
            Assert.Equal("v2-9", S(db.Get(B("k29"))!));
            Assert.Equal(30, db.Scan().Count());
        }
    }

    [Fact]
    public void DeepLevelsForm_AndEachLevelAboveZeroIsNonOverlapping()
    {
        var options = Leveled(memTableBytes: 4096, threshold: 2,
            baseBytes: 4096, ratio: 3, targetBytes: 4096);
        using (var db = LsmDb.Open(options))
        {
            var value = new string('x', 200);
            for (var chunk = 0; chunk < 16; chunk++)
            {
                for (var i = 0; i < 20; i++)
                    db.Put(B($"key-{chunk * 20 + i:D3}"), B($"{value}{chunk * 20 + i:D3}"));
                db.Flush();
            }
            db.Compact();
            Assert.Equal(320, db.Scan().Count());
        }

        var tables = ManifestTables();
        Assert.Contains(tables, t => t.Level >= 2); // ~70 KB cannot fit L1 (4 KB) or L2 (12 KB) budgets
        foreach (var levelGroup in tables.Where(t => t.Level >= 1).GroupBy(t => t.Level))
        {
            var spans = new List<(byte[] Min, byte[] Max)>();
            foreach (var (id, _) in levelGroup)
            {
                using var reader = SSTable.Reader.Open(id, Path.Combine(Dir, SSTable.FileName(id)));
                spans.Add((reader.MinKey, reader.MaxKey));
            }
            spans.Sort((a, b) => ByteArrayComparer.Instance.Compare(a.Min, b.Min));
            for (var i = 1; i < spans.Count; i++)
            {
                Assert.True(
                    ByteArrayComparer.Instance.Compare(spans[i - 1].Max, spans[i].Min) < 0,
                    $"level {levelGroup.Key} tables overlap: " +
                    $"[{S(spans[i - 1].Min)}..{S(spans[i - 1].Max)}] vs [{S(spans[i].Min)}..{S(spans[i].Max)}]");
            }
        }
    }

    [Fact]
    public void Model_RandomWorkload_UnderLeveledCompaction()
    {
        var rng = new Random(777);
        var model = new SortedDictionary<string, string>(StringComparer.Ordinal);
        var keys = Enumerable.Range(0, 40).Select(i => $"k{i:D2}").ToArray();
        var options = Leveled(memTableBytes: 700, threshold: 2,
            baseBytes: 2048, ratio: 3, targetBytes: 2048);

        void Verify(LsmDb db)
        {
            var expected = model.Select(kv => (kv.Key, kv.Value)).ToList();
            var actual = db.Scan().Select(kv => (S(kv.Key), S(kv.Value))).ToList();
            Assert.Equal(expected, actual);
        }

        var db = LsmDb.Open(options);
        for (var step = 0; step < 220; step++)
        {
            var key = keys[rng.Next(keys.Length)];
            switch (rng.Next(5))
            {
                case 0:
                case 1:
                case 2:
                {
                    var value = $"v{step}-{new string('y', rng.Next(0, 60))}";
                    db.Put(B(key), B(value));
                    model[key] = value;
                    break;
                }
                case 3:
                    Assert.Equal(model.Remove(key), db.Delete(B(key)));
                    break;
                case 4:
                    db.Flush();
                    break;
            }
            if (step % 55 == 54)
            {
                db.Compact();
                Verify(db);
                db.SimulateCrash();
                db = LsmDb.Open(options);
                Verify(db);
            }
        }
        Verify(db);
        db.Dispose();
    }

    [Fact]
    public void Tombstone_RetainedWhileAnOlderCopySitsOutsideTheMerge_DroppedOnceItJoins()
    {
        // Phase 1: value of "k" settles into L1 with an unrelated key.
        long valueTableId;
        using (var db = LsmDb.Open(Leveled()))
        {
            db.Put(B("k"), B("v1"));
            db.Flush();
            db.Put(B("p"), B("vp"));
            db.Flush();
            db.Compact(); // L0 (2 tables) drains into L1 as one table
            var tables = ManifestTables();
            Assert.Single(tables);
            Assert.Equal(1, tables[0].Level);
            valueTableId = tables[0].Id;
        }
        // Phase 2 (hand-built state): pin the value table at L2, so the next
        // merge at L1 cannot include it — the tombstone must be retained.
        EditManifestLevel(valueTableId, 2);
        using (var db = LsmDb.Open(Leveled()))
        {
            db.Delete(B("k"));
            db.Flush();
            db.Put(B("q"), B("vq"));
            db.Flush();
            db.Compact(); // drains L0 into L1; L2's copy of k is NOT an input
            Assert.Null(db.Get(B("k")));
            Assert.Equal("v1", S(ReadTableValue(valueTableId, B("k"))!)); // the old copy still exists below
            // k's tombstone (L1) + q (L1) + k,p (L2) = 4 physical entries.
            Assert.Equal(4, TotalEntryCount());
        }
        // Phase 3: shrink the L1 budget so the next settle merges the L1
        // table WITH the overlapping L2 table — now the drop is provable.
        using (var db = LsmDb.Open(Leveled(baseBytes: 10)))
        {
            db.Compact();
            Assert.Null(db.Get(B("k")));
            Assert.Equal("vp", S(db.Get(B("p"))!));
            Assert.Equal("vq", S(db.Get(B("q"))!));
            // Only the two live values remain, physically.
            Assert.Equal(2, TotalEntryCount());
        }
        using (var db = LsmDb.Open(Leveled()))
        {
            Assert.Null(db.Get(B("k")));
            Assert.Equal(2, db.Scan().Count());
        }
    }

    private byte[]? ReadTableValue(long id, byte[] key)
    {
        using var reader = SSTable.Reader.Open(id, Path.Combine(Dir, SSTable.FileName(id)));
        return reader.TryGet(key, out var entry) && entry.Kind == EntryKind.Value
            ? entry.Value
            : null;
    }

    [Fact]
    public void CrashWindow_UnlistedTableFile_IsAnOrphan_DeletedUnread()
    {
        long liveId;
        using (var db = LsmDb.Open(Leveled()))
        {
            db.Put(B("k"), B("v"));
            db.Flush();
            db.Compact();
            liveId = ManifestTables().Single().Id;
        }
        // A compaction output (or a retired input) that the manifest does
        // not list — e.g. crash between file write and manifest swap.
        File.Copy(
            Path.Combine(Dir, SSTable.FileName(liveId)),
            Path.Combine(Dir, SSTable.FileName(99)));
        Assert.True(File.Exists(Path.Combine(Dir, SSTable.FileName(99))));

        using (var db = LsmDb.Open(Leveled()))
        {
            Assert.False(File.Exists(Path.Combine(Dir, SSTable.FileName(99))));
            Assert.Equal("v", S(db.Get(B("k"))!));
        }
    }

    [Fact]
    public void StrategySwitch_TieredDirectoryOpensUnderLeveled_AndBack()
    {
        using (var db = LsmDb.Open(Options()))
        {
            for (var i = 0; i < 20; i++)
                db.Put(B($"k{i:D2}"), B($"v{i}"));
            db.Flush();
        }
        using (var db = LsmDb.Open(Leveled()))
        {
            Assert.Equal(20, db.Scan().Count());
            db.Put(B("k05"), B("updated"));
            db.Flush();
            db.Compact();
            Assert.Equal("updated", S(db.Get(B("k05"))!));
            Assert.Equal(20, db.Scan().Count());
        }
        // Back under tiered options: levels in the manifest are inert
        // metadata (all tables read newest-first), merges flatten as usual.
        using (var db = LsmDb.Open(Options()))
        {
            Assert.Equal("updated", S(db.Get(B("k05"))!));
            db.Compact();
            Assert.Equal(20, db.Scan().Count());
        }
    }

    [Fact]
    public void WriteAmplification_LeveledVersusTiered_OnTheSameOverwriteWorkload()
    {
        // The workload: a 400-key live set (~85 KB), then 12 rounds that
        // each rewrite a tenth of the keys, flushing and compacting after
        // every round. Tiered compaction merges EVERY table on every round,
        // so it rewrites the whole live set each time; leveled compaction
        // only moves the small update batches down the levels. The assertion
        // compares the engine's own count of SSTable bytes written.
        long Run(CompactionStrategy strategy)
        {
            var dir = Path.Combine(Path.GetTempPath(), $"lsm-amp-{Guid.NewGuid():N}");
            Directory.CreateDirectory(dir);
            try
            {
                long written;
                using (var db = LsmDb.Open(new StorageOptions
                {
                    DirectoryPath = dir,
                    MemTableSizeBytes = 256 * 1024,
                    CompactionTableThreshold = strategy == CompactionStrategy.Leveled ? 2 : 1000,
                    CompactionStrategy = strategy,
                    LeveledBaseLevelSizeBytes = 16 * 1024,
                    LeveledSizeRatio = 4,
                    LeveledTargetTableSizeBytes = 16 * 1024,
                }))
                {
                    var keys = Enumerable.Range(0, 400).Select(i => $"key-{i:D3}").ToArray();
                    var filler = new string('x', 200);
                    foreach (var key in keys)
                        db.Put(B(key), B(filler + key));
                    db.Flush();
                    db.Compact();
                    for (var round = 0; round < 12; round++)
                    {
                        var value = new string((char)('a' + round), 200);
                        for (var i = round * 40; i < round * 40 + 40; i++)
                            db.Put(B(keys[i % keys.Length]), B(value + keys[i % keys.Length]));
                        db.Flush();
                        db.Compact();
                    }
                    Assert.Equal(400, db.Scan().Count());
                    written = db.TotalTableBytesWritten;
                }
                return written;
            }
            finally
            {
                Directory.Delete(dir, recursive: true);
            }
        }

        var tiered = Run(CompactionStrategy.Tiered);
        var leveled = Run(CompactionStrategy.Leveled);
        Assert.True(leveled < tiered,
            $"expected leveled write amplification below tiered: leveled={leveled}, tiered={tiered}");
    }
}
