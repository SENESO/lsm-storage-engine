using static StorageEngine.Tests.TestSupport;

namespace StorageEngine.Tests;

public class BloomFilterTests
{
    [Fact]
    public void BloomFilter_NeverReportsAnInsertedKeyAbsent()
    {
        var filter = BloomFilter.Create(expectedEntries: 10_000, bitsPerKey: 10);
        var keys = new List<byte[]>();
        for (var i = 0; i < 10_000; i++)
        {
            var key = B($"bloom-key-{i}");
            keys.Add(key);
            filter.Add(key);
        }

        // The one hard rule of a bloom filter: no false negatives, ever.
        foreach (var key in keys)
            Assert.True(filter.MightContain(key), $"false negative for {S(key)}");
    }

    [Fact]
    public void BloomFilter_FalsePositiveRate_StaysLow()
    {
        var filter = BloomFilter.Create(expectedEntries: 10_000, bitsPerKey: 10);
        for (var i = 0; i < 10_000; i++)
            filter.Add(B($"present-{i}"));

        var falsePositives = 0;
        for (var i = 0; i < 10_000; i++)
        {
            if (filter.MightContain(B($"absent-{i}")))
                falsePositives++;
        }

        // 10 bits/key ≈ 1% expected. 5% is a generous ceiling that still
        // proves the filter is doing its job.
        Assert.True(falsePositives < 500, $"false positive rate too high: {falsePositives}/10000");
    }

    [Fact]
    public void BloomFilter_AtScale_NoFalseNegatives_AndFalsePositiveRateStaysNearTheory()
    {
        // The compaction path sizes a merged table's filter from an upper
        // bound; the flush path sizes it exactly. Either way the filter's
        // promises must hold at production-ish counts, not just at 10k:
        // zero false negatives, and a false-positive rate near the ~1%
        // that 10 bits/key predicts.
        const int entries = 100_000;
        var filter = BloomFilter.Create(expectedEntries: entries, bitsPerKey: 10);
        for (var i = 0; i < entries; i++)
            filter.Add(B($"scale-present-{i}"));

        for (var i = 0; i < entries; i++)
            Assert.True(filter.MightContain(B($"scale-present-{i}")), $"false negative at {i}");

        var falsePositives = 0;
        for (var i = 0; i < entries; i++)
        {
            if (filter.MightContain(B($"scale-absent-{i}")))
                falsePositives++;
        }
        Assert.True(falsePositives < entries / 50,
            $"false positive rate too high at scale: {falsePositives}/{entries} (2% ceiling, ~1% expected)");
    }

    [Fact]
    public void BloomFilter_SurvivesSerializationRoundTrip()
    {
        var filter = BloomFilter.Create(expectedEntries: 100, bitsPerKey: 10);
        filter.Add(B("alpha"));
        filter.Add(B("beta"));

        var restored = BloomFilter.FromBytes(filter.ToBytes());
        Assert.True(restored.MightContain(B("alpha")));
        Assert.True(restored.MightContain(B("beta")));
    }
}
