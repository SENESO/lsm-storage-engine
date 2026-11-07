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
