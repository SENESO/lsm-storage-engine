using static StorageEngine.Tests.TestSupport;

namespace StorageEngine.Tests;

public class ConcurrencyTests : DbTestBase
{
    [Fact]
    public async Task ConcurrentReaders_NeverSeeTornOrWrongValues()
    {
        using var db = LsmDb.Open(Options(memTableBytes: 64 * 1024));
        const int keyCount = 2000;
        static string Key(int i) => $"key-{i:D5}";
        static string Value(int i) => $"val-{i:D5}";

        using var stop = new CancellationTokenSource();
        var readers = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
        {
            var random = new Random();
            while (!stop.IsCancellationRequested)
            {
                var i = random.Next(keyCount);
                var value = db.Get(B(Key(i)));
                // A key is either not written yet (null) or carries exactly its
                // own value — never another key's value, never a torn write.
                if (value is not null)
                    Assert.Equal(Value(i), S(value));
            }
        })).ToArray();

        var writer = Task.Run(() =>
        {
            for (var i = 0; i < keyCount; i++)
                db.Put(B(Key(i)), B(Value(i)));
        });

        await writer;
        stop.Cancel();
        await Task.WhenAll(readers);

        for (var i = 0; i < keyCount; i++)
            Assert.Equal(Value(i), S(db.Get(B(Key(i)))!));
    }
}
