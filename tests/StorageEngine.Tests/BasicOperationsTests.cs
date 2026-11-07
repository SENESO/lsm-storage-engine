using static StorageEngine.Tests.TestSupport;

namespace StorageEngine.Tests;

public class BasicOperationsTests : DbTestBase
{
    [Fact]
    public void Put_ThenGet_ReturnsTheValue()
    {
        using var db = LsmDb.Open(Options());
        db.Put(B("greeting"), B("hello"));
        Assert.Equal("hello", S(db.Get(B("greeting"))!));
    }

    [Fact]
    public void Get_MissingKey_ReturnsNull()
    {
        using var db = LsmDb.Open(Options());
        Assert.Null(db.Get(B("nope")));
    }

    [Fact]
    public void Put_Overwrite_LastWriteWins()
    {
        using var db = LsmDb.Open(Options());
        db.Put(B("k"), B("first"));
        db.Put(B("k"), B("second"));
        Assert.Equal("second", S(db.Get(B("k"))!));
    }

    [Fact]
    public void Delete_ExistingKey_ReturnsTrue_AndRemovesIt()
    {
        using var db = LsmDb.Open(Options());
        db.Put(B("k"), B("v"));
        Assert.True(db.Delete(B("k")));
        Assert.Null(db.Get(B("k")));
    }

    [Fact]
    public void Delete_MissingKey_ReturnsFalse()
    {
        using var db = LsmDb.Open(Options());
        Assert.False(db.Delete(B("ghost")));
    }

    [Fact]
    public void Put_EmptyValue_IsStored_AndDistinctFromMissing()
    {
        using var db = LsmDb.Open(Options());
        db.Put(B("empty"), Array.Empty<byte>());
        var value = db.Get(B("empty"));
        Assert.NotNull(value);
        Assert.Empty(value);
        Assert.Null(db.Get(B("missing")));
    }

    [Fact]
    public void BinaryKeysAndValues_RoundTrip()
    {
        using var db = LsmDb.Open(Options());
        var key = new byte[] { 0x00, 0xFF, 0x10, 0x80, 0x00 };
        var value = new byte[] { 0xDE, 0xAD, 0x00, 0xBE, 0xEF };
        db.Put(key, value);
        Assert.Equal(value, db.Get(key));
    }

    [Fact]
    public void Write_Batch_AppliesEveryOperation()
    {
        using var db = LsmDb.Open(Options());
        db.Put(B("old"), B("x"));
        var batch = new WriteBatch()
            .Put(B("a"), B("1"))
            .Put(B("b"), B("2"))
            .Delete(B("old"));
        db.Write(batch);

        Assert.Equal("1", S(db.Get(B("a"))!));
        Assert.Equal("2", S(db.Get(B("b"))!));
        Assert.Null(db.Get(B("old")));
    }

    [Fact]
    public void Write_EmptyBatch_IsANoOp()
    {
        using var db = LsmDb.Open(Options());
        db.Write(new WriteBatch());
        Assert.Null(db.Get(B("anything")));
    }

    [Fact]
    public void Reopen_CleanDispose_PreservesWrites()
    {
        using (var db = LsmDb.Open(Options()))
        {
            db.Put(B("a"), B("1"));
            db.Put(B("b"), B("2"));
        }
        using (var db = LsmDb.Open(Options()))
        {
            Assert.Equal("1", S(db.Get(B("a"))!));
            Assert.Equal("2", S(db.Get(B("b"))!));
        }
    }
}
