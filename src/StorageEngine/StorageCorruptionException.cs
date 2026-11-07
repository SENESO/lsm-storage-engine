namespace StorageEngine;

/// <summary>
/// Thrown when on-disk data fails an integrity check (a WAL record with valid
/// records after it fails its CRC, an SSTable footer does not parse, the manifest
/// references a missing table). Corruption is always loud: the engine never
/// silently skips or repairs damaged data beyond truncating a torn WAL tail.
/// </summary>
public sealed class StorageCorruptionException : Exception
{
    public StorageCorruptionException(string message) : base(message)
    {
    }

    public StorageCorruptionException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
