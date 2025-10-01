namespace AotKafka;

/// <summary>
/// Compression type for message payloads
/// </summary>
public enum CompressionType
{
    /// <summary>
    /// No compression
    /// </summary>
    None = 0,

    /// <summary>
    /// Gzip compression
    /// </summary>
    Gzip = 1,

    /// <summary>
    /// Snappy compression
    /// </summary>
    Snappy = 2,

    /// <summary>
    /// LZ4 compression
    /// </summary>
    Lz4 = 3,

    /// <summary>
    /// Zstandard compression
    /// </summary>
    Zstd = 4
}