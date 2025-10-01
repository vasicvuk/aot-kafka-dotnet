namespace AotKafka.Native;

/// <summary>
/// Compression codec types
/// </summary>
internal enum CompressionCodec
{
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Zstd = 4
}