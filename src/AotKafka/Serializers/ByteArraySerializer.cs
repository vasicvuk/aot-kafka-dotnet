namespace AotKafka.Serializers;

/// <summary>
/// Byte array serializer (passthrough)
/// </summary>
public class ByteArraySerializer : ISerializer<byte[]>
{
    /// <inheritdoc/>
    public byte[] Serialize(byte[] data, SerializationContext context)
    {
        return data ?? Array.Empty<byte>();
    }
}