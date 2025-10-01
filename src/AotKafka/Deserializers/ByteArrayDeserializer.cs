namespace AotKafka.Deserializers;

/// <summary>
/// Byte array deserializer (passthrough)
/// </summary>
public class ByteArrayDeserializer : IDeserializer<byte[]>
{
    /// <inheritdoc/>
    public byte[] Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull || data.Length == 0)
        {
            return Array.Empty<byte>();
        }

        return data.ToArray();
    }
}