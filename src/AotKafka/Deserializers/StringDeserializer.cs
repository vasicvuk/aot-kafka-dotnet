using System.Text;

namespace AotKafka.Deserializers;

/// <summary>
/// String deserializer (UTF-8 decoding)
/// </summary>
public class StringDeserializer : IDeserializer<string>
{
    /// <inheritdoc/>
    public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull || data.Length == 0)
        {
            return string.Empty;
        }

        return Encoding.UTF8.GetString(data);
    }
}