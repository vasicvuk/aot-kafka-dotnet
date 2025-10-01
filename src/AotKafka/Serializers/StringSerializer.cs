using System.Text;

namespace AotKafka.Serializers;

/// <summary>
/// String serializer (UTF-8 encoding)
/// </summary>
public class StringSerializer : ISerializer<string>
{
    /// <inheritdoc/>
    public byte[] Serialize(string data, SerializationContext context)
    {
        if (data == null)
        {
            return Array.Empty<byte>();
        }

        return Encoding.UTF8.GetBytes(data);
    }
}