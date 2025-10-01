namespace AotKafka;

/// <summary>
/// Deserializes data of type <typeparamref name="T"/> from a byte array
/// </summary>
public interface IDeserializer<T>
{
    /// <summary>
    /// Deserialize data
    /// </summary>
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context);
}