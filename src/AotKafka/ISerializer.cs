namespace AotKafka;

/// <summary>
/// Serializes data of type <typeparamref name="T"/> to a byte array
/// </summary>
public interface ISerializer<T>
{
    /// <summary>
    /// Serialize data
    /// </summary>
    public byte[] Serialize(T data, SerializationContext context);
}