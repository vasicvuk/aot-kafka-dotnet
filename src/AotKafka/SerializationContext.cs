namespace AotKafka;

/// <summary>
/// Context information for serialization
/// </summary>
public class SerializationContext
{
    /// <summary>
    /// Gets the topic name
    /// </summary>
    public string Topic { get; init; } = null!;

    /// <summary>
    /// Gets the partition
    /// </summary>
    public Partition Partition { get; init; }

    /// <summary>
    /// Gets the headers
    /// </summary>
    public Headers? Headers { get; init; }
}