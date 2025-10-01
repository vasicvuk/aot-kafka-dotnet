namespace AotKafka;

/// <summary>
/// Represents a Kafka message
/// </summary>
/// <typeparam name="TKey">Message key type</typeparam>
/// <typeparam name="TValue">Message value type</typeparam>
public class Message<TKey, TValue>
{
    /// <summary>
    /// Gets or sets the message key
    /// </summary>
    public TKey? Key { get; set; }

    /// <summary>
    /// Gets or sets the message value
    /// </summary>
    public TValue? Value { get; set; }

    /// <summary>
    /// Gets or sets the message headers
    /// </summary>
    public Headers? Headers { get; set; }

    /// <summary>
    /// Gets or sets the message timestamp
    /// </summary>
    public Timestamp Timestamp { get; set; } = Timestamp.Default;
}