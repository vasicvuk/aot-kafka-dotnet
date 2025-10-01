namespace AotKafka;

/// <summary>
/// The result of a produce operation
/// </summary>
/// <typeparam name="TKey">Message key type</typeparam>
/// <typeparam name="TValue">Message value type</typeparam>
public class DeliveryResult<TKey, TValue>
{
    /// <summary>
    /// Gets the message that was delivered
    /// </summary>
    public required Message<TKey, TValue> Message { get; init; }

    /// <summary>
    /// Gets the topic the message was delivered to
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// Gets the partition the message was delivered to
    /// </summary>
    public required Partition Partition { get; init; }

    /// <summary>
    /// Gets the offset of the produced message
    /// </summary>
    public required Offset Offset { get; init; }

    /// <summary>
    /// Gets the persistence status of the produced message
    /// </summary>
    public required PersistenceStatus Status { get; init; }

    /// <summary>
    /// Gets the topic/partition/offset
    /// </summary>
    public TopicPartitionOffset TopicPartitionOffset => new()
    {
        Topic = Topic,
        Partition = Partition,
        Offset = Offset
    };
}