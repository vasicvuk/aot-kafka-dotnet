namespace AotKafka;

/// <summary>
/// The result of a consume operation
/// </summary>
/// <typeparam name="TKey">Message key type</typeparam>
/// <typeparam name="TValue">Message value type</typeparam>
public class ConsumeResult<TKey, TValue>
{
    /// <summary>
    /// Gets the consumed message
    /// </summary>
    public required Message<TKey, TValue> Message { get; init; }

    /// <summary>
    /// Gets the topic the message was consumed from
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// Gets the partition the message was consumed from
    /// </summary>
    public required Partition Partition { get; init; }

    /// <summary>
    /// Gets the offset of the consumed message
    /// </summary>
    public required Offset Offset { get; init; }

    /// <summary>
    /// Gets whether this represents end of partition
    /// </summary>
    public bool IsPartitionEOF { get; init; }

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