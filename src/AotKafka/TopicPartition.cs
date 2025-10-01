namespace AotKafka;

/// <summary>
/// Represents a specific partition within a Kafka topic
/// </summary>
public record TopicPartition
{
    /// <summary>
    /// Gets the topic name
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// Gets the partition
    /// </summary>
    public required Partition Partition { get; init; }

    /// <inheritdoc/>
    public override string ToString() => $"{Topic} [{Partition}]";
}