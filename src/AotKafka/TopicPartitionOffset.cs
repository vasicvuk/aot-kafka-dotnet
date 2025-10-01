namespace AotKafka;

/// <summary>
/// Represents a specific offset within a topic partition
/// </summary>
public record TopicPartitionOffset : TopicPartition
{
    /// <summary>
    /// Gets the offset
    /// </summary>
    public required Offset Offset { get; init; }

    /// <inheritdoc/>
    public override string ToString() => $"{Topic} [{Partition}] @{Offset}";
}