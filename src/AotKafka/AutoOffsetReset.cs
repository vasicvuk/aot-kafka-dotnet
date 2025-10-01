namespace AotKafka;

/// <summary>
/// Action to take when there is no initial offset in Kafka or if the current offset no longer exists
/// </summary>
public enum AutoOffsetReset
{
    /// <summary>
    /// Automatically reset to earliest offset
    /// </summary>
    Earliest,

    /// <summary>
    /// Automatically reset to latest offset
    /// </summary>
    Latest,

    /// <summary>
    /// Trigger error when no offset available
    /// </summary>
    Error
}