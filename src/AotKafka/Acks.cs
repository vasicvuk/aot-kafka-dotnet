namespace AotKafka;

/// <summary>
/// Producer acknowledgment mode
/// </summary>
public enum Acks
{
    /// <summary>
    /// No acknowledgment (fire and forget)
    /// </summary>
    None = 0,

    /// <summary>
    /// Wait for leader acknowledgment only
    /// </summary>
    Leader = 1,

    /// <summary>
    /// Wait for all in-sync replicas to acknowledge
    /// </summary>
    All = -1
}