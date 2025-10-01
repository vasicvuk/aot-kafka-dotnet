namespace AotKafka;

/// <summary>
/// Message persistence status
/// </summary>
public enum PersistenceStatus
{
    /// <summary>
    /// Message was never transmitted to the broker
    /// </summary>
    NotPersisted = 0,

    /// <summary>
    /// Message was transmitted but acknowledgment was not received
    /// </summary>
    PossiblyPersisted = 1,

    /// <summary>
    /// Message was successfully acknowledged by the broker
    /// </summary>
    Persisted = 2
}