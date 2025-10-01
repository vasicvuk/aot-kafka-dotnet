namespace AotKafka;

/// <summary>
/// Supported SASL mechanisms.
/// </summary>
public enum SaslMechanism
{
    /// <summary>
    /// SASL/PLAIN credentials.
    /// </summary>
    Plain,

    /// <summary>
    /// SCRAM mechanism using SHA-256 hashing.
    /// </summary>
    ScramSha256,

    /// <summary>
    /// SCRAM mechanism using SHA-512 hashing.
    /// </summary>
    ScramSha512
}

