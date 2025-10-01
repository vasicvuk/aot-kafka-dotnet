namespace AotKafka;

/// <summary>
/// Supported security protocols for Kafka connections.
/// </summary>
public enum SecurityProtocol
{
    /// <summary>
    /// Unencrypted plaintext communication.
    /// </summary>
    Plaintext,

    /// <summary>
    /// TLS encrypted communication without SASL.
    /// </summary>
    Ssl,

    /// <summary>
    /// SASL authentication over plaintext connections.
    /// </summary>
    SaslPlaintext,

    /// <summary>
    /// SASL authentication negotiated over TLS connections.
    /// </summary>
    SaslSsl
}

