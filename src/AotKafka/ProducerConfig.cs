namespace AotKafka;

/// <summary>
/// Producer configuration
/// </summary>
public class ProducerConfig
{
    /// <summary>
    /// Initial list of brokers as a CSV list of broker host or host:port
    /// </summary>
    public string BootstrapServers { get; set; } = null!;

    /// <summary>
    /// Client identifier
    /// </summary>
    public string? ClientId { get; set; }

    /// <summary>
    /// Security protocol to use for broker communication.
    /// </summary>
    public SecurityProtocol SecurityProtocol { get; set; } = SecurityProtocol.Plaintext;

    /// <summary>
    /// SASL mechanism to use when SecurityProtocol requires SASL.
    /// </summary>
    public SaslMechanism SaslMechanism { get; set; } = SaslMechanism.Plain;

    /// <summary>
    /// SASL username when SecurityProtocol is configured for SASL.
    /// </summary>
    public string? SaslUsername { get; set; }

    /// <summary>
    /// SASL password when SecurityProtocol is configured for SASL.
    /// </summary>
    public string? SaslPassword { get; set; }

    /// <summary>
    /// Compression codec to use for compressing message sets
    /// </summary>
    public CompressionType CompressionType { get; set; } = CompressionType.None;

    /// <summary>
    /// Local message timeout in milliseconds
    /// </summary>
    public int MessageTimeoutMs { get; set; } = 300000;

    /// <summary>
    /// Number of acknowledgments the producer requires the leader to have received
    /// </summary>
    public Acks Acks { get; set; } = Acks.All;

    /// <summary>
    /// When set to true, the producer will ensure that messages are successfully produced exactly once
    /// </summary>
    public bool EnableIdempotence { get; set; } = false;

    /// <summary>
    /// Maximum number of in-flight requests per broker connection
    /// </summary>
    public int MaxInFlight { get; set; } = 5;

    /// <summary>
    /// Delay in milliseconds to wait for messages in the producer queue to accumulate
    /// </summary>
    public int LingerMs { get; set; } = 0;

    /// <summary>
    /// Maximum size (in bytes) of all messages batched in one MessageSet
    /// </summary>
    public int BatchSize { get; set; } = 16384;

    internal Dictionary<string, string> ToNativeConfig()
    {
        if (string.IsNullOrWhiteSpace(BootstrapServers))
        {
            throw new ArgumentException("BootstrapServers must not be null or empty", nameof(BootstrapServers));
        }

        if (MessageTimeoutMs <= 0)
        {
            throw new ArgumentException("MessageTimeoutMs must be greater than 0", nameof(MessageTimeoutMs));
        }

        if (MaxInFlight <= 0)
        {
            throw new ArgumentException("MaxInFlight must be greater than 0", nameof(MaxInFlight));
        }

        if (EnableIdempotence && MaxInFlight > 5)
        {
            throw new ArgumentException("When EnableIdempotence is true, MaxInFlight must be <= 5", nameof(MaxInFlight));
        }

        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = BootstrapServers,
            ["compression.type"] = CompressionType.ToString().ToLowerInvariant(),
            ["message.timeout.ms"] = MessageTimeoutMs.ToString(),
            ["acks"] = Acks switch
            {
                Acks.None => "0",
                Acks.Leader => "1",
                Acks.All => "all",
                _ => "all"
            },
            ["enable.idempotence"] = EnableIdempotence.ToString().ToLowerInvariant(),
            ["max.in.flight.requests.per.connection"] = MaxInFlight.ToString(),
            ["linger.ms"] = LingerMs.ToString(),
            ["batch.size"] = BatchSize.ToString()
        };

        if (!string.IsNullOrWhiteSpace(ClientId))
        {
            config["client.id"] = ClientId;
        }

        config.ApplySecurity(SecurityProtocol, SaslMechanism, SaslUsername, SaslPassword, nameof(ProducerConfig));

        return config;
    }
}

