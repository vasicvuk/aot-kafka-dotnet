namespace AotKafka;

/// <summary>
/// Consumer configuration
/// </summary>
public class ConsumerConfig
{
    /// <summary>
    /// Initial list of brokers as a CSV list of broker host or host:port
    /// </summary>
    public string BootstrapServers { get; set; } = null!;

    /// <summary>
    /// Client group id string. All clients sharing the same group.id belong to the same group
    /// </summary>
    public string GroupId { get; set; } = null!;

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
    /// Action to take when there is no initial offset in Kafka
    /// </summary>
    public AutoOffsetReset AutoOffsetReset { get; set; } = AutoOffsetReset.Latest;

    /// <summary>
    /// Automatically commit offsets in the background
    /// </summary>
    public bool EnableAutoCommit { get; set; } = true;

    /// <summary>
    /// The frequency in milliseconds that the consumer offsets are committed
    /// </summary>
    public int AutoCommitIntervalMs { get; set; } = 5000;

    /// <summary>
    /// Client group session and failure detection timeout
    /// </summary>
    public int SessionTimeoutMs { get; set; } = 10000;

    /// <summary>
    /// Maximum allowed time between calls to consume
    /// </summary>
    public int MaxPollIntervalMs { get; set; } = 300000;

    /// <summary>
    /// Emit RD_KAFKA_RESP_ERR__PARTITION_EOF event whenever the consumer reaches the end of a partition
    /// </summary>
    public bool EnablePartitionEof { get; set; } = false;

    internal Dictionary<string, string> ToNativeConfig()
    {
        if (string.IsNullOrWhiteSpace(BootstrapServers))
        {
            throw new ArgumentException("BootstrapServers must not be null or empty", nameof(BootstrapServers));
        }

        if (string.IsNullOrWhiteSpace(GroupId))
        {
            throw new ArgumentException("GroupId must not be null or empty", nameof(GroupId));
        }

        if (SessionTimeoutMs <= 0)
        {
            throw new ArgumentException("SessionTimeoutMs must be greater than 0", nameof(SessionTimeoutMs));
        }

        if (MaxPollIntervalMs <= SessionTimeoutMs)
        {
            throw new ArgumentException("MaxPollIntervalMs must be greater than SessionTimeoutMs", nameof(MaxPollIntervalMs));
        }

        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = BootstrapServers,
            ["group.id"] = GroupId,
            ["auto.offset.reset"] = AutoOffsetReset.ToString().ToLowerInvariant(),
            ["enable.auto.commit"] = EnableAutoCommit.ToString().ToLowerInvariant(),
            ["auto.commit.interval.ms"] = AutoCommitIntervalMs.ToString(),
            ["session.timeout.ms"] = SessionTimeoutMs.ToString(),
            ["max.poll.interval.ms"] = MaxPollIntervalMs.ToString(),
            ["enable.partition.eof"] = EnablePartitionEof.ToString().ToLowerInvariant()
        };

        if (!string.IsNullOrWhiteSpace(ClientId))
        {
            config["client.id"] = ClientId;
        }

        config.ApplySecurity(SecurityProtocol, SaslMechanism, SaslUsername, SaslPassword, nameof(ConsumerConfig));

        return config;
    }
}

