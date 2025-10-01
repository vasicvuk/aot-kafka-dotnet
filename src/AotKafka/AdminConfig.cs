namespace AotKafka;

/// <summary>
/// Configuration for the Kafka admin client.
/// </summary>
public class AdminConfig
{
    /// <summary>
    /// Initial list of brokers as a CSV list of host or host:port.
    /// </summary>
    public string BootstrapServers { get; set; } = null!;

    /// <summary>
    /// Optional client identifier.
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
    /// Maximum time the client will wait for an admin request to complete.
    /// </summary>
    public int RequestTimeoutMs { get; set; } = 10000;

    /// <summary>
    /// Network level timeout for socket operations.
    /// </summary>
    public int SocketTimeoutMs { get; set; } = 10000;

    /// <summary>
    /// Enable librdkafka debug logging. Comma-separated list of debug contexts.
    /// Common values: all, broker, topic, metadata, security, protocol, queue.
    /// </summary>
    public string? Debug { get; set; }

    /// <summary>
    /// Log level for librdkafka. Values: 0=EMERG, 1=ALERT, 2=CRIT, 3=ERROR, 4=WARN, 5=NOTICE, 6=INFO, 7=DEBUG.
    /// </summary>
    public int LogLevel { get; set; } = 6; // INFO level

    /// <summary>
    /// Time to wait for broker metadata to be refreshed.
    /// </summary>
    public int MetadataMaxAgeMs { get; set; } = 90000;

    /// <summary>
    /// Maximum time to wait for broker metadata updates.
    /// </summary>
    public int MetadataRequestTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Number of seconds to wait for broker connection to be established.
    /// </summary>
    public int SocketConnectionSetupTimeoutMs { get; set; } = 30000;

    internal Dictionary<string, string> ToNativeConfig()
    {
        if (string.IsNullOrWhiteSpace(BootstrapServers))
        {
            throw new ArgumentException("BootstrapServers must not be null or empty", nameof(BootstrapServers));
        }

        if (RequestTimeoutMs <= 0)
        {
            throw new ArgumentException("RequestTimeoutMs must be greater than 0", nameof(RequestTimeoutMs));
        }

        if (SocketTimeoutMs <= 0)
        {
            throw new ArgumentException("SocketTimeoutMs must be greater than 0", nameof(SocketTimeoutMs));
        }

        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = BootstrapServers,
            ["request.timeout.ms"] = RequestTimeoutMs.ToString(),
            ["socket.timeout.ms"] = SocketTimeoutMs.ToString(),
            ["metadata.request.timeout.ms"] = MetadataRequestTimeoutMs.ToString(),
            ["socket.connection.setup.timeout.ms"] = SocketConnectionSetupTimeoutMs.ToString(),
            ["log_level"] = LogLevel.ToString(),
         };

        if (!string.IsNullOrWhiteSpace(ClientId))
        {
            config["client.id"] = ClientId;
        }

        if (!string.IsNullOrWhiteSpace(Debug))
        {
            config["debug"] = Debug;
        }

        config.ApplySecurity(SecurityProtocol, SaslMechanism, SaslUsername, SaslPassword, nameof(AdminConfig));

        return config;
    }
}

