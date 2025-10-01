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
            ["socket.timeout.ms"] = SocketTimeoutMs.ToString()
        };

        if (!string.IsNullOrWhiteSpace(ClientId))
        {
            config["client.id"] = ClientId;
        }

        config.ApplySecurity(SecurityProtocol, SaslMechanism, SaslUsername, SaslPassword, nameof(AdminConfig));

        return config;
    }
}

