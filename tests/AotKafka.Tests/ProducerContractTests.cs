using Xunit;

namespace AotKafka.Tests;

/// <summary>
/// Contract tests for Producer - verify API contracts without Kafka infrastructure
/// </summary>
public class ProducerContractTests
{
    [Fact]
    public void ProducerConfig_Creation_ShouldSucceed()
    {
        // Arrange & Act
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };

        // Assert
        Assert.NotNull(config);
        Assert.Equal("localhost:9092", config.BootstrapServers);
    }

    [Fact]
    public void Producer_Constructor_WithNullBootstrapServers_ShouldThrow()
    {
        // Arrange
        var config = new ProducerConfig
        {
            BootstrapServers = null!
        };

        // Act & Assert
        Assert.Throws<ArgumentException>(() =>
        {
            using var producer = new Producer<string, string>(config);
        });
    }

    [Fact]
    public void Producer_Constructor_WithEmptyBootstrapServers_ShouldThrow()
    {
        // Arrange
        var config = new ProducerConfig
        {
            BootstrapServers = ""
        };

        // Act & Assert
        Assert.Throws<ArgumentException>(() =>
        {
            using var producer = new Producer<string, string>(config);
        });
    }

    [Fact]
    public void ProducerConfig_SetProperties_ShouldWork()
    {
        // Arrange & Act
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
            ClientId = "test-producer",
            CompressionType = CompressionType.Gzip,
            MessageTimeoutMs = 5000,
            Acks = Acks.All,
            EnableIdempotence = true,
            MaxInFlight = 5,
            LingerMs = 10,
            BatchSize = 16384
        };

        // Assert
        Assert.Equal("localhost:9092", config.BootstrapServers);
        Assert.Equal("test-producer", config.ClientId);
        Assert.Equal(CompressionType.Gzip, config.CompressionType);
        Assert.Equal(5000, config.MessageTimeoutMs);
        Assert.Equal(Acks.All, config.Acks);
        Assert.True(config.EnableIdempotence);
        Assert.Equal(5, config.MaxInFlight);
        Assert.Equal(10, config.LingerMs);
        Assert.Equal(16384, config.BatchSize);
    }

    [Fact]
    public void Message_Creation_ShouldSucceed()
    {
        // Arrange & Act
        var message = new Message<string, string>
        {
            Key = "key1",
            Value = "value1"
        };

        // Assert
        Assert.Equal("key1", message.Key);
        Assert.Equal("value1", message.Value);
        Assert.Null(message.Headers);
        Assert.Equal(default, message.Timestamp);
    }

    [Fact]
    public void Message_WithHeaders_ShouldSucceed()
    {
        // Arrange
        var headers = new Headers();
        headers.Add("header1", System.Text.Encoding.UTF8.GetBytes("value1"));

        // Act
        var message = new Message<string, string>
        {
            Key = "key1",
            Value = "value1",
            Headers = headers
        };

        // Assert
        Assert.NotNull(message.Headers);
        Assert.Single(message.Headers);
    }
    [Fact]
    public void ProducerConfig_ToNativeConfig_WithSasl_ShouldPopulateValues()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "example:9093",
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            SaslMechanism = SaslMechanism.ScramSha512,
            SaslUsername = "user",
            SaslPassword = "pass"
        };

        var nativeConfig = config.ToNativeConfig();

        Assert.Equal("sasl_plaintext", nativeConfig["security.protocol"]);
        Assert.Equal("SCRAM-SHA-512", nativeConfig["sasl.mechanisms"]);
        Assert.Equal("user", nativeConfig["sasl.username"]);
        Assert.Equal("pass", nativeConfig["sasl.password"]);
    }

}
