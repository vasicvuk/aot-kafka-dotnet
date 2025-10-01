using Xunit;

namespace AotKafka.Tests;

/// <summary>
/// Contract tests for Consumer - verify API contracts without Kafka infrastructure
/// </summary>
public class ConsumerContractTests
{
    [Fact]
    public void ConsumerConfig_Creation_ShouldSucceed()
    {
        // Arrange & Act
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "test-group"
        };

        // Assert
        Assert.NotNull(config);
        Assert.Equal("localhost:9092", config.BootstrapServers);
        Assert.Equal("test-group", config.GroupId);
    }

    [Fact]
    public void Consumer_Constructor_WithNullBootstrapServers_ShouldThrow()
    {
        // Arrange
        var config = new ConsumerConfig
        {
            BootstrapServers = null!,
            GroupId = "test-group"
        };

        // Act & Assert
        Assert.Throws<ArgumentException>(() =>
        {
            using var consumer = new Consumer<string, string>(config);
        });
    }

    [Fact]
    public void Consumer_Constructor_WithNullGroupId_ShouldThrow()
    {
        // Arrange
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = null!
        };

        // Act & Assert
        Assert.Throws<ArgumentException>(() =>
        {
            using var consumer = new Consumer<string, string>(config);
        });
    }

    [Fact]
    public void ConsumerConfig_SetProperties_ShouldWork()
    {
        // Arrange & Act
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "test-group",
            ClientId = "test-consumer",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            AutoCommitIntervalMs = 5000,
            SessionTimeoutMs = 10000,
            MaxPollIntervalMs = 300000,
            EnablePartitionEof = true
        };

        // Assert
        Assert.Equal("localhost:9092", config.BootstrapServers);
        Assert.Equal("test-group", config.GroupId);
        Assert.Equal("test-consumer", config.ClientId);
        Assert.Equal(AutoOffsetReset.Earliest, config.AutoOffsetReset);
        Assert.False(config.EnableAutoCommit);
        Assert.Equal(5000, config.AutoCommitIntervalMs);
        Assert.Equal(10000, config.SessionTimeoutMs);
        Assert.Equal(300000, config.MaxPollIntervalMs);
        Assert.True(config.EnablePartitionEof);
    }

    [Fact]
    public void ConsumeResult_Creation_ShouldSucceed()
    {
        // Arrange & Act
        var result = new ConsumeResult<string, string>
        {
            Topic = "test-topic",
            Partition = new Partition(0),
            Offset = new Offset(10),
            Message = new Message<string, string>
            {
                Key = "key1",
                Value = "value1"
            },
            IsPartitionEOF = false
        };

        // Assert
        Assert.Equal("test-topic", result.Topic);
        Assert.Equal(0, result.Partition.Value);
        Assert.Equal(10, result.Offset.Value);
        Assert.NotNull(result.Message);
        Assert.False(result.IsPartitionEOF);
    }

    [Fact]
    public void TopicPartition_Creation_ShouldSucceed()
    {
        // Arrange & Act
        var topicPartition = new TopicPartition
        {
            Topic = "test-topic",
            Partition = new Partition(5)
        };

        // Assert
        Assert.Equal("test-topic", topicPartition.Topic);
        Assert.Equal(5, topicPartition.Partition.Value);
        Assert.Contains("test-topic", topicPartition.ToString());
        Assert.Contains("5", topicPartition.ToString());
    }

    [Fact]
    public void TopicPartitionOffset_Creation_ShouldSucceed()
    {
        // Arrange & Act
        var tpo = new TopicPartitionOffset
        {
            Topic = "test-topic",
            Partition = new Partition(3),
            Offset = new Offset(100)
        };

        // Assert
        Assert.Equal("test-topic", tpo.Topic);
        Assert.Equal(3, tpo.Partition.Value);
        Assert.Equal(100, tpo.Offset.Value);
        Assert.Contains("test-topic", tpo.ToString());
        Assert.Contains("3", tpo.ToString());
        Assert.Contains("100", tpo.ToString());
    }
    [Fact]
    public void ConsumerConfig_ToNativeConfig_WithSasl_ShouldPopulateValues()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "example:9093",
            GroupId = "group",
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            SaslMechanism = SaslMechanism.ScramSha256,
            SaslUsername = "user",
            SaslPassword = "pass"
        };

        var nativeConfig = config.ToNativeConfig();

        Assert.Equal("sasl_plaintext", nativeConfig["security.protocol"]);
        Assert.Equal("SCRAM-SHA-256", nativeConfig["sasl.mechanisms"]);
        Assert.Equal("user", nativeConfig["sasl.username"]);
        Assert.Equal("pass", nativeConfig["sasl.password"]);
    }

    [Fact]
    public void ConsumerConfig_ToNativeConfig_WithSaslMissingCredentials_ShouldThrow()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "example:9093",
            GroupId = "group",
            SecurityProtocol = SecurityProtocol.SaslPlaintext
        };

        Assert.Throws<ArgumentException>(() => config.ToNativeConfig());
    }

}
