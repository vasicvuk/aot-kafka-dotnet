using Xunit;
using Testcontainers.Redpanda;

namespace AotKafka.IntegrationTests;

/// <summary>
/// Integration tests for Producer using Redpanda container
/// </summary>
public class ProducerIntegrationTests : IAsyncLifetime
{
    private RedpandaContainer? _redpanda;
    private string _bootstrapServers = string.Empty;

    public async Task InitializeAsync()
    {
        _redpanda = new RedpandaBuilder()
            .WithImage("docker.redpanda.com/redpandadata/redpanda:v24.2.4")
            .Build();

        await _redpanda.StartAsync();
        _bootstrapServers = _redpanda.GetBootstrapAddress();
    }

    public async Task DisposeAsync()
    {
        if (_redpanda != null)
        {
            await _redpanda.DisposeAsync();
        }
    }

    [Fact]
    public void Producer_CreateAndDispose_ShouldSucceed()
    {
        // Arrange
        var config = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers
        };

        // Act & Assert
        using var producer = new Producer<string, string>(config);
        Assert.NotNull(producer);
        Assert.NotNull(producer.Name);
    }

    [Fact]
    public void Producer_ProduceSync_ShouldSucceed()
    {
        // Arrange
        var config = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers
        };

        using var producer = new Producer<string, string>(config);
        var message = new Message<string, string>
        {
            Key = "test-key",
            Value = "test-value"
        };

        // Act
        var exception = Record.Exception(() =>
        {
            producer.Produce("test-topic", message, deliveryReport =>
            {
                Assert.NotNull(deliveryReport);
                Assert.Equal("test-topic", deliveryReport.Topic);
            });

            producer.Flush(TimeSpan.FromSeconds(10));
        });

        // Assert
        Assert.Null(exception);
    }

    [Fact]
    public async Task Producer_ProduceAsync_ShouldSucceed()
    {
        // Arrange
        var config = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers
        };

        using var producer = new Producer<string, string>(config);
        var message = new Message<string, string>
        {
            Key = "async-key",
            Value = "async-value"
        };

        // Act
        var result = await producer.ProduceAsync("test-topic-async", message);

        // Assert
        Assert.NotNull(result);
        Assert.Equal("test-topic-async", result.Topic);
        Assert.Equal(PersistenceStatus.Persisted, result.Status);
    }

    [Fact]
    public async Task Producer_ProduceMultipleMessages_ShouldSucceed()
    {
        // Arrange
        var config = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers,
            LingerMs = 100,
            BatchSize = 1024
        };

        using var producer = new Producer<string, string>(config);
        const int messageCount = 100;

        // Act
        var tasks = new List<Task<DeliveryResult<string, string>>>();
        for (int i = 0; i < messageCount; i++)
        {
            var message = new Message<string, string>
            {
                Key = $"key-{i}",
                Value = $"value-{i}"
            };
            tasks.Add(producer.ProduceAsync("test-topic-multi", message));
        }

        var results = await Task.WhenAll(tasks);

        // Assert
        Assert.Equal(messageCount, results.Length);
        Assert.All(results, result =>
        {
            Assert.NotNull(result);
            Assert.Equal("test-topic-multi", result.Topic);
            Assert.Equal(PersistenceStatus.Persisted, result.Status);
        });
    }

    [Fact]
    public async Task Producer_WithCompression_ShouldSucceed()
    {
        // Arrange
        var config = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers,
            CompressionType = CompressionType.Gzip
        };

        using var producer = new Producer<string, string>(config);
        var message = new Message<string, string>
        {
            Key = "compressed-key",
            Value = new string('x', 1000) // Large value to benefit from compression
        };

        // Act
        var result = await producer.ProduceAsync("test-topic-compressed", message);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(PersistenceStatus.Persisted, result.Status);
    }

    [Fact]
    public async Task Producer_WithIdempotence_ShouldSucceed()
    {
        // Arrange
        var config = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers,
            EnableIdempotence = true,
            MaxInFlight = 5,
            Acks = Acks.All
        };

        using var producer = new Producer<string, string>(config);
        var message = new Message<string, string>
        {
            Key = "idempotent-key",
            Value = "idempotent-value"
        };

        // Act
        var result = await producer.ProduceAsync("test-topic-idempotent", message);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(PersistenceStatus.Persisted, result.Status);
    }

    [Fact]
    public async Task Producer_WithByteArraySerializer_ShouldSucceed()
    {
        // Arrange
        var config = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers
        };

        using var producer = new Producer<byte[], byte[]>(config);
        var message = new Message<byte[], byte[]>
        {
            Key = new byte[] { 1, 2, 3 },
            Value = new byte[] { 4, 5, 6, 7, 8 }
        };

        // Act
        var result = await producer.ProduceAsync("test-topic-bytes", message);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(PersistenceStatus.Persisted, result.Status);
    }

    [Fact]
    public void Producer_Flush_ShouldWaitForMessages()
    {
        // Arrange
        var config = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers
        };

        using var producer = new Producer<string, string>(config);

        // Send several messages
        for (int i = 0; i < 10; i++)
        {
            var message = new Message<string, string>
            {
                Key = $"flush-key-{i}",
                Value = $"flush-value-{i}"
            };
            producer.Produce("test-topic-flush", message);
        }

        // Act
        var remainingMessages = producer.Flush(TimeSpan.FromSeconds(10));

        // Assert
        Assert.Equal(0, remainingMessages);
    }
}
