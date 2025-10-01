using Xunit;
using Testcontainers.Redpanda;

namespace AotKafka.IntegrationTests;

/// <summary>
/// Integration tests for Consumer using Redpanda container
/// </summary>
public class ConsumerIntegrationTests : IAsyncLifetime
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
    public void Consumer_CreateAndDispose_ShouldSucceed()
    {
        // Arrange
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "test-group"
        };

        // Act & Assert
        using var consumer = new Consumer<string, string>(config);
        Assert.NotNull(consumer);
        Assert.NotNull(consumer.Name);
    }

    [Fact]
    public void Consumer_Subscribe_ShouldSucceed()
    {
        // Arrange
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "test-group-subscribe"
        };

        using var consumer = new Consumer<string, string>(config);

        // Act & Assert
        var exception = Record.Exception(() => consumer.Subscribe("test-topic"));
        Assert.Null(exception);
    }

    [Fact]
    public void Consumer_SubscribeMultipleTopics_ShouldSucceed()
    {
        // Arrange
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "test-group-multi"
        };

        using var consumer = new Consumer<string, string>(config);

        // Act & Assert
        var exception = Record.Exception(() =>
            consumer.Subscribe(new[] { "topic1", "topic2", "topic3" }));
        Assert.Null(exception);
    }

    [Fact]
    public async Task Consumer_ProduceAndConsume_ShouldSucceed()
    {
        // Arrange - Produce a message first
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers
        };

        using var producer = new Producer<string, string>(producerConfig);
        var testMessage = new Message<string, string>
        {
            Key = "consume-test-key",
            Value = "consume-test-value"
        };

        await producer.ProduceAsync("test-consume-topic", testMessage);
        producer.Flush(TimeSpan.FromSeconds(5));

        // Act - Consume the message
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "test-consume-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new Consumer<string, string>(consumerConfig);
        consumer.Subscribe("test-consume-topic");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var result = consumer.Consume(cts.Token);

        // Assert
        Assert.NotNull(result);
        Assert.Equal("test-consume-topic", result.Topic);
        Assert.NotNull(result.Message);
        Assert.Equal("consume-test-key", result.Message.Key);
        Assert.Equal("consume-test-value", result.Message.Value);
    }

    [Fact]
    public async Task Consumer_ConsumeMultipleMessages_ShouldSucceed()
    {
        // Arrange - Produce multiple messages
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers
        };

        const int messageCount = 10;
        using var producer = new Producer<string, string>(producerConfig);

        for (int i = 0; i < messageCount; i++)
        {
            var message = new Message<string, string>
            {
                Key = $"multi-key-{i}",
                Value = $"multi-value-{i}"
            };
            await producer.ProduceAsync("test-multi-consume", message);
        }
        producer.Flush(TimeSpan.FromSeconds(5));

        // Act - Consume all messages
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "test-multi-consume-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var consumer = new Consumer<string, string>(consumerConfig);
        consumer.Subscribe("test-multi-consume");

        var consumedMessages = new List<ConsumeResult<string, string>>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        for (int i = 0; i < messageCount; i++)
        {
            var result = consumer.Consume(cts.Token);
            consumedMessages.Add(result);
        }

        // Assert
        Assert.Equal(messageCount, consumedMessages.Count);
        for (int i = 0; i < messageCount; i++)
        {
            Assert.Contains(consumedMessages, m => m.Message.Key == $"multi-key-{i}");
        }
    }

    [Fact]
    public async Task Consumer_WithManualCommit_ShouldSucceed()
    {
        // Arrange - Produce a message
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers
        };

        using var producer = new Producer<string, string>(producerConfig);
        await producer.ProduceAsync("test-manual-commit", new Message<string, string>
        {
            Key = "commit-key",
            Value = "commit-value"
        });
        producer.Flush(TimeSpan.FromSeconds(5));

        // Act - Consume and manually commit
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "test-manual-commit-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new Consumer<string, string>(consumerConfig);
        consumer.Subscribe("test-manual-commit");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var result = consumer.Consume(cts.Token);

        // Assert
        Assert.NotNull(result);
        var exception = Record.Exception(() => consumer.Commit());
        Assert.Null(exception);
    }

    [Fact]
    public async Task Consumer_WithByteArrayDeserializer_ShouldSucceed()
    {
        // Arrange - Produce binary message
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers
        };

        using var producer = new Producer<byte[], byte[]>(producerConfig);
        var keyBytes = new byte[] { 1, 2, 3 };
        var valueBytes = new byte[] { 4, 5, 6, 7, 8 };

        await producer.ProduceAsync("test-bytes-consume", new Message<byte[], byte[]>
        {
            Key = keyBytes,
            Value = valueBytes
        });
        producer.Flush(TimeSpan.FromSeconds(5));

        // Act - Consume binary message
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "test-bytes-consume-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new Consumer<byte[], byte[]>(consumerConfig);
        consumer.Subscribe("test-bytes-consume");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var result = consumer.Consume(cts.Token);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(keyBytes, result.Message.Key);
        Assert.Equal(valueBytes, result.Message.Value);
    }

    [Fact]
    public async Task Consumer_WithEarliestOffset_ShouldConsumeFromBeginning()
    {
        // Arrange - Produce messages
        var topicName = "test-earliest-offset";
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers
        };

        using var producer = new Producer<string, string>(producerConfig);
        await producer.ProduceAsync(topicName, new Message<string, string>
        {
            Key = "first",
            Value = "first-message"
        });
        producer.Flush(TimeSpan.FromSeconds(5));

        // Act - Consume from earliest
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = $"test-earliest-group-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new Consumer<string, string>(consumerConfig);
        consumer.Subscribe(topicName);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var result = consumer.Consume(cts.Token);

        // Assert
        Assert.NotNull(result);
        Assert.Equal("first", result.Message.Key);
        Assert.Equal("first-message", result.Message.Value);
    }

    [Fact]
    public void Consumer_Close_ShouldSucceed()
    {
        // Arrange
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "test-close-group"
        };

        using var consumer = new Consumer<string, string>(config);
        consumer.Subscribe("test-topic");

        // Act & Assert
        var exception = Record.Exception(() => consumer.Close());
        Assert.Null(exception);
    }
}
