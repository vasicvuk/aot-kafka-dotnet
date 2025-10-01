using Xunit;
using Testcontainers.Redpanda;

namespace AotKafka.IntegrationTests;

/// <summary>
/// End-to-end integration tests for Producer and Consumer working together
/// </summary>
public class ProducerConsumerIntegrationTests : IAsyncLifetime
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
    public async Task EndToEnd_ProduceAndConsumeWithCompression_ShouldSucceed()
    {
        // Arrange
        var topic = "test-e2e-compression";
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers,
            CompressionType = CompressionType.Snappy
        };

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = $"e2e-compression-group-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        // Act - Produce
        using var producer = new Producer<string, string>(producerConfig);
        var largeValue = new string('X', 10000);
        await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = "compressed-key",
            Value = largeValue
        });
        producer.Flush(TimeSpan.FromSeconds(5));

        // Act - Consume
        using var consumer = new Consumer<string, string>(consumerConfig);
        consumer.Subscribe(topic);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var result = consumer.Consume(cts.Token);

        // Assert
        Assert.NotNull(result);
        Assert.Equal("compressed-key", result.Message.Key);
        Assert.Equal(largeValue, result.Message.Value);
    }

    [Fact]
    public async Task EndToEnd_MultipleProducersOneConsumer_ShouldConsumeAllMessages()
    {
        // Arrange
        var topic = "test-multi-producers";
        const int producerCount = 3;
        const int messagesPerProducer = 5;

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers
        };

        // Act - Multiple producers
        var produceTasks = new List<Task>();
        for (int p = 0; p < producerCount; p++)
        {
            var producerId = p;
            produceTasks.Add(Task.Run(async () =>
            {
                using var producer = new Producer<string, string>(producerConfig);
                for (int m = 0; m < messagesPerProducer; m++)
                {
                    await producer.ProduceAsync(topic, new Message<string, string>
                    {
                        Key = $"producer-{producerId}-msg-{m}",
                        Value = $"value-{producerId}-{m}"
                    });
                }
                producer.Flush(TimeSpan.FromSeconds(5));
            }));
        }

        await Task.WhenAll(produceTasks);

        // Act - Single consumer
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = $"multi-producer-group-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new Consumer<string, string>(consumerConfig);
        consumer.Subscribe(topic);

        var messages = new List<ConsumeResult<string, string>>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        for (int i = 0; i < producerCount * messagesPerProducer; i++)
        {
            var result = consumer.Consume(cts.Token);
            messages.Add(result);
        }

        // Assert
        Assert.Equal(producerCount * messagesPerProducer, messages.Count);
        var keys = messages.Select(m => m.Message.Key).ToHashSet();
        Assert.Equal(producerCount * messagesPerProducer, keys.Count);
    }

    [Fact]
    public async Task EndToEnd_ConsumerGroup_ShouldDistributeMessages()
    {
        // Arrange
        var topic = $"test-consumer-group-{Guid.NewGuid()}";
        var groupId = $"cg-{Guid.NewGuid()}";
        const int messageCount = 20;
        const int partitionCount = 4;

        using var admin = new AdminClient(new AdminConfig
        {
            BootstrapServers = _bootstrapServers
        });
        await admin.CreateTopicAsync(topic, partitionCount, replicationFactor: 1, operationTimeout: TimeSpan.FromSeconds(30));

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers
        };

        // Act - Produce messages
        using var producer = new Producer<string, string>(producerConfig);
        for (int i = 0; i < messageCount; i++)
        {
            await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = $"cg-key-{i}",
                Value = $"cg-value-{i}"
            });
        }
        producer.Flush(TimeSpan.FromSeconds(5));

        // Act - Two consumers in same group
        var consumerConfig1 = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        var consumerConfig2 = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer1 = new Consumer<string, string>(consumerConfig1);
        using var consumer2 = new Consumer<string, string>(consumerConfig2);

        consumer1.Subscribe(topic);
        consumer2.Subscribe(topic);

        var messages1 = new List<string>();
        var messages2 = new List<string>();

        // Give consumers time to join group and get partitions assigned
        await Task.Delay(2000);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        // Consume from both consumers
        var consume1 = Task.Run(() =>
        {
            try
            {
                while (!cts.Token.IsCancellationRequested && messages1.Count < messageCount / 2 + 5)
                {
                    var result = consumer1.Consume(cts.Token);
                    if (result != null && !result.IsPartitionEOF && result.Message.Key != null)
                    {
                        messages1.Add(result.Message.Key);
                    }
                }
            }
            catch (OperationCanceledException) { }
        });

        var consume2 = Task.Run(() =>
        {
            try
            {
                while (!cts.Token.IsCancellationRequested && messages2.Count < messageCount / 2 + 5)
                {
                    var result = consumer2.Consume(cts.Token);
                    if (result != null && !result.IsPartitionEOF && result.Message.Key != null)
                    {
                        messages2.Add(result.Message.Key);
                    }
                }
            }
            catch (OperationCanceledException) { }
        });

        await Task.WhenAny(Task.WhenAll(consume1, consume2), Task.Delay(TimeSpan.FromSeconds(15)));

        // Assert - Both consumers should have received messages
        var totalConsumed = messages1.Count + messages2.Count;
        Assert.True(totalConsumed >= messageCount, $"Expected at least {messageCount} messages, got {totalConsumed}");

        // No duplicates between consumers (consumer group semantics)
        var allKeys = messages1.Concat(messages2).ToList();
        var uniqueKeys = allKeys.Distinct().Count();
        Assert.Equal(allKeys.Count, uniqueKeys);
    }

    [Fact]
    public async Task EndToEnd_OrderingWithinPartition_ShouldBePreserved()
    {
        // Arrange
        var topic = "test-ordering";
        const int messageCount = 50;

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers
        };

        // Act - Produce messages with same key (goes to same partition)
        using var producer = new Producer<string, string>(producerConfig);
        for (int i = 0; i < messageCount; i++)
        {
            await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = "same-key", // Same key ensures same partition
                Value = $"message-{i}"
            });
        }
        producer.Flush(TimeSpan.FromSeconds(5));

        // Act - Consume in order
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = $"ordering-group-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new Consumer<string, string>(consumerConfig);
        consumer.Subscribe(topic);

        var messages = new List<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        for (int i = 0; i < messageCount; i++)
        {
            var result = consumer.Consume(cts.Token);
            Assert.NotNull(result.Message.Value);
            messages.Add(result.Message.Value);
        }

        // Assert - Messages should be in order
        for (int i = 0; i < messageCount; i++)
        {
            Assert.Equal($"message-{i}", messages[i]);
        }
    }

    [Fact]
    public async Task EndToEnd_LargeMessage_ShouldSucceed()
    {
        // Arrange
        var topic = "test-large-message";
        var largeValue = new string('A', 100_000); // 100KB message

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers,
            CompressionType = CompressionType.Lz4,
            BatchSize = 200_000 // Allow larger batches
        };

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = $"large-msg-group-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        // Act - Produce large message
        using var producer = new Producer<string, string>(producerConfig);
        await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = "large-key",
            Value = largeValue
        });
        producer.Flush(TimeSpan.FromSeconds(10));

        // Act - Consume large message
        using var consumer = new Consumer<string, string>(consumerConfig);
        consumer.Subscribe(topic);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var result = consumer.Consume(cts.Token);

        // Assert
        Assert.NotNull(result);
        Assert.NotNull(result.Message.Value);
        Assert.Equal(largeValue.Length, result.Message.Value.Length);
        Assert.Equal(largeValue, result.Message.Value);
    }
}

