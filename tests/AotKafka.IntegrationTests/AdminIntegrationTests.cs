using Xunit;
using Testcontainers.Redpanda;
using AotKafka.Native;

namespace AotKafka.IntegrationTests;

public class AdminIntegrationTests : IAsyncLifetime
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
    public async Task CreateTopic_ShouldAllowProduceConsume()
    {
        var topic = $"admin-topic-{Guid.NewGuid()}";

        using var admin = new AdminClient(new AdminConfig
        {
            BootstrapServers = _bootstrapServers
        });

        await admin.CreateTopicAsync(topic, numPartitions: 3, replicationFactor: 1, operationTimeout: TimeSpan.FromSeconds(30));

        using var producer = new Producer<string, string>(new ProducerConfig
        {
            BootstrapServers = _bootstrapServers
        });

        await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = "admin-key",
            Value = "admin-value"
        });
        producer.Flush(TimeSpan.FromSeconds(5));

        using var consumer = new Consumer<string, string>(new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = $"admin-consumer-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        });

        consumer.Subscribe(topic);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var result = consumer.Consume(cts.Token);

        Assert.NotNull(result.Message);
        Assert.Equal("admin-value", result.Message.Value);
    }

    [Fact]
    public async Task CreateTopic_WhenTopicExists_ShouldThrow()
    {
        var topic = $"admin-duplicate-{Guid.NewGuid()}";

        using var admin = new AdminClient(new AdminConfig
        {
            BootstrapServers = _bootstrapServers
        });

        await admin.CreateTopicAsync(topic, numPartitions: 1, replicationFactor: 1, operationTimeout: TimeSpan.FromSeconds(15));

        var exception = await Assert.ThrowsAsync<KafkaException>(() =>
            admin.CreateTopicAsync(topic, numPartitions: 1, replicationFactor: 1, operationTimeout: TimeSpan.FromSeconds(10)));

        Assert.Equal(ErrorCode.TopicAlreadyExists, exception.Error.Code);
    }
}




