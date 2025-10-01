using Xunit;
using AotKafka.Native;

namespace AotKafka.IntegrationTests;

[Collection("Kafka Collection")]
public class AdminIntegrationTests : IClassFixture<KafkaFixture>
{
    private readonly KafkaFixture _kafkaFixture;

    public AdminIntegrationTests(KafkaFixture kafkaFixture)
    {
        _kafkaFixture = kafkaFixture;
    }

    [Fact]
    public async Task CreateTopic_ShouldAllowProduceConsume()
    {
        var topic = $"admin-topic-{Guid.NewGuid()}";

        using var admin = new AdminClient(new AdminConfig
        {
            BootstrapServers = _kafkaFixture.BootstrapServers
        });

        await admin.CreateTopicAsync(topic, numPartitions: 3, replicationFactor: 1, operationTimeout: TimeSpan.FromSeconds(30));

        using var producer = new Producer<string, string>(new ProducerConfig
        {
            BootstrapServers = _kafkaFixture.BootstrapServers
        });

        await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = "admin-key",
            Value = "admin-value"
        });
        producer.Flush(TimeSpan.FromSeconds(5));

        using var consumer = new Consumer<string, string>(new ConsumerConfig
        {
            BootstrapServers = _kafkaFixture.BootstrapServers,
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
            BootstrapServers = _kafkaFixture.BootstrapServers
        });

        await admin.CreateTopicAsync(topic, numPartitions: 1, replicationFactor: 1, operationTimeout: TimeSpan.FromSeconds(15));

        var exception = await Assert.ThrowsAsync<KafkaException>(() =>
            admin.CreateTopicAsync(topic, numPartitions: 1, replicationFactor: 1, operationTimeout: TimeSpan.FromSeconds(10)));

        Assert.Equal(ErrorCode.TopicAlreadyExists, exception.Error.Code);
    }
}




