using System;
using System.Threading.Tasks;
using AotKafka;
using Xunit;

namespace AotKafka.IntegrationTests;

[Collection("Kafka Collection")]
public class SaslIntegrationTests : IClassFixture<KafkaFixture>
{
    private readonly KafkaFixture _saslKafkaFixture;

    public SaslIntegrationTests(KafkaFixture saslKafkaFixture)
    {
        _saslKafkaFixture = saslKafkaFixture;
    }

    [Fact]
    public async Task Sasl_ScramSha256_ShouldProduceAndConsume()
    {
        Console.WriteLine("=== Test: Sasl_ScramSha256_ShouldProduceAndConsume ===");
        var topic = $"sasl-scram256-{Guid.NewGuid():N}";
        Console.WriteLine($"Topic: {topic}");

        Console.WriteLine("Creating topic with admin client...");
        var adminConfig = CreateAdminConfig(SaslMechanism.ScramSha256, KafkaFixture.ScramUser, KafkaFixture.Scram256Pass);
        using (var admin = new AdminClient(adminConfig))
        {
            await admin.CreateTopicAsync(topic, numPartitions: 3, replicationFactor: 1, operationTimeout: TimeSpan.FromSeconds(120));
        }
        Console.WriteLine("Topic created successfully");

        Console.WriteLine("Producing message...");
        var producerConfig = CreateProducerConfig(SaslMechanism.ScramSha256, KafkaFixture.ScramUser, KafkaFixture.Scram256Pass);
        using var producer = new Producer<string, string>(producerConfig);
        await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = "sasl-key",
            Value = "sasl-value"
        });
        producer.Flush(TimeSpan.FromSeconds(10));
        Console.WriteLine("Message produced successfully");

        Console.WriteLine("Consuming message...");
        var consumerConfig = CreateConsumerConfig(SaslMechanism.ScramSha256, KafkaFixture.ScramUser, KafkaFixture.Scram256Pass);
        using var consumer = new Consumer<string, string>(consumerConfig);
        consumer.Subscribe(topic);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        ConsumeResult<string, string> result;
        try
        {
            result = consumer.Consume(cts.Token);
        }
        finally
        {
            consumer.Close();
        }

        Console.WriteLine($"Message consumed: Key={result.Message.Key}, Value={result.Message.Value}");
        Assert.NotNull(result.Message);
        Assert.Equal("sasl-value", result.Message.Value);
        Console.WriteLine("=== Test completed successfully ===");
    }

    [Fact]
    public async Task Sasl_ScramSha512_AdminAndProducer_ShouldWork()
    {
        Console.WriteLine("=== Test: Sasl_ScramSha512_AdminAndProducer_ShouldWork ===");
        var topic = $"sasl-scram512-{Guid.NewGuid():N}";
        Console.WriteLine($"Topic: {topic}");

        Console.WriteLine("Creating topic with admin client (SCRAM-SHA-512)...");
        var adminConfig = CreateAdminConfig(SaslMechanism.ScramSha512, KafkaFixture.ScramUser, KafkaFixture.Scram512Pass);
        using (var admin = new AdminClient(adminConfig))
        {
            await admin.CreateTopicAsync(topic, numPartitions: 2, replicationFactor: 1, operationTimeout: TimeSpan.FromSeconds(30));
        }
        Console.WriteLine("Topic created successfully");

        Console.WriteLine("Producing message (SCRAM-SHA-512)...");
        var producerConfig = CreateProducerConfig(SaslMechanism.ScramSha512, KafkaFixture.ScramUser, KafkaFixture.Scram512Pass);
        using var producer = new Producer<string, string>(producerConfig);
        await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = "scram512",
            Value = "scram512-value"
        });
        producer.Flush(TimeSpan.FromSeconds(10));
        Console.WriteLine("Message produced successfully");
        Console.WriteLine("=== Test completed successfully ===");
    }

    private AdminConfig CreateAdminConfig(SaslMechanism mechanism, string username, string password) => new()
    {
        BootstrapServers = _saslKafkaFixture.SaslBootstrapServers,
        SecurityProtocol = SecurityProtocol.SaslPlaintext,
        SaslMechanism = mechanism,
        SaslUsername = username,
        SaslPassword = password,
        RequestTimeoutMs = 15000,
        SocketTimeoutMs = 15000
    };

    private ProducerConfig CreateProducerConfig(SaslMechanism mechanism, string username, string password) => new()
    {
        BootstrapServers = _saslKafkaFixture.SaslBootstrapServers,
        SecurityProtocol = SecurityProtocol.SaslPlaintext,
        SaslMechanism = mechanism,
        SaslUsername = username,
        SaslPassword = password
    };

    private ConsumerConfig CreateConsumerConfig(SaslMechanism mechanism, string username, string password) => new()
    {
        BootstrapServers = _saslKafkaFixture.SaslBootstrapServers,
        GroupId = $"grp-{Guid.NewGuid():N}",
        SecurityProtocol = SecurityProtocol.SaslPlaintext,
        SaslMechanism = mechanism,
        SaslUsername = username,
        SaslPassword = password,
        AutoOffsetReset = AutoOffsetReset.Earliest
    };
}

