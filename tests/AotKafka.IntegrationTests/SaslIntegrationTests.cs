using System;
using System.Threading.Tasks;
using AotKafka;
using Xunit;

namespace AotKafka.IntegrationTests;

[Collection("SASL Kafka Collection")]
public class SaslIntegrationTests : IClassFixture<KafkaFixtureSasl>
{
    private readonly KafkaFixtureSasl _saslKafkaFixture;
    public SaslIntegrationTests(KafkaFixtureSasl saslKafkaFixture)
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
        var adminConfig = CreateAdminConfig(SaslMechanism.ScramSha256, KafkaFixtureSasl.ScramUser, KafkaFixtureSasl.Scram256Pass);
        Console.WriteLine($"Admin config debug: {adminConfig.Debug}, Log level: {adminConfig.LogLevel}");
        Console.WriteLine($"Connecting to: {_saslKafkaFixture.BootstrapServers}");
        using (var admin = new AdminClient(adminConfig))
        {
            Console.WriteLine($"Admin client created: {admin.Name}");
            Console.WriteLine("Waiting 2 seconds for broker connection to stabilize...");
            await Task.Delay(2000);
            await admin.CreateTopicAsync(topic, numPartitions: 3, replicationFactor: 1, operationTimeout: TimeSpan.FromSeconds(180));
        }
        Console.WriteLine("Topic created successfully");

        Console.WriteLine("Producing message...");
        var producerConfig = CreateProducerConfig(SaslMechanism.ScramSha256, KafkaFixtureSasl.ScramUser, KafkaFixtureSasl.Scram256Pass);
        using var producer = new Producer<string, string>(producerConfig);
        await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = "sasl-key",
            Value = "sasl-value"
        });
        producer.Flush(TimeSpan.FromSeconds(10));
        Console.WriteLine("Message produced successfully");

        Console.WriteLine("Consuming message...");
        var consumerConfig = CreateConsumerConfig(SaslMechanism.ScramSha256, KafkaFixtureSasl.ScramUser, KafkaFixtureSasl.Scram256Pass);
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
        var adminConfig = CreateAdminConfig(SaslMechanism.ScramSha512, KafkaFixtureSasl.ScramUser, KafkaFixtureSasl.Scram512Pass);
        Console.WriteLine($"Admin config debug: {adminConfig.Debug}, Log level: {adminConfig.LogLevel}");
        Console.WriteLine($"Connecting to: {_saslKafkaFixture.BootstrapServers}");
        using (var admin = new AdminClient(adminConfig))
        {
            Console.WriteLine($"Admin client created: {admin.Name}");
            Console.WriteLine("Waiting 2 seconds for broker connection to stabilize...");
            await Task.Delay(2000);
            await admin.CreateTopicAsync(topic, numPartitions: 2, replicationFactor: 1, operationTimeout: TimeSpan.FromSeconds(180));
        }
        Console.WriteLine("Topic created successfully");

        Console.WriteLine("Producing message (SCRAM-SHA-512)...");
        var producerConfig = CreateProducerConfig(SaslMechanism.ScramSha512, KafkaFixtureSasl.ScramUser, KafkaFixtureSasl.Scram512Pass);
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
        BootstrapServers = _saslKafkaFixture.BootstrapServers,
        SecurityProtocol = SecurityProtocol.SaslPlaintext,
        SaslMechanism = mechanism,
        SaslUsername = username,
        SaslPassword = password,
        RequestTimeoutMs = 60000,        // Increased to 60 seconds
        SocketTimeoutMs = 30000,        // Increased to 30 seconds
        MetadataRequestTimeoutMs = 60000,      // Added metadata timeout
        SocketConnectionSetupTimeoutMs = 45000, // Added connection setup timeout
        MetadataMaxAgeMs = 90000,        // Increased metadata max age
        LogLevel = 7,                   // DEBUG level for maximum verbosity
        Debug = "all"                   // Enable all debug contexts
    };

    private ProducerConfig CreateProducerConfig(SaslMechanism mechanism, string username, string password) => new()
    {
        BootstrapServers = _saslKafkaFixture.BootstrapServers,
        SecurityProtocol = SecurityProtocol.SaslPlaintext,
        SaslMechanism = mechanism,
        SaslUsername = username,
        SaslPassword = password,
        MetadataRequestTimeoutMs = 60000,
        SocketConnectionSetupTimeoutMs = 45000,
        LogLevel = 7,
        Debug = "all"
    };

    private ConsumerConfig CreateConsumerConfig(SaslMechanism mechanism, string username, string password) => new()
    {
        BootstrapServers = _saslKafkaFixture.BootstrapServers,
        GroupId = $"grp-{Guid.NewGuid():N}",
        SecurityProtocol = SecurityProtocol.SaslPlaintext,
        SaslMechanism = mechanism,
        SaslUsername = username,
        SaslPassword = password,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        MetadataRequestTimeoutMs = 60000,
        SocketConnectionSetupTimeoutMs = 45000,
        LogLevel = 7,
        Debug = "all"
    };
}

