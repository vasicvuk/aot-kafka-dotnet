using System;
using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using Testcontainers.Kafka;
using Xunit;

namespace AotKafka.IntegrationTests;

/// <summary>
/// Shared Kafka container fixture for all integration tests
/// </summary>
public class KafkaFixture : IAsyncLifetime
{
    private KafkaContainer? _kafkaContainer;
    private string _bootstrapServers = string.Empty;

    public string BootstrapServers => _bootstrapServers;

    public string SaslBootstrapServers => _saslBootstrapServers;

    private string _saslBootstrapServers = string.Empty;
    public const ushort KafkaPort = 9092;
    public const ushort BrokerPort = 9093;
    public const ushort ControllerPort = 9094;

    public async Task InitializeAsync()
    {
        _kafkaContainer = new KafkaBuilder()
            .WithKRaft() // Enable KRaft mode (no Zookeeper)
            .WithImage("confluentinc/cp-kafka:7.5.0")
        .Build();
        // Start Kafka in KRaft mode (without Zookeeper)
        await _kafkaContainer.StartAsync();

        _bootstrapServers = _kafkaContainer.GetBootstrapAddress();
    }

    public async Task DisposeAsync()
    {
        if (_kafkaContainer != null)
        {
            await _kafkaContainer.DisposeAsync();
        }
    }
}

/// <summary>
/// Collection fixture to share Kafka container across multiple test classes
/// </summary>
[CollectionDefinition("Kafka Collection")]
public class KafkaCollection : ICollectionFixture<KafkaFixture>
{
    // This class has no implementation, it's just a marker for the collection
}
