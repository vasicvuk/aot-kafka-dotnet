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
public class KafkaFixtureSasl : IAsyncLifetime
{
    private KafkaContainer? _kafkaContainer;
    private string _bootstrapServers = string.Empty;

    public string BootstrapServers => _bootstrapServers;


    public const ushort KafkaPort = 9092;
    public const ushort BrokerPort = 9093;
    public const ushort ControllerPort = 9094;
    public const string ScramUser = "admin";
    public const string Scram256Pass = "admin-secret";
    public const string Scram512Pass = "user512-secret";

    private static int GetFreeTcpPort()
    {
        var l = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        l.Start();
        int port = ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }
    public async Task InitializeAsync()
    {
        var hostJaasPath = Path.Combine(Path.GetTempPath(), "kafka_jaas.conf");
        Directory.CreateDirectory(Path.GetDirectoryName(hostJaasPath)!);
        File.WriteAllText(hostJaasPath,
        @"KafkaServer {
  org.apache.kafka.common.security.scram.ScramLoginModule required;
};
KafkaClient {
  org.apache.kafka.common.security.scram.ScramLoginModule required;
};");

        int hostPlaintext = GetFreeTcpPort();
        int hostSasl = GetFreeTcpPort();

        _kafkaContainer = new KafkaBuilder()
            .WithKRaft() // Enable KRaft mode (no Zookeeper)
            .WithImage("confluentinc/cp-kafka:7.5.0")
           .WithPortBinding(hostPlaintext, KafkaPort)  // hostPlaintext : 9092

  // listeners: two external + two internal
  .WithEnvironment("KAFKA_LISTENERS",
    $"PLAINTEXT://:{KafkaPort},BROKER://:{BrokerPort},CONTROLLER://:{ControllerPort}")
  .WithEnvironment("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
    "PLAINTEXT:SASL_PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT")
  .WithEnvironment("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
.WithEnvironment("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")

  // IMPORTANT: advertise the *host* ports you just picked
  .WithEnvironment("KAFKA_ADVERTISED_LISTENERS",
    $"PLAINTEXT://127.0.0.1:{hostPlaintext}," +
    $"BROKER://127.0.0.1:{BrokerPort}") // BROKER only used internally

  // Enable SCRAM on the SASL listener
  .WithEnvironment("KAFKA_SASL_ENABLED_MECHANISMS", "SCRAM-SHA-256,SCRAM-SHA-512")
  .WithEnvironment("KAFKA_LISTENER_NAME_SASL_PLAINTEXT_SASL_ENABLED_MECHANISMS",
    "SCRAM-SHA-256,SCRAM-SHA-512")
  .WithEnvironment("KAFKA_LISTENER_NAME_SASL_PLAINTEXT_SCRAM_SHA_256_SASL_JAAS_CONFIG",
    "org.apache.kafka.common.security.scram.ScramLoginModule required;")
  .WithEnvironment("KAFKA_LISTENER_NAME_SASL_PLAINTEXT_SCRAM_SHA_512_SASL_JAAS_CONFIG",
    "org.apache.kafka.common.security.scram.ScramLoginModule required;")

  // JAAS file if you keep it
  .WithEnvironment("KAFKA_OPTS", "-Djava.security.auth.login.config=/etc/kafka/jaas.conf")
  .WithBindMount(hostJaasPath, "/etc/kafka/jaas.conf", accessMode: AccessMode.ReadOnly)
  .Build();
        // Start Kafka in KRaft mode (without Zookeeper)
        await _kafkaContainer.StartAsync();

        var cmd = $"/usr/bin/kafka-configs --bootstrap-server localhost:{BrokerPort} --alter --add-config 'SCRAM-SHA-256=[password={Scram256Pass}]' --entity-type users --entity-name {ScramUser}";

        var result = await _kafkaContainer.ExecAsync(new[]
                {
          "bash","-lc", cmd
        });

        var cmd2 = $"/usr/bin/kafka-configs --bootstrap-server localhost:{BrokerPort} --alter --add-config 'SCRAM-SHA-512=[password={Scram512Pass}]' --entity-type users --entity-name {ScramUser}";

        var result2 = await _kafkaContainer.ExecAsync(new[]
                {
          "bash","-lc", cmd2
        });

        if (result.ExitCode != 0 || result2.ExitCode != 0)
        {
            throw new Exception($"Failed to create SCRAM user. Exit={result.ExitCode}\nSTDOUT:\n{result.Stdout}\nSTDERR:\n{result.Stderr}");
        }
        _bootstrapServers = $"127.0.0.1:{hostPlaintext}";
    }

    public async Task DisposeAsync()
    {
        if (_kafkaContainer != null)
        {
            await _kafkaContainer.DisposeAsync();
        }
    }
}
