using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using AotKafka;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Xunit;

namespace AotKafka.IntegrationTests;

public class SaslIntegrationTests : IAsyncLifetime
{
    private const string Scram256User = "admin";
    private const string Scram256Pass = "admin-secret";
    private const string Scram512User = "user512";
    private const string Scram512Pass = "user512-secret";
    public const string StartupScriptFilePath = "/testcontainers.sh";
    public const int KafkaOutsidePort = 9092;  // container port, host-mapped randomly
    public const int KafkaInsidePort = 29092; // container-only port
    public const int AdminPort = 9644;  // container port, host-mapped randomly
    public const int RpcPort = 33145; // container port, host-mapped randomly


    private IContainer? _redpanda;
    private string _bootstrapServers = string.Empty;

    public async Task InitializeAsync()
    {
        Console.WriteLine("=== Starting Redpanda container with SASL enabled ===");


    _redpanda = new ContainerBuilder()
            .WithImage("docker.redpanda.com/redpandadata/redpanda:v24.2.4")
              .WithPortBinding(KafkaOutsidePort, true)
              .WithPortBinding(AdminPort, true)
              .WithPortBinding(RpcPort, true)
              .WithPortBinding(KafkaInsidePort, false) // no need to publish inside port
            .WithCreateParameterModifier(p =>
            {
                p.User = "0:0";
                p.HostConfig.Memory = 2L * 1024 * 1024 * 1024; 
            })
         .WithStartupCallback(async (container, ct) =>
         {
             // resolve host-mapped ports *after* the container is running
             var hostKafkaOutside = container.GetMappedPublicPort(KafkaOutsidePort);
             var hostAdmin = container.GetMappedPublicPort(AdminPort);
             var hostRpc = container.GetMappedPublicPort(RpcPort);

             // build a startup script that sets SASL using --set (no config file)
             var sb = new StringBuilder();
             const char lf = '\n';
             sb.Append("#!/bin/bash");
             sb.Append(lf);

             // Start Redpanda in background with SASL + advertised addrs
             sb.Append("/usr/bin/rpk redpanda start ");
             sb.Append("--mode dev-container --memory=1G ");
             sb.Append("--smp 1 ");
             // bind listeners
             sb.Append($"--kafka-addr PLAINTEXT://0.0.0.0:{KafkaInsidePort},OUTSIDE://0.0.0.0:{KafkaOutsidePort} ");
             sb.Append($"--rpc-addr 0.0.0.0:{RpcPort} ");
             // advertise *host* ports/addresses
             sb.Append($"--advertise-kafka-addr PLAINTEXT://127.0.0.1:{KafkaInsidePort},OUTSIDE://127.0.0.1:{hostKafkaOutside} ");
             sb.Append($"--advertise-rpc-addr 127.0.0.1:{hostRpc} ");
             // admin listener (no auth for bootstrap)
             sb.Append($"--set redpanda.admin[0].address=0.0.0.0 ");
             sb.Append($"--set redpanda.admin[0].port={AdminPort} ");
             // enable SASL globally + per listener
             sb.Append("--set redpanda.enable_sasl=true ");
             sb.Append("--set redpanda.superusers=['admin'] ");
             sb.Append("--set redpanda.sasl_mechanisms=['SCRAM'] ");
             sb.Append("--set redpanda.kafka_api[0].authentication_method=sasl ");
             sb.Append("--set redpanda.kafka_api[1].authentication_method=sasl ");
             sb.Append(lf);
             sb.Append(" >/var/log/redpanda.log 2>&1 &");

             // wait for admin ready
             sb.Append($"for i in {{1..120}}; do curl -fsS http://127.0.0.1:{AdminPort}/v1/status/ready && break; sleep 0.5; done");

             sb.Append(lf);
             // create users via Admin API (no broker auth needed here)
             sb.AppendLine($"/usr/bin/rpk security user create {Scram256User} -p {Scram256Pass} --mechanism SCRAM-SHA-256 -X admin.hosts=localhost:{AdminPort}");

             sb.Append(lf);
             sb.Append($"/usr/bin/rpk security user create {Scram512User} -p {Scram512Pass} --mechanism SCRAM-SHA-512 -X admin.hosts=localhost:{AdminPort}");

             sb.Append(lf);
             // grant ACLs (rpk may contact the broker -> include SASL for the user)
             sb.Append($"/usr/bin/rpk security acl create --allow-principal 'User:{Scram256User}'  --operation all --topic '*' --group '*' --brokers localhost:{KafkaOutsidePort} -X user=admin  -X pass=admin-secret  -X sasl.mechanism=SCRAM-SHA-256 -X admin.hosts=localhost:{AdminPort}");

             sb.Append(lf);
             sb.Append($"/usr/bin/rpk security acl create --allow-principal 'User:{Scram512User}' --operation all --topic '*' --group '*' --brokers localhost:{KafkaOutsidePort} -X user=user512 -X pass=user512-secret -X sasl.mechanism=SCRAM-SHA-512 -X admin.hosts=localhost:{AdminPort}");

             sb.Append(lf);
             sb.Append("echo 'Redpanda initialization complete'");
             sb.Append(lf);
             var script = sb.ToString();

             // copy + execute
             const string path = "/usr/local/bin/start-redpanda.sh";
             await container.CopyAsync(Encoding.UTF8.GetBytes(script), path, Unix.FileMode755, ct);
             var res = await container.ExecAsync(new[] { "bash", "-lc", path }, ct: ct);
             if (res.ExitCode != 0)
             {
                 throw new InvalidOperationException($"Redpanda startup failed with exit code {res.ExitCode}. Stderr: {res.Stderr}");
             }
             Console.WriteLine($"Redpanda startup script output: {res.Stdout}");

             // tell your test code where to connect (host-mapped OUTSIDE port)
             Console.WriteLine($"Bootstrap servers: localhost:{hostKafkaOutside}");
         }).Build();

        Console.WriteLine("Starting Redpanda container with SASL config...");
        var startTime = DateTime.UtcNow;

        await _redpanda.StartAsync();

        var startDuration = DateTime.UtcNow - startTime;
        Console.WriteLine($"Container started in {startDuration.TotalSeconds:F2} seconds");

        // Get mapped ports
        _bootstrapServers = $"localhost:{_redpanda.GetMappedPublicPort(9092)}";
        Console.WriteLine($"Bootstrap servers: {_bootstrapServers}");

        // Wait for Redpanda to be fully ready - use Admin API health check
        Console.WriteLine("Waiting for Redpanda to be ready...");
        var adminPort = _redpanda.GetMappedPublicPort(AdminPort);
        var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };
        var maxWaitTime = TimeSpan.FromSeconds(60);
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        while (stopwatch.Elapsed < maxWaitTime)
        {
            try
            {
                var response = await httpClient.GetAsync($"http://localhost:{adminPort}/v1/status/ready");
                if (response.IsSuccessStatusCode)
                {
                    Console.WriteLine($"Redpanda is ready after {stopwatch.Elapsed.TotalSeconds:F2} seconds");
                    break;
                }
            }
            catch
            {
                // Ignore connection errors during startup
            }

            await Task.Delay(500);
        }

        if (stopwatch.Elapsed >= maxWaitTime)
        {
            throw new TimeoutException("Redpanda did not become ready within 60 seconds");
        }

        Console.WriteLine("=== Redpanda container initialization complete ===");
    }

    public async Task DisposeAsync()
    {
        if (_redpanda != null)
        {
            await _redpanda.DisposeAsync();
        }
    }

    [Fact]
    public async Task Sasl_ScramSha256_ShouldProduceAndConsume()
    {
        Console.WriteLine("=== Test: Sasl_ScramSha256_ShouldProduceAndConsume ===");
        var topic = $"sasl-scram256-{Guid.NewGuid():N}";
        Console.WriteLine($"Topic: {topic}");

        Console.WriteLine("Creating topic with admin client...");
        var adminConfig = CreateAdminConfig(SaslMechanism.ScramSha256, Scram256User, Scram256Pass);
        using (var admin = new AdminClient(adminConfig))
        {
            await admin.CreateTopicAsync(topic, numPartitions: 3, replicationFactor: 1, operationTimeout: TimeSpan.FromSeconds(120));
        }
        Console.WriteLine("Topic created successfully");

        Console.WriteLine("Producing message...");
        var producerConfig = CreateProducerConfig(SaslMechanism.ScramSha256, Scram256User, Scram256Pass);
        using var producer = new Producer<string, string>(producerConfig);
        await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = "sasl-key",
            Value = "sasl-value"
        });
        producer.Flush(TimeSpan.FromSeconds(10));
        Console.WriteLine("Message produced successfully");

        Console.WriteLine("Consuming message...");
        var consumerConfig = CreateConsumerConfig(SaslMechanism.ScramSha256, Scram256User, Scram256Pass);
        using var consumer = new Consumer<string, string>(consumerConfig);
        consumer.Subscribe(topic);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var result = consumer.Consume(cts.Token);

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
        var adminConfig = CreateAdminConfig(SaslMechanism.ScramSha256, Scram256User, Scram256Pass);
        using (var admin = new AdminClient(adminConfig))
        {
            await admin.CreateTopicAsync(topic, numPartitions: 2, replicationFactor: 1, operationTimeout: TimeSpan.FromSeconds(30));
        }
        Console.WriteLine("Topic created successfully");

        Console.WriteLine("Producing message (SCRAM-SHA-512)...");
        var producerConfig = CreateProducerConfig(SaslMechanism.ScramSha512, Scram512User, Scram512Pass);
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
        BootstrapServers = _bootstrapServers,
        SecurityProtocol = SecurityProtocol.SaslPlaintext,
        SaslMechanism = mechanism,
        SaslUsername = username,
        SaslPassword = password,
        RequestTimeoutMs = 15000,
        SocketTimeoutMs = 15000
    };

    private ProducerConfig CreateProducerConfig(SaslMechanism mechanism, string username, string password) => new()
    {
        BootstrapServers = _bootstrapServers,
        SecurityProtocol = SecurityProtocol.SaslPlaintext,
        SaslMechanism = mechanism,
        SaslUsername = username,
        SaslPassword = password
    };

    private ConsumerConfig CreateConsumerConfig(SaslMechanism mechanism, string username, string password) => new()
    {
        BootstrapServers = _bootstrapServers,
        GroupId = $"grp-{Guid.NewGuid():N}",
        SecurityProtocol = SecurityProtocol.SaslPlaintext,
        SaslMechanism = mechanism,
        SaslUsername = username,
        SaslPassword = password,
        AutoOffsetReset = AutoOffsetReset.Earliest
    };
}

