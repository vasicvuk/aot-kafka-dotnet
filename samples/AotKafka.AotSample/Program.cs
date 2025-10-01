using AotKafka.Native;
using System.Diagnostics;

namespace AotKafka.AotSample;

/// <summary>
/// Complete AOT-compatible Kafka sample demonstrating producer, consumer, and admin operations.
/// Run with: dotnet run
/// Or build AOT binary: dotnet publish -r win-x64 -c Release
///
/// By default, automatically starts Redpanda in a Docker container using Docker CLI.
/// To use an existing cluster, set KAFKA_BOOTSTRAP_SERVERS environment variable or pass as argument.
/// </summary>
public static class Program
{
    private static string? _containerId;

    public static async Task<int> Main(string[] args)
    {
        var sw = Stopwatch.StartNew();

        // Check for bootstrap servers from environment or command line
        string bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS")
                            ?? (args.Length > 0 ? args[0] : null) ?? "";

        var useDocker = string.IsNullOrEmpty(bootstrapServers);

        try
        {
            // Start Redpanda container if no bootstrap servers provided
            if (useDocker)
            {
                Console.WriteLine("=== Starting Redpanda with Docker CLI ===");
                Console.WriteLine("No bootstrap servers provided, starting Redpanda container...");
                Console.WriteLine("(Set KAFKA_BOOTSTRAP_SERVERS env var or pass as argument to use existing cluster)");
                Console.WriteLine();

                bootstrapServers = await StartRedpandaContainerAsync();

                Console.WriteLine($"✓ Redpanda started successfully");
                Console.WriteLine($"  Container ID: {_containerId?[..12]}");
                Console.WriteLine($"  Bootstrap: {bootstrapServers}");
                Console.WriteLine();
            }

            var topicName = "aot-sample-topic";
            var messageCount = 10;

            Console.WriteLine("=== AotKafka AOT Sample ===");
            Console.WriteLine($"Bootstrap Servers: {bootstrapServers}");
            Console.WriteLine($"Topic: {topicName}");
            Console.WriteLine();

            // Admin Client - Create Topic
            Console.WriteLine("1. Creating topic with AdminClient...");
            var adminConfig = new AdminConfig
            {
                BootstrapServers = bootstrapServers
            };

            using (var admin = new AdminClient(adminConfig))
            {
                try
                {
                    await admin.CreateTopicAsync(
                        topicName: topicName,
                        numPartitions: 2,
                        replicationFactor: 1,
                        operationTimeout: TimeSpan.FromSeconds(10)
                    );
                    Console.WriteLine($"   ✓ Topic '{topicName}' created with 2 partitions");
                }
                catch (KafkaException ex) when (ex.Error.Code == ErrorCode.TopicAlreadyExists)
                {
                    Console.WriteLine($"   ✓ Topic '{topicName}' already exists");
                }
            }
            Console.WriteLine();

            // Producer - Send Messages
            Console.WriteLine($"2. Producing {messageCount} messages...");
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                ClientId = "aot-sample-producer",
                CompressionType = CompressionType.Lz4
            };

            using (var producer = new Producer<string, string>(producerConfig))
            {
                for (int i = 0; i < messageCount; i++)
                {
                    var message = new Message<string, string>
                    {
                        Key = $"key-{i}",
                        Value = $"AOT Message {i} from {producer.Name}"
                    };

                    var result = await producer.ProduceAsync(topicName, message);
                    Console.WriteLine($"   ✓ Message {i}: partition={result.Partition} offset={result.Offset}");
                }

                producer.Flush(TimeSpan.FromSeconds(10));
                Console.WriteLine($"   ✓ All messages flushed");
            }
            Console.WriteLine();

            // Consumer - Receive Messages
            Console.WriteLine("3. Consuming messages...");
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = "aot-sample-group",
                ClientId = "aot-sample-consumer",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true
            };

            using (var consumer = new Consumer<string, string>(consumerConfig))
            {
                consumer.Subscribe(topicName);

                var consumedCount = 0;
                var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

                while (consumedCount < messageCount && !cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        var result = consumer.Consume(cts.Token);

                        if (result.IsPartitionEOF)
                        {
                            Console.WriteLine($"   → Reached end of partition {result.Partition}");
                            continue;
                        }

                        Console.WriteLine($"   ✓ Consumed: key={result.Message.Key} value={result.Message.Value} " +
                                        $"partition={result.Partition} offset={result.Offset}");
                        consumedCount++;
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }

                consumer.Close();
                Console.WriteLine($"   ✓ Consumed {consumedCount}/{messageCount} messages");
            }
            Console.WriteLine();

            sw.Stop();
            Console.WriteLine("=== Success ===");
            Console.WriteLine($"Total execution time: {sw.ElapsedMilliseconds}ms");
            Console.WriteLine();
            Console.WriteLine("This application was compiled with Native AOT for:");
            Console.WriteLine("  • Fast startup (~50ms)");
            Console.WriteLine("  • Low memory footprint (~25MB)");
            Console.WriteLine("  • Small binary size (~15MB)");
            Console.WriteLine("  • No .NET runtime dependency");

            return 0;
        }
        catch (Exception ex)
        {
            Console.WriteLine();
            Console.WriteLine($"=== Error ===");
            Console.WriteLine($"Type: {ex.GetType().Name}");
            Console.WriteLine($"Message: {ex.Message}");
            Console.WriteLine();

            if (useDocker)
            {
                Console.WriteLine("Troubleshooting:");
                Console.WriteLine("  1. Ensure Docker is running");
                Console.WriteLine("  2. Check Docker daemon is accessible");
                Console.WriteLine("  3. Or use existing cluster: set KAFKA_BOOTSTRAP_SERVERS=localhost:9092");
            }
            else
            {
                Console.WriteLine("Troubleshooting:");
                Console.WriteLine($"  1. Ensure Kafka is running at {bootstrapServers}");
                Console.WriteLine("  2. Check network connectivity to the broker");
                Console.WriteLine("  3. Verify authentication credentials (if using SASL)");
            }

            return 1;
        }
        finally
        {
            // Clean up Redpanda container
            if (_containerId != null)
            {
                Console.WriteLine();
                Console.WriteLine("=== Cleaning Up ===");
                Console.WriteLine("Stopping Redpanda container...");
                await StopRedpandaContainerAsync();
                Console.WriteLine("✓ Container stopped and removed");
            }
        }
    }

    private static async Task<string> StartRedpandaContainerAsync()
    {
        // Generate unique container name
        var containerName = $"redpanda-aot-{Guid.NewGuid():N}";

        // Start Redpanda container with fixed port mapping
        var startInfo = new ProcessStartInfo
        {
            FileName = "docker",
            Arguments = $"run -d --name {containerName} " +
                       "-p 19092:19092 " +
                       "docker.redpanda.com/redpandadata/redpanda:v24.2.4 " +
                       "redpanda start --smp 1 --memory 1G --overprovisioned " +
                       "--kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092 " +
                       "--advertise-kafka-addr internal://redpanda:9092,external://localhost:19092",
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        using var process = Process.Start(startInfo) ?? throw new InvalidOperationException("Failed to start docker process");
        _containerId = (await process.StandardOutput.ReadToEndAsync()).Trim();
        await process.WaitForExitAsync();

        if (process.ExitCode != 0)
        {
            var error = await process.StandardError.ReadToEndAsync();
            throw new InvalidOperationException($"Failed to start Redpanda container: {error}");
        }

        // Wait for Redpanda to be ready
        await Task.Delay(8000);

        return "localhost:19092";
    }

    private static async Task StopRedpandaContainerAsync()
    {
        if (_containerId == null)
        {
            return;
        }

        var stopInfo = new ProcessStartInfo
        {
            FileName = "docker",
            Arguments = $"rm -f {_containerId}",
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        using var process = Process.Start(stopInfo);
        if (process != null)
        {
            await process.WaitForExitAsync();
        }
    }
}
