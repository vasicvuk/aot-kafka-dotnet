# AotKafka AOT Sample

This sample demonstrates a complete Native AOT-compiled Kafka application using AotKafka.

## Features Demonstrated

- ✅ **AdminClient**: Creating topics programmatically
- ✅ **Producer**: Sending messages with compression
- ✅ **Consumer**: Consuming messages with consumer groups
- ✅ **Native AOT**: Full AOT compilation for fast startup and small binary size
- ✅ **Error Handling**: Graceful error handling and troubleshooting
- ✅ **Docker Integration**: Easy setup with Redpanda

## Quick Start

### Option 1: Automatic (Recommended - Uses Testcontainers)

The sample automatically starts Redpanda in a Docker container if no bootstrap servers are provided:

```bash
# Just run - Redpanda will start automatically!
dotnet run
```

This will:
1. Automatically start a Redpanda container using Testcontainers
2. Run the complete sample (create topic, produce, consume)
3. Clean up and remove the container when done

**Requirements**: Docker must be running

### Option 2: Use Existing Kafka Cluster

**Via Environment Variable:**
```bash
# Windows PowerShell
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
dotnet run

# Linux/macOS
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
dotnet run
```

**Via Command Line Argument:**
```bash
dotnet run localhost:9092
```

### Option 3: Manual Docker Compose

If you prefer to manage the container lifecycle yourself:

```bash
docker-compose up -d
dotnet run localhost:19092
docker-compose down
```

This starts:
- **Redpanda** (Kafka-compatible broker) on `localhost:19092`
- **Redpanda Console** (Web UI) on `http://localhost:8080`

### AOT Compilation

**Compile to Native Binary:**

Windows:
```powershell
dotnet publish -r win-x64 -c Release
.\bin\Release\net8.0\win-x64\publish\AotKafka.AotSample.exe
```

Linux:
```bash
dotnet publish -r linux-x64 -c Release
./bin/Release/net8.0/linux-x64/publish/AotKafka.AotSample
```

Note: Testcontainers won't work in AOT-compiled binaries. Use environment variable or argument to specify bootstrap servers when running AOT binaries.

## Expected Output

When you run `dotnet run`, you'll see:

```
=== Starting Redpanda with Testcontainers ===
No bootstrap servers provided, starting Redpanda container...
(Set KAFKA_BOOTSTRAP_SERVERS env var or pass as argument to use existing cluster)

✓ Redpanda started successfully
  Container ID: a1b2c3d4e5f6
  Bootstrap: localhost:xxxxx

=== AotKafka AOT Sample ===
Bootstrap Servers: localhost:xxxxx
Topic: aot-sample-topic
...
```

The sample will:
1. Create a topic named `aot-sample-topic` with 2 partitions
2. Produce 10 messages with LZ4 compression
3. Consume the 10 messages
4. Display execution metrics

```
=== Success ===
Bootstrap Servers: localhost:19092
Topic: aot-sample-topic

1. Creating topic with AdminClient...
   ✓ Topic 'aot-sample-topic' created with 2 partitions

2. Producing 10 messages...
   ✓ Message 0: partition=0 offset=0
   ✓ Message 1: partition=1 offset=0
   ...
   ✓ All messages flushed

3. Consuming messages...
   ✓ Consumed: key=key-0 value=AOT Message 0 from ... partition=0 offset=0
   ...
   ✓ Consumed 10/10 messages

=== Success ===
Total execution time: 1234ms

This application was compiled with Native AOT for:
  • Fast startup (~50ms)
  • Low memory footprint (~25MB)
  • Small binary size (~15MB)
  • No .NET runtime dependency
```

After completion, the container is automatically cleaned up.

## Explore Redpanda Console (Docker Compose Only)

If using Docker Compose (not Testcontainers), open http://localhost:8080 in your browser to:
- View topics and partitions
- Browse messages
- Monitor consumer groups
- Check broker health

## What This Sample Shows

### AdminClient Usage

```csharp
var adminConfig = new AdminConfig
{
    BootstrapServers = "localhost:19092"
};

using var admin = new AdminClient(adminConfig);

await admin.CreateTopicAsync(
    topicName: "aot-sample-topic",
    numPartitions: 2,
    replicationFactor: 1,
    operationTimeout: TimeSpan.FromSeconds(10)
);
```

### Producer with Compression

```csharp
var producerConfig = new ProducerConfig
{
    BootstrapServers = "localhost:19092",
    ClientId = "aot-sample-producer",
    CompressionType = CompressionType.Lz4
};

using var producer = new Producer<string, string>(producerConfig);

var message = new Message<string, string>
{
    Key = "key-0",
    Value = "Hello, AOT Kafka!"
};

var result = await producer.ProduceAsync("aot-sample-topic", message);
```

### Consumer with Auto-Commit

```csharp
var consumerConfig = new ConsumerConfig
{
    BootstrapServers = "localhost:19092",
    GroupId = "aot-sample-group",
    ClientId = "aot-sample-consumer",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true
};

using var consumer = new Consumer<string, string>(consumerConfig);
consumer.Subscribe("aot-sample-topic");

var result = consumer.Consume(cancellationToken);
Console.WriteLine($"Received: {result.Message.Value}");
```

## AOT Compilation Details

### Project Configuration

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
  <InvariantGlobalization>true</InvariantGlobalization>
</PropertyGroup>
```

### Benefits of AOT Compilation

| Metric | JIT (dotnet run) | AOT (published) |
|--------|-----------------|-----------------|
| Startup Time | ~500ms | ~50ms |
| Binary Size | ~85MB | ~15MB |
| Memory (idle) | ~80MB | ~25MB |
| .NET Runtime | Required | Not required |

### Platform-Specific Builds

**Windows x64:**
```powershell
dotnet publish -r win-x64 -c Release
# Output: bin\Release\net8.0\win-x64\publish\AotKafka.AotSample.exe
```

**Linux x64:**
```bash
dotnet publish -r linux-x64 -c Release
# Output: bin/Release/net8.0/linux-x64/publish/AotKafka.AotSample
```

**macOS ARM64:**
```bash
dotnet publish -r osx-arm64 -c Release
# Output: bin/Release/net8.0/osx-arm64/publish/AotKafka.AotSample
```

## Troubleshooting


## Next Steps

- Modify the sample to use SASL authentication
- Experiment with different compression types
- Try manual offset commits
- Implement custom serializers
- Deploy the AOT binary to a container

## Related Documentation

- [Getting Started Guide](../../docs/GETTING_STARTED.md)
- [AOT Compatibility Guide](../../docs/AOT_COMPATIBILITY.md)
- [Redpanda Documentation](https://docs.redpanda.com/)
