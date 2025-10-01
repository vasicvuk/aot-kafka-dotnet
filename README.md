# AOT Kafka .NET

[![.NET](https://img.shields.io/badge/.NET-8.0-purple)](https://dotnet.microsoft.com/)
[![AOT Compatible](https://img.shields.io/badge/AOT-Compatible-green)](https://learn.microsoft.com/en-us/dotnet/core/deploying/native-aot/)

A high-performance, AOT-compatible .NET Kafka client built on top of librdkafka. Provides a familiar API similar to confluent-kafka-dotnet while supporting Native AOT compilation for faster startup times and reduced memory footprint.

## Features

- ✅ **Native AOT Compatible**: Full support for .NET Native AOT compilation
- ✅ **Producer & Consumer**: Complete implementation for producing and consuming messages
- ✅ **Admin Client**: Create and manage topics programmatically
- ✅ **Consumer Groups**: Coordinated consumption with automatic rebalancing
- ✅ **SASL Authentication**: Support for PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512
- ✅ **Compression**: Support for Gzip, Snappy, LZ4, and Zstd compression
- ✅ **Thread-Safe**: Producer supports concurrent operations from multiple threads
- ✅ **Cross-Platform**: Windows x64, Linux x64, and macOS ARM64 support
- ✅ **Familiar API**: Similar interface to confluent-kafka-dotnet for easy migration

## Installation

```bash
dotnet add package AotKafka
```

## Quick Start

### Producer Example

```csharp
using AotKafka;

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092"
};

using var producer = new Producer<string, string>(config);

var message = new Message<string, string>
{
    Key = "my-key",
    Value = "Hello, Kafka!"
};

var result = await producer.ProduceAsync("my-topic", message);
Console.WriteLine($"Message delivered to {result.TopicPartitionOffset}");
```

### Consumer Example

```csharp
using AotKafka;

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "my-consumer-group",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

using var consumer = new Consumer<string, string>(config);
consumer.Subscribe("my-topic");

var cts = new CancellationTokenSource();

while (!cts.Token.IsCancellationRequested)
{
    var result = consumer.Consume(cts.Token);
    Console.WriteLine($"Received: {result.Message.Value}");
}

consumer.Close();
```

### Admin Client Example

```csharp
using AotKafka;

var config = new AdminConfig
{
    BootstrapServers = "localhost:9092"
};

using var admin = new AdminClient(config);

await admin.CreateTopicAsync(
    topicName: "my-topic",
    numPartitions: 3,
    replicationFactor: 1
);
```

### AOT Compilation

To compile your application with Native AOT:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net8.0</TargetFramework>
    <PublishAot>true</PublishAot>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="AotKafka" Version="1.0.0" />
  </ItemGroup>
</Project>
```

```bash
dotnet publish -r win-x64 -c Release
# or
dotnet publish -r linux-x64 -c Release
```

## Configuration

### Producer Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `BootstrapServers` | Comma-separated list of broker addresses | Required |
| `ClientId` | Client identifier | null |
| `SecurityProtocol` | Security protocol (Plaintext, SaslPlaintext, SaslSsl, Ssl) | Plaintext |
| `SaslMechanism` | SASL mechanism (Plain, ScramSha256, ScramSha512) | Plain |
| `SaslUsername` | SASL username | null |
| `SaslPassword` | SASL password | null |
| `CompressionType` | Message compression (None, Gzip, Snappy, Lz4, Zstd) | None |
| `MessageTimeoutMs` | Message delivery timeout | 300000 |
| `Acks` | Acknowledgment level (None, Leader, All) | All |
| `EnableIdempotence` | Enable idempotent producer | false |
| `MaxInFlight` | Max in-flight requests per connection | 5 |
| `LingerMs` | Batch accumulation delay | 0 |
| `BatchSize` | Max batch size in bytes | 16384 |

### Consumer Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `BootstrapServers` | Comma-separated list of broker addresses | Required |
| `GroupId` | Consumer group identifier | Required |
| `ClientId` | Client identifier | null |
| `SecurityProtocol` | Security protocol (Plaintext, SaslPlaintext, SaslSsl, Ssl) | Plaintext |
| `SaslMechanism` | SASL mechanism (Plain, ScramSha256, ScramSha512) | Plain |
| `SaslUsername` | SASL username | null |
| `SaslPassword` | SASL password | null |
| `AutoOffsetReset` | Offset reset behavior (Earliest, Latest, Error) | Latest |
| `EnableAutoCommit` | Enable automatic offset commits | true |
| `AutoCommitIntervalMs` | Auto-commit interval | 5000 |
| `SessionTimeoutMs` | Session timeout | 10000 |
| `MaxPollIntervalMs` | Max poll interval | 300000 |
| `EnablePartitionEof` | Emit partition EOF events | false |

### Admin Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `BootstrapServers` | Comma-separated list of broker addresses | Required |
| `SecurityProtocol` | Security protocol (Plaintext, SaslPlaintext, SaslSsl, Ssl) | Plaintext |
| `SaslMechanism` | SASL mechanism (Plain, ScramSha256, ScramSha512) | Plain |
| `SaslUsername` | SASL username | null |
| `SaslPassword` | SASL password | null |

## Acknowledgments

This project was inspired by [confluent-kafka-dotnet](https://github.com/confluentinc/confluent-kafka-dotnet) and specifically [issue #2146](https://github.com/confluentinc/confluent-kafka-dotnet/issues/2146) discussing AOT compatibility. We are grateful to the Confluent team for their excellent work on the original library.

## Architecture

The library follows a three-layer architecture for safety and maintainability:

1. **AotKafka.Native**: P/Invoke declarations using `LibraryImport` for AOT compatibility
2. **AotKafka.Core**: SafeHandle wrappers for memory safety and resource management
3. **AotKafka**: Public API surface with familiar types and patterns

## Security

AotKafka supports SASL authentication for secure broker connections:

### SASL/PLAIN

```csharp
var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    SecurityProtocol = SecurityProtocol.SaslPlaintext,
    SaslMechanism = SaslMechanism.Plain,
    SaslUsername = "your-username",
    SaslPassword = "your-password"
};
```

### SASL/SCRAM-SHA-256

```csharp
var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "my-group",
    SecurityProtocol = SecurityProtocol.SaslPlaintext,
    SaslMechanism = SaslMechanism.ScramSha256,
    SaslUsername = "your-username",
    SaslPassword = "your-password"
};
```

### SASL/SCRAM-SHA-512

```csharp
var config = new AdminConfig
{
    BootstrapServers = "localhost:9092",
    SecurityProtocol = SecurityProtocol.SaslPlaintext,
    SaslMechanism = SaslMechanism.ScramSha512,
    SaslUsername = "your-username",
    SaslPassword = "your-password"
};
```

## Platform Support

- **Windows x64**: ✅ Full support with bundled native libraries
- **Linux x64**: ✅ Full support with bundled native libraries
- **macOS ARM64**: ⚠️ Best-effort support (may require manual librdkafka installation)

## Requirements

- .NET 8.0 or later
- librdkafka native library (automatically included via NuGet)

## Samples

Check out the [AOT Sample](samples/AotKafka.AotSample) for a complete working example demonstrating:

- AdminClient topic creation
- Producer with compression
- Consumer with consumer groups
- Error handling and troubleshooting
- Native AOT compilation
- Docker Compose setup with Redpanda

```bash
cd samples/AotKafka.AotSample
docker-compose up -d
dotnet run
```

## Testing

The library includes comprehensive test coverage.

Run all tests:
```bash
dotnet test
```

Run specific test suites:
```bash
dotnet test tests/AotKafka.Tests                    # Contract tests
dotnet test tests/AotKafka.IntegrationTests         # Integration tests
```

## Contributing

Contributions are welcome! Please ensure:

1. All tests pass
2. Code follows .editorconfig standards
3. AOT compatibility is maintained
4. Test coverage remains above 90%

## License

This project is licensed under the MIT License.

## Resources

- [Documentation](docs/GETTING_STARTED.md)
- [AOT Compatibility Guide](docs/AOT_COMPATIBILITY.md)
- [Confluent Kafka Documentation](https://docs.confluent.io/platform/current/clients/index.html)
- [librdkafka Documentation](https://github.com/confluentinc/librdkafka)