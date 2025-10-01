# Getting Started with AotKafka

This guide will help you get started with AotKafka, a high-performance, AOT-compatible .NET Kafka client.

## Table of Contents

- [Installation](#installation)
- [Basic Producer](#basic-producer)
- [Basic Consumer](#basic-consumer)
- [Admin Client](#admin-client)
- [Security Configuration](#security-configuration)
- [Advanced Features](#advanced-features)
- [Error Handling](#error-handling)
- [Best Practices](#best-practices)

## Installation

Add the AotKafka package to your project:

```bash
dotnet add package AotKafka
```

The package automatically includes the required librdkafka native libraries for Windows and Linux.

## Basic Producer

The producer sends messages to Kafka topics:

```csharp
using AotKafka;

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    ClientId = "my-producer"
};

using var producer = new Producer<string, string>(config);

var message = new Message<string, string>
{
    Key = "user-123",
    Value = "Hello, Kafka!"
};

try
{
    var result = await producer.ProduceAsync("my-topic", message);
    Console.WriteLine($"Message delivered to partition {result.Partition} at offset {result.Offset}");
}
catch (ProduceException ex)
{
    Console.WriteLine($"Failed to deliver message: {ex.Error.Reason}");
}
```

### Producer with Compression

```csharp
var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    CompressionType = CompressionType.Lz4,
    LingerMs = 10,      // Wait up to 10ms to batch messages
    BatchSize = 32768   // 32KB batch size
};

using var producer = new Producer<string, string>(config);
```

### Synchronous Produce

For scenarios where you need to wait for delivery:

```csharp
var message = new Message<string, string> { Key = "key", Value = "value" };
await producer.ProduceAsync("my-topic", message);
producer.Flush(TimeSpan.FromSeconds(10));
```

## Basic Consumer

The consumer reads messages from Kafka topics:

```csharp
using AotKafka;

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "my-consumer-group",
    ClientId = "my-consumer",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true
};

using var consumer = new Consumer<string, string>(config);
consumer.Subscribe("my-topic");

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

try
{
    while (!cts.Token.IsCancellationRequested)
    {
        var result = consumer.Consume(cts.Token);

        if (result.IsPartitionEOF)
        {
            Console.WriteLine($"Reached end of partition {result.Partition}");
            continue;
        }

        Console.WriteLine($"Received: Key={result.Message.Key}, Value={result.Message.Value}");

        // Optionally commit manually
        // consumer.Commit(result);
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("Consumer cancelled");
}
finally
{
    consumer.Close();
}
```

### Manual Offset Commits

For at-least-once delivery guarantees:

```csharp
var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "my-group",
    EnableAutoCommit = false  // Manual commit
};

using var consumer = new Consumer<string, string>(config);
consumer.Subscribe("my-topic");

while (true)
{
    var result = consumer.Consume(cts.Token);

    // Process message
    ProcessMessage(result.Message);

    // Commit after successful processing
    consumer.Commit(result);
}
```

### Subscribe to Multiple Topics

```csharp
consumer.Subscribe(new[] { "topic1", "topic2", "topic3" });
```

## Admin Client

The admin client provides topic management operations:

```csharp
using AotKafka;

var config = new AdminConfig
{
    BootstrapServers = "localhost:9092"
};

using var admin = new AdminClient(config);

// Create a topic with 3 partitions and replication factor 1
await admin.CreateTopicAsync(
    topicName: "my-new-topic",
    numPartitions: 3,
    replicationFactor: 1,
    operationTimeout: TimeSpan.FromSeconds(30)
);

Console.WriteLine("Topic created successfully");
```

### Create Topic with Custom Configuration

```csharp
var topicConfig = new Dictionary<string, string>
{
    ["retention.ms"] = "86400000",        // 1 day
    ["cleanup.policy"] = "delete",
    ["compression.type"] = "lz4"
};

await admin.CreateTopicAsync(
    topicName: "configured-topic",
    numPartitions: 6,
    replicationFactor: 2,
    operationTimeout: TimeSpan.FromSeconds(30),
    topicConfig: topicConfig
);
```

## Security Configuration

### SASL/PLAIN Authentication

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
    GroupId = "secure-group",
    SecurityProtocol = SecurityProtocol.SaslPlaintext,
    SaslMechanism = SaslMechanism.ScramSha256,
    SaslUsername = "your-username",
    SaslPassword = "your-password"
};
```

### SASL/SCRAM-SHA-512

```csharp
var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    SecurityProtocol = SecurityProtocol.SaslPlaintext,
    SaslMechanism = SaslMechanism.ScramSha512,
    SaslUsername = "your-username",
    SaslPassword = "your-password"
};
```

### SSL/TLS (Future Support)

```csharp
// Coming soon
var config = new ProducerConfig
{
    BootstrapServers = "localhost:9093",
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.ScramSha256,
    SaslUsername = "your-username",
    SaslPassword = "your-password"
};
```

## Advanced Features

### Custom Serializers/Deserializers

Implement custom serialization for complex types:

```csharp
using AotKafka.Serializers;
using AotKafka.Deserializers;
using System.Text.Json;

public class JsonSerializer<T> : ISerializer<T>
{
    public byte[] Serialize(T data, SerializationContext context)
    {
        return JsonSerializer.SerializeToUtf8Bytes(data);
    }
}

public class JsonDeserializer<T> : IDeserializer<T>
{
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull) return default!;
        return JsonSerializer.Deserialize<T>(data)!;
    }
}

// Usage
var producer = new Producer<string, MyCustomType>(
    config,
    keySerializer: null,  // Use default string serializer
    valueSerializer: new JsonSerializer<MyCustomType>()
);
```

### Byte Array Messages

For raw binary data:

```csharp
var producer = new Producer<byte[], byte[]>(config);

var message = new Message<byte[], byte[]>
{
    Key = Encoding.UTF8.GetBytes("key"),
    Value = new byte[] { 0x01, 0x02, 0x03, 0x04 }
};

await producer.ProduceAsync("binary-topic", message);
```

### Consumer Groups with Multiple Consumers

Run multiple consumer instances in the same group for parallel processing:

```csharp
// Consumer 1
var config1 = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "parallel-group",
    ClientId = "consumer-1"
};
var consumer1 = new Consumer<string, string>(config1);

// Consumer 2 (same group)
var config2 = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "parallel-group",
    ClientId = "consumer-2"
};
var consumer2 = new Consumer<string, string>(config2);

// Both subscribe to the same topic - partitions will be distributed
consumer1.Subscribe("my-topic");
consumer2.Subscribe("my-topic");
```

### Idempotent Producer

For exactly-once semantics within a partition:

```csharp
var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    EnableIdempotence = true,
    Acks = Acks.All,
    MaxInFlight = 5
};
```

## Error Handling

### Producer Error Handling

```csharp
try
{
    var result = await producer.ProduceAsync("my-topic", message);
}
catch (ProduceException ex)
{
    Console.WriteLine($"Error code: {ex.Error.Code}");
    Console.WriteLine($"Reason: {ex.Error.Reason}");
    Console.WriteLine($"Is retriable: {ex.Error.IsError}");

    // Retry logic for transient errors
    if (ex.Error.Code == ErrorCode.RequestTimedOut)
    {
        // Retry
    }
}
catch (KafkaException ex)
{
    Console.WriteLine($"Kafka error: {ex.Error.Reason}");
}
```

### Consumer Error Handling

```csharp
try
{
    var result = consumer.Consume(cts.Token);
}
catch (ConsumeException ex)
{
    Console.WriteLine($"Consume error: {ex.Error.Reason}");
}
catch (OperationCanceledException)
{
    Console.WriteLine("Consume cancelled");
}
```

## Best Practices

### 1. Always Dispose Resources

```csharp
using var producer = new Producer<string, string>(config);
using var consumer = new Consumer<string, string>(config);
using var admin = new AdminClient(config);
```

### 2. Close Consumers Gracefully

```csharp
try
{
    // Consume loop
}
finally
{
    consumer.Close();  // Leave consumer group cleanly
}
```

### 3. Handle Partition EOF

```csharp
var config = new ConsumerConfig
{
    // ... other settings
    EnablePartitionEof = true
};

var result = consumer.Consume(cts.Token);
if (result.IsPartitionEOF)
{
    // Reached end of partition
    continue;
}
```

### 4. Use Appropriate Timeouts

```csharp
var config = new ProducerConfig
{
    MessageTimeoutMs = 30000  // 30 seconds
};

var consumerConfig = new ConsumerConfig
{
    SessionTimeoutMs = 10000,      // 10 seconds
    MaxPollIntervalMs = 300000     // 5 minutes
};
```

### 5. Monitor Producer Queue

```csharp
// Flush before shutdown to deliver pending messages
producer.Flush(TimeSpan.FromSeconds(10));
```

### 6. Configure Batching for Throughput

```csharp
var config = new ProducerConfig
{
    LingerMs = 100,      // Wait up to 100ms for batching
    BatchSize = 65536,   // 64KB batches
    CompressionType = CompressionType.Lz4
};
```

### 7. Use CancellationToken

```csharp
var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

while (!cts.Token.IsCancellationRequested)
{
    var result = consumer.Consume(cts.Token);
    // Process...
}
```

## Next Steps

- Read the [AOT Compatibility Guide](AOT_COMPATIBILITY.md) to learn about Native AOT compilation
- Check out the [sample application](../samples/AotKafka.AotSample) for a complete working example
- Explore the [librdkafka configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) for advanced settings
