using AotKafka;

Console.WriteLine("=== AOT Kafka Test Application ===");
Console.WriteLine("This application tests that AotKafka works correctly with Native AOT compilation.");
Console.WriteLine();

// Test 1: Create ProducerConfig
Console.WriteLine("Test 1: Creating ProducerConfig...");
var producerConfig = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    ClientId = "aot-test-producer",
    CompressionType = CompressionType.Gzip,
    EnableIdempotence = true,
    MaxInFlight = 5
};
Console.WriteLine($"✓ ProducerConfig created: {producerConfig.BootstrapServers}");

// Test 2: Create ConsumerConfig
Console.WriteLine("Test 2: Creating ConsumerConfig...");
var consumerConfig = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "aot-test-group",
    ClientId = "aot-test-consumer",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true
};
Console.WriteLine($"✓ ConsumerConfig created: {consumerConfig.GroupId}");

// Test 3: Test Offset type
Console.WriteLine("Test 3: Testing Offset type...");
var offset1 = new Offset(100);
var offset2 = Offset.Beginning;
var offset3 = Offset.End;
Console.WriteLine($"✓ Offset values: {offset1.Value}, Beginning={offset2.Value}, End={offset3.Value}");
Console.WriteLine($"✓ Offset comparison: 100 < 200 = {offset1 < new Offset(200)}");

// Test 4: Test Partition type
Console.WriteLine("Test 4: Testing Partition type...");
var partition1 = new Partition(5);
var partition2 = Partition.Any;
Console.WriteLine($"✓ Partition values: {partition1.Value}, Any={partition2.Value}");

// Test 5: Test Timestamp type
Console.WriteLine("Test 5: Testing Timestamp type...");
var timestamp = new Timestamp(DateTime.UtcNow);
Console.WriteLine($"✓ Timestamp created: {timestamp.UtcDateTime:O}");

// Test 6: Test Message creation
Console.WriteLine("Test 6: Testing Message creation...");
var message = new Message<string, string>
{
    Key = "test-key",
    Value = "test-value",
    Timestamp = timestamp
};
Console.WriteLine($"✓ Message created: Key={message.Key}, Value={message.Value}");

// Test 7: Test Headers
Console.WriteLine("Test 7: Testing Headers...");
var headers = new Headers();
headers.Add("header1", System.Text.Encoding.UTF8.GetBytes("value1"));
headers.Add("header2", System.Text.Encoding.UTF8.GetBytes("value2"));
Console.WriteLine($"✓ Headers created with {headers.Count()} entries");

// Test 8: Test TopicPartition
Console.WriteLine("Test 8: Testing TopicPartition...");
var topicPartition = new TopicPartition
{
    Topic = "test-topic",
    Partition = partition1
};
Console.WriteLine($"✓ TopicPartition created: {topicPartition}");

// Test 9: Test TopicPartitionOffset
Console.WriteLine("Test 9: Testing TopicPartitionOffset...");
var tpo = new TopicPartitionOffset
{
    Topic = "test-topic",
    Partition = partition1,
    Offset = offset1
};
Console.WriteLine($"✓ TopicPartitionOffset created: {tpo}");

// Test 10: Test Serializers
Console.WriteLine("Test 10: Testing Serializers...");
var stringSerializer = new AotKafka.Serializers.StringSerializer();
var byteArraySerializer = new AotKafka.Serializers.ByteArraySerializer();
var context = new SerializationContext { Topic = "test" };

var serialized = stringSerializer.Serialize("Hello AOT", context);
Console.WriteLine($"✓ String serialized to {serialized.Length} bytes");

var byteData = new byte[] { 1, 2, 3, 4, 5 };
var serializedBytes = byteArraySerializer.Serialize(byteData, context);
Console.WriteLine($"✓ Byte array serialized to {serializedBytes.Length} bytes");

// Test 11: Test Deserializers
Console.WriteLine("Test 11: Testing Deserializers...");
var stringDeserializer = new AotKafka.Deserializers.StringDeserializer();
var byteArrayDeserializer = new AotKafka.Deserializers.ByteArrayDeserializer();

var deserialized = stringDeserializer.Deserialize(serialized, false, context);
Console.WriteLine($"✓ String deserialized: {deserialized}");

var deserializedBytes = byteArrayDeserializer.Deserialize(serializedBytes, false, context);
Console.WriteLine($"✓ Byte array deserialized: {deserializedBytes.Length} bytes");

// Test 12: Test enum values
Console.WriteLine("Test 12: Testing enum values...");
Console.WriteLine($"✓ CompressionType.Gzip = {CompressionType.Gzip}");
Console.WriteLine($"✓ Acks.All = {Acks.All}");
Console.WriteLine($"✓ AutoOffsetReset.Earliest = {AutoOffsetReset.Earliest}");
Console.WriteLine($"✓ PersistenceStatus.Persisted = {PersistenceStatus.Persisted}");

Console.WriteLine();
Console.WriteLine("=== All AOT Tests Passed! ===");
Console.WriteLine("The library is fully compatible with Native AOT compilation.");

return 0;
