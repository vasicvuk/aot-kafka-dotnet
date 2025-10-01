using Xunit;
using AotKafka.Serializers;
using AotKafka.Deserializers;
using System.Text;

namespace AotKafka.Tests;

/// <summary>
/// Contract tests for serialization - verify serializers and deserializers
/// </summary>
public class SerializationContractTests
{
    [Fact]
    public void StringSerializer_SerializeNull_ShouldReturnEmpty()
    {
        // Arrange
        var serializer = new StringSerializer();
        var context = new SerializationContext { Topic = "test" };

        // Act
        var result = serializer.Serialize(null!, context);

        // Assert
        Assert.NotNull(result);
        Assert.Empty(result);
    }

    [Fact]
    public void StringSerializer_SerializeString_ShouldReturnUtf8Bytes()
    {
        // Arrange
        var serializer = new StringSerializer();
        var context = new SerializationContext { Topic = "test" };
        var input = "Hello, Kafka!";

        // Act
        var result = serializer.Serialize(input, context);

        // Assert
        var expected = Encoding.UTF8.GetBytes(input);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void StringDeserializer_DeserializeEmpty_ShouldReturnEmpty()
    {
        // Arrange
        var deserializer = new StringDeserializer();
        var context = new SerializationContext { Topic = "test" };

        // Act
        var result = deserializer.Deserialize(ReadOnlySpan<byte>.Empty, false, context);

        // Assert
        Assert.NotNull(result);
        Assert.Empty(result);
    }

    [Fact]
    public void StringDeserializer_DeserializeNull_ShouldReturnEmpty()
    {
        // Arrange
        var deserializer = new StringDeserializer();
        var context = new SerializationContext { Topic = "test" };

        // Act
        var result = deserializer.Deserialize(ReadOnlySpan<byte>.Empty, true, context);

        // Assert
        Assert.NotNull(result);
        Assert.Empty(result);
    }

    [Fact]
    public void StringDeserializer_DeserializeUtf8_ShouldReturnString()
    {
        // Arrange
        var deserializer = new StringDeserializer();
        var context = new SerializationContext { Topic = "test" };
        var input = "Hello, Kafka!";
        var bytes = Encoding.UTF8.GetBytes(input);

        // Act
        var result = deserializer.Deserialize(bytes, false, context);

        // Assert
        Assert.Equal(input, result);
    }

    [Fact]
    public void ByteArraySerializer_SerializeNull_ShouldReturnEmpty()
    {
        // Arrange
        var serializer = new ByteArraySerializer();
        var context = new SerializationContext { Topic = "test" };

        // Act
        var result = serializer.Serialize(null!, context);

        // Assert
        Assert.NotNull(result);
        Assert.Empty(result);
    }

    [Fact]
    public void ByteArraySerializer_SerializeBytes_ShouldReturnSameBytes()
    {
        // Arrange
        var serializer = new ByteArraySerializer();
        var context = new SerializationContext { Topic = "test" };
        var input = new byte[] { 1, 2, 3, 4, 5 };

        // Act
        var result = serializer.Serialize(input, context);

        // Assert
        Assert.Equal(input, result);
    }

    [Fact]
    public void ByteArrayDeserializer_DeserializeEmpty_ShouldReturnEmpty()
    {
        // Arrange
        var deserializer = new ByteArrayDeserializer();
        var context = new SerializationContext { Topic = "test" };

        // Act
        var result = deserializer.Deserialize(ReadOnlySpan<byte>.Empty, false, context);

        // Assert
        Assert.NotNull(result);
        Assert.Empty(result);
    }

    [Fact]
    public void ByteArrayDeserializer_DeserializeNull_ShouldReturnEmpty()
    {
        // Arrange
        var deserializer = new ByteArrayDeserializer();
        var context = new SerializationContext { Topic = "test" };

        // Act
        var result = deserializer.Deserialize(ReadOnlySpan<byte>.Empty, true, context);

        // Assert
        Assert.NotNull(result);
        Assert.Empty(result);
    }

    [Fact]
    public void ByteArrayDeserializer_DeserializeBytes_ShouldReturnSameBytes()
    {
        // Arrange
        var deserializer = new ByteArrayDeserializer();
        var context = new SerializationContext { Topic = "test" };
        var input = new byte[] { 1, 2, 3, 4, 5 };

        // Act
        var result = deserializer.Deserialize(input, false, context);

        // Assert
        Assert.Equal(input, result);
    }

    [Fact]
    public void SerializationRoundTrip_String_ShouldPreserveValue()
    {
        // Arrange
        var serializer = new StringSerializer();
        var deserializer = new StringDeserializer();
        var context = new SerializationContext { Topic = "test" };
        var original = "Test message with Unicode: \u00E9\u00F1\u00FC";

        // Act
        var serialized = serializer.Serialize(original, context);
        var deserialized = deserializer.Deserialize(serialized, false, context);

        // Assert
        Assert.Equal(original, deserialized);
    }

    [Fact]
    public void SerializationContext_Creation_ShouldSucceed()
    {
        // Arrange & Act
        var context = new SerializationContext
        {
            Topic = "test-topic",
            Partition = new Partition(5)
        };

        // Assert
        Assert.Equal("test-topic", context.Topic);
        Assert.Equal(5, context.Partition.Value);
    }
}
