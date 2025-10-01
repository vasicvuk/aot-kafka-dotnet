using Xunit;

namespace AotKafka.Tests;

/// <summary>
/// Contract tests for core types - verify Offset, Partition, Timestamp behavior
/// </summary>
public class TypesContractTests
{
    [Fact]
    public void Offset_SpecialValues_ShouldBeCorrect()
    {
        // Assert
        Assert.Equal(-1001, Offset.Unset.Value);
        Assert.Equal(-2, Offset.Beginning.Value);
        Assert.Equal(-1, Offset.End.Value);
    }

    [Fact]
    public void Offset_Equality_ShouldWork()
    {
        // Arrange
        var offset1 = new Offset(100);
        var offset2 = new Offset(100);
        var offset3 = new Offset(200);

        // Assert
        Assert.Equal(offset1, offset2);
        Assert.NotEqual(offset1, offset3);
        Assert.True(offset1 == offset2);
        Assert.False(offset1 == offset3);
        Assert.False(offset1 != offset2);
        Assert.True(offset1 != offset3);
    }

    [Fact]
    public void Offset_Comparison_ShouldWork()
    {
        // Arrange
        var offset1 = new Offset(100);
        var offset2 = new Offset(200);

        // Assert
        Assert.True(offset1 < offset2);
        Assert.True(offset2 > offset1);
        Assert.True(offset1 <= offset2);
        Assert.True(offset2 >= offset1);
        Assert.True(offset1 <= new Offset(100));
        Assert.True(offset1.CompareTo(offset2) < 0);
        Assert.True(offset2.CompareTo(offset1) > 0);
        Assert.Equal(0, offset1.CompareTo(new Offset(100)));
    }

    [Fact]
    public void Offset_ImplicitConversion_ShouldWork()
    {
        // Arrange
        Offset offset = 150L;
        long value = offset;

        // Assert
        Assert.Equal(150, offset.Value);
        Assert.Equal(150L, value);
    }

    [Fact]
    public void Offset_ToString_ShouldReturnValue()
    {
        // Arrange
        var offset = new Offset(42);

        // Act
        var result = offset.ToString();

        // Assert
        Assert.Equal("42", result);
    }

    [Fact]
    public void Offset_GetHashCode_ShouldBeConsistent()
    {
        // Arrange
        var offset1 = new Offset(100);
        var offset2 = new Offset(100);

        // Assert
        Assert.Equal(offset1.GetHashCode(), offset2.GetHashCode());
    }

    [Fact]
    public void Partition_SpecialValues_ShouldBeCorrect()
    {
        // Assert
        Assert.Equal(-1, Partition.Unassigned.Value);
        Assert.Equal(-1, Partition.Any.Value);
    }

    [Fact]
    public void Partition_Equality_ShouldWork()
    {
        // Arrange
        var partition1 = new Partition(5);
        var partition2 = new Partition(5);
        var partition3 = new Partition(10);

        // Assert
        Assert.Equal(partition1, partition2);
        Assert.NotEqual(partition1, partition3);
        Assert.True(partition1 == partition2);
        Assert.False(partition1 == partition3);
    }

    [Fact]
    public void Partition_ImplicitConversion_ShouldWork()
    {
        // Arrange
        Partition partition = 7;
        int value = partition;

        // Assert
        Assert.Equal(7, partition.Value);
        Assert.Equal(7, value);
    }

    [Fact]
    public void Partition_ToString_ShouldReturnValue()
    {
        // Arrange
        var partition = new Partition(3);

        // Act
        var result = partition.ToString();

        // Assert
        Assert.Equal("3", result);
    }

    [Fact]
    public void Timestamp_Default_ShouldBeZero()
    {
        // Assert
        Assert.Equal(0, Timestamp.Default.UnixTimestampMs);
    }

    [Fact]
    public void Timestamp_FromUnixMs_ShouldWork()
    {
        // Arrange
        var timestamp = new Timestamp(1609459200000); // 2021-01-01 00:00:00 UTC

        // Assert
        Assert.Equal(1609459200000, timestamp.UnixTimestampMs);
        Assert.Equal(2021, timestamp.UtcDateTime.Year);
        Assert.Equal(1, timestamp.UtcDateTime.Month);
        Assert.Equal(1, timestamp.UtcDateTime.Day);
    }

    [Fact]
    public void Timestamp_FromDateTime_ShouldWork()
    {
        // Arrange
        var dateTime = new DateTime(2021, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var timestamp = new Timestamp(dateTime);

        // Assert
        Assert.Equal(1609459200000, timestamp.UnixTimestampMs);
    }

    [Fact]
    public void Timestamp_Equality_ShouldWork()
    {
        // Arrange
        var timestamp1 = new Timestamp(1000000);
        var timestamp2 = new Timestamp(1000000);
        var timestamp3 = new Timestamp(2000000);

        // Assert
        Assert.Equal(timestamp1, timestamp2);
        Assert.NotEqual(timestamp1, timestamp3);
        Assert.True(timestamp1 == timestamp2);
        Assert.False(timestamp1 == timestamp3);
    }

    [Fact]
    public void Timestamp_ToString_ShouldReturnIso8601()
    {
        // Arrange
        var timestamp = new Timestamp(new DateTime(2021, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        // Act
        var result = timestamp.ToString();

        // Assert
        Assert.Contains("2021", result);
        Assert.Contains("01", result);
    }

    [Fact]
    public void Headers_AddAndIterate_ShouldWork()
    {
        // Arrange
        var headers = new Headers();

        // Act
        headers.Add("key1", new byte[] { 1, 2, 3 });
        headers.Add("key2", new byte[] { 4, 5, 6 });

        // Assert
        var headerList = headers.ToList();
        Assert.Equal(2, headerList.Count);
        Assert.Contains(headerList, h => h.Key == "key1");
        Assert.Contains(headerList, h => h.Key == "key2");
    }

    [Fact]
    public void Headers_AddHeader_WithNullKey_ShouldThrow()
    {
        // Arrange
        var headers = new Headers();

        // Act & Assert - ArgumentException.ThrowIfNullOrWhiteSpace throws ArgumentNullException for null
        Assert.Throws<ArgumentNullException>(() => headers.Add(null!, new byte[] { 1 }));
    }

    [Fact]
    public void Headers_AddHeader_WithNullValue_ShouldThrow()
    {
        // Arrange
        var headers = new Headers();

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => headers.Add("key", null!));
    }

    [Fact]
    public void Headers_AddIHeader_ShouldWork()
    {
        // Arrange
        var headers = new Headers();
        var header = new Header { Key = "test", Value = new byte[] { 1, 2 } };

        // Act
        headers.Add(header);

        // Assert
        var headerList = headers.ToList();
        Assert.Single(headerList);
        Assert.Equal("test", headerList[0].Key);
        Assert.Equal(new byte[] { 1, 2 }, headerList[0].Value);
    }

    [Fact]
    public void CompressionType_EnumValues_ShouldBeCorrect()
    {
        // Assert - verify enum exists with expected values
        Assert.Equal(0, (int)CompressionType.None);
        Assert.True(Enum.IsDefined(typeof(CompressionType), CompressionType.Gzip));
        Assert.True(Enum.IsDefined(typeof(CompressionType), CompressionType.Snappy));
        Assert.True(Enum.IsDefined(typeof(CompressionType), CompressionType.Lz4));
        Assert.True(Enum.IsDefined(typeof(CompressionType), CompressionType.Zstd));
    }

    [Fact]
    public void Acks_EnumValues_ShouldBeCorrect()
    {
        // Assert - verify enum exists with expected values
        Assert.True(Enum.IsDefined(typeof(Acks), Acks.None));
        Assert.True(Enum.IsDefined(typeof(Acks), Acks.Leader));
        Assert.True(Enum.IsDefined(typeof(Acks), Acks.All));
    }

    [Fact]
    public void AutoOffsetReset_EnumValues_ShouldBeCorrect()
    {
        // Assert - verify enum exists with expected values
        Assert.True(Enum.IsDefined(typeof(AutoOffsetReset), AutoOffsetReset.Latest));
        Assert.True(Enum.IsDefined(typeof(AutoOffsetReset), AutoOffsetReset.Earliest));
        Assert.True(Enum.IsDefined(typeof(AutoOffsetReset), AutoOffsetReset.Error));
    }

    [Fact]
    public void PersistenceStatus_EnumValues_ShouldBeCorrect()
    {
        // Assert - verify enum exists with expected values
        Assert.True(Enum.IsDefined(typeof(PersistenceStatus), PersistenceStatus.NotPersisted));
        Assert.True(Enum.IsDefined(typeof(PersistenceStatus), PersistenceStatus.PossiblyPersisted));
        Assert.True(Enum.IsDefined(typeof(PersistenceStatus), PersistenceStatus.Persisted));
    }
}
