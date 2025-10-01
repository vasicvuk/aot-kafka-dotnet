using System.Runtime.InteropServices;

namespace AotKafka.Native;

/// <summary>
/// Native rd_kafka_message_t structure
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public unsafe struct RdKafkaMessage
{
    public ErrorCode Err;
    public IntPtr Rkt;
    public int Partition;
    public IntPtr Payload;
    public UIntPtr Len;
    public IntPtr Key;
    public UIntPtr KeyLen;
    public long Offset;
    public IntPtr PrivateData;
}

/// <summary>
/// Native rd_kafka_topic_partition_t structure
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public struct RdKafkaTopicPartition
{
    public IntPtr Topic;
    public int Partition;
    public long Offset;
    public IntPtr Metadata;
    public UIntPtr MetadataSize;
    public IntPtr Opaque;
    public ErrorCode Err;
    public IntPtr PrivateField;
}