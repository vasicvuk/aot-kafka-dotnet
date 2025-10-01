using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using AotKafka.Native;

namespace AotKafka.Core;

/// <summary>
/// Safe handle for rd_kafka_topic_t
/// </summary>
public sealed class SafeKafkaTopicHandle : SafeHandleZeroOrMinusOneIsInvalid
{
    public SafeKafkaTopicHandle() : base(ownsHandle: true)
    {
    }

    protected override bool ReleaseHandle()
    {
        LibRdKafka.rd_kafka_topic_destroy(handle);
        return true;
    }
}