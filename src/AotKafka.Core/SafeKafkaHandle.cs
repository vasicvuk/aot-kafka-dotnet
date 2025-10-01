using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using AotKafka.Native;

namespace AotKafka.Core;

/// <summary>
/// Safe handle for rd_kafka_t
/// </summary>
public sealed class SafeKafkaHandle : SafeHandleZeroOrMinusOneIsInvalid
{
    public SafeKafkaHandle() : base(ownsHandle: true)
    {
    }

    protected override bool ReleaseHandle()
    {
        LibRdKafka.rd_kafka_destroy(handle);
        return true;
    }
}