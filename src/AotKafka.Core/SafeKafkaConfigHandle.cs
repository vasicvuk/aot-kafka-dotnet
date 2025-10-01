using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using AotKafka.Native;

namespace AotKafka.Core;

/// <summary>
/// Safe handle for rd_kafka_conf_t
/// </summary>
public sealed class SafeKafkaConfigHandle : SafeHandleZeroOrMinusOneIsInvalid
{
    public SafeKafkaConfigHandle() : base(ownsHandle: true)
    {
    }

    protected override bool ReleaseHandle()
    {
        LibRdKafka.rd_kafka_conf_destroy(handle);
        return true;
    }
}