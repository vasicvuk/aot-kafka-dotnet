using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;

namespace AotKafka.Native;

public static partial class LibRdKafka
{
    [LibraryImport("librdkafka", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial ErrorCode rd_kafka_produce(
        IntPtr rkt,
        int partition,
        int msgflags,
        IntPtr payload,
        UIntPtr len,
        IntPtr key,
        UIntPtr keylen,
        IntPtr msg_opaque);

    [LibraryImport("librdkafka", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial ErrorCode rd_kafka_flush(IntPtr rk, int timeout_ms);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial int rd_kafka_outq_len(IntPtr rk);

    [LibraryImport("librdkafka", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial IntPtr rd_kafka_topic_new(
        IntPtr rk,
        string topic,
        IntPtr conf);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial void rd_kafka_topic_destroy(IntPtr rkt);
}