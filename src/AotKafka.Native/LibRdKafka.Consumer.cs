using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;

namespace AotKafka.Native;

public static partial class LibRdKafka
{
    [LibraryImport("librdkafka", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial ErrorCode rd_kafka_subscribe(
        IntPtr rk,
        IntPtr topics);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial IntPtr rd_kafka_consumer_poll(
        IntPtr rk,
        int timeout_ms);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial void rd_kafka_message_destroy(IntPtr rkmessage);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial ErrorCode rd_kafka_commit(
        IntPtr rk,
        IntPtr offsets,
        int async);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial ErrorCode rd_kafka_consumer_close(IntPtr rk);

    [LibraryImport("librdkafka", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial IntPtr rd_kafka_topic_partition_list_new(int size);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial void rd_kafka_topic_partition_list_destroy(IntPtr rkparlist);

    [LibraryImport("librdkafka", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial IntPtr rd_kafka_topic_partition_list_add(
        IntPtr rkparlist,
        string topic,
        int partition);

    [LibraryImport("librdkafka", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial IntPtr rd_kafka_topic_name(IntPtr rkt);
}