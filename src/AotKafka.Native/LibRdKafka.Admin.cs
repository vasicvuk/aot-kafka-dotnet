using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace AotKafka.Native;

public static partial class LibRdKafka
{
    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial IntPtr rd_kafka_queue_new(IntPtr rk);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial void rd_kafka_queue_destroy(IntPtr rkqu);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial IntPtr rd_kafka_queue_poll(IntPtr rkqu, int timeout_ms);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial IntPtr rd_kafka_AdminOptions_new(IntPtr rk, RdKafkaAdminOp forApi);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial void rd_kafka_AdminOptions_destroy(IntPtr options);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial ErrorCode rd_kafka_AdminOptions_set_operation_timeout(
        IntPtr options,
        int timeout_ms,
        IntPtr errstr,
        UIntPtr errstr_size);

    [LibraryImport("librdkafka", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial IntPtr rd_kafka_NewTopic_new(
        string topic,
        int num_partitions,
        int replication_factor,
        IntPtr errstr,
        UIntPtr errstr_size);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial void rd_kafka_NewTopic_destroy(IntPtr new_topic);

    [LibraryImport("librdkafka", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial ErrorCode rd_kafka_NewTopic_set_config(
        IntPtr new_topic,
        string name,
        string value);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial void rd_kafka_CreateTopics(
        IntPtr rk,
        IntPtr new_topics,
        UIntPtr new_topic_cnt,
        IntPtr options,
        IntPtr rkqu);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial RdKafkaEventType rd_kafka_event_type(IntPtr rkev);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial ErrorCode rd_kafka_event_error(IntPtr rkev);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial IntPtr rd_kafka_event_error_string(IntPtr rkev);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial void rd_kafka_event_destroy(IntPtr rkev);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial IntPtr rd_kafka_event_CreateTopics_result(IntPtr rkev);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial IntPtr rd_kafka_CreateTopics_result_topics(
        IntPtr result,
        out UIntPtr count);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial ErrorCode rd_kafka_topic_result_error(IntPtr topicres);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial IntPtr rd_kafka_topic_result_error_string(IntPtr topicres);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial IntPtr rd_kafka_topic_result_name(IntPtr topicres);
}
