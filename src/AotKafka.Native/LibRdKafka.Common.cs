using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;

namespace AotKafka.Native;

public static partial class LibRdKafka
{
    [LibraryImport("librdkafka", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial IntPtr rd_kafka_new(
        RdKafkaType type,
        IntPtr conf,
        IntPtr errstr,
        UIntPtr errstr_size);

    [LibraryImport("librdkafka", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial void rd_kafka_destroy(IntPtr rk);

    [LibraryImport("librdkafka", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial IntPtr rd_kafka_name(IntPtr rk);

    [LibraryImport("librdkafka", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial IntPtr rd_kafka_err2str(ErrorCode err);

    [LibraryImport("librdkafka")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial ErrorCode rd_kafka_last_error();
}