using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;

namespace AotKafka.Native;

public static partial class LibRdKafka
{
    [LibraryImport("librdkafka", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial IntPtr rd_kafka_conf_new();

    [LibraryImport("librdkafka", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial void rd_kafka_conf_destroy(IntPtr conf);

    [LibraryImport("librdkafka", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial ErrorCode rd_kafka_conf_set(
        IntPtr conf,
        string name,
        string value,
        IntPtr errstr,
        UIntPtr errstr_size);
}