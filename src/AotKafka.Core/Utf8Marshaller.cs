using System.Runtime.InteropServices;
using System.Text;

namespace AotKafka.Core;

/// <summary>
/// AOT-safe UTF-8 string marshaling helpers
/// </summary>
public static class Utf8Marshaller
{
    public static unsafe IntPtr StringToHGlobalUtf8(string? str)
    {
        if (str == null)
        {
            return IntPtr.Zero;
        }

        var bytes = Encoding.UTF8.GetBytes(str);
        var ptr = Marshal.AllocHGlobal(bytes.Length + 1);
        Marshal.Copy(bytes, 0, ptr, bytes.Length);
        Marshal.WriteByte(ptr, bytes.Length, 0); // null terminator
        return ptr;
    }

    public static unsafe string? PtrToStringUtf8(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero)
        {
            return null;
        }

        var length = 0;
        while (Marshal.ReadByte(ptr, length) != 0)
        {
            length++;
        }

        if (length == 0)
        {
            return string.Empty;
        }

        var bytes = new byte[length];
        Marshal.Copy(ptr, bytes, 0, length);
        return Encoding.UTF8.GetString(bytes);
    }

    public static void FreeHGlobal(IntPtr ptr)
    {
        if (ptr != IntPtr.Zero)
        {
            Marshal.FreeHGlobal(ptr);
        }
    }
}