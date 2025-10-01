using AotKafka.Native;

namespace AotKafka.Core;

/// <summary>
/// Error information
/// </summary>
public readonly struct Error
{
    public ErrorCode Code { get; init; }
    public string Reason { get; init; }
    public bool IsFatal { get; init; }

    public Error(ErrorCode code, string reason, bool isFatal = false)
    {
        Code = code;
        Reason = reason ?? string.Empty;
        IsFatal = isFatal;
    }

    public bool IsError => Code != ErrorCode.NoError;
}

/// <summary>
/// Error handling utilities
/// </summary>
public static class ErrorHandler
{
    public static Error FromErrorCode(ErrorCode code)
    {
        var reasonPtr = LibRdKafka.rd_kafka_err2str(code);
        var reason = Utf8Marshaller.PtrToStringUtf8(reasonPtr) ?? "Unknown error";
        return new Error(code, reason, IsFatalError(code));
    }

    private static bool IsFatalError(ErrorCode code)
    {
        return code switch
        {
            ErrorCode.Destroy => true,
            ErrorCode.Fail => true,
            ErrorCode.CriticalSystemResource => true,
            _ => false
        };
    }
}