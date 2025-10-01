using System.Runtime.InteropServices;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using AotKafka.Core;
using AotKafka.Native;

namespace AotKafka;

/// <summary>
/// Kafka admin client providing topic management operations.
/// </summary>
public class AdminClient : IDisposable
{
    private readonly SafeKafkaHandle _handle;
    private bool _disposed;

    /// <summary>
    /// Gets the client name assigned by librdkafka.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Create a new admin client using the provided configuration.
    /// </summary>
    public AdminClient(AdminConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);

        var nativeConfig = config.ToNativeConfig();
        var confHandle = LibRdKafka.rd_kafka_conf_new();

        try
        {
            foreach (var kvp in nativeConfig)
            {
                var errstr = Marshal.AllocHGlobal(512);
                try
                {
                    var error = LibRdKafka.rd_kafka_conf_set(confHandle, kvp.Key, kvp.Value, errstr, (UIntPtr)512);
                    if (error != ErrorCode.NoError)
                    {
                        var message = Utf8Marshaller.PtrToStringUtf8(errstr) ?? "unknown";
                        throw new ArgumentException($"Failed to set config '{kvp.Key}': {message}");
                    }
                }
                finally
                {
                    Marshal.FreeHGlobal(errstr);
                }
            }

            var errstrCreate = Marshal.AllocHGlobal(512);
            try
            {
                var handlePtr = LibRdKafka.rd_kafka_new(RdKafkaType.Producer, confHandle, errstrCreate, (UIntPtr)512);
                if (handlePtr == IntPtr.Zero)
                {
                    var message = Utf8Marshaller.PtrToStringUtf8(errstrCreate) ?? "Failed to create admin client";
                    throw new KafkaException(new Error(ErrorCode.Fail, message));
                }

                _handle = new SafeKafkaHandle();
                Marshal.InitHandle(_handle, handlePtr);
            }
            finally
            {
                Marshal.FreeHGlobal(errstrCreate);
            }

            var namePtr = LibRdKafka.rd_kafka_name(_handle.DangerousGetHandle());
            Name = Utf8Marshaller.PtrToStringUtf8(namePtr) ?? "unknown";
        }
        catch
        {
            LibRdKafka.rd_kafka_conf_destroy(confHandle);
            throw;
        }
    }

    /// <summary>
    /// Create a topic with the specified partition and replication settings.
    /// </summary>
    public Task CreateTopicAsync(
        string topicName,
        int numPartitions,
        short replicationFactor,
        CancellationToken cancellationToken = default,
        TimeSpan? operationTimeout = null,
        IDictionary<string, string>? topicConfig = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);
        if (numPartitions <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(numPartitions), "Partition count must be greater than 0.");
        }
        if (replicationFactor <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(replicationFactor), "Replication factor must be greater than 0.");
        }

        ObjectDisposedException.ThrowIf(_disposed, this);

        CreateTopicInternal(topicName, numPartitions, replicationFactor, topicConfig, operationTimeout, cancellationToken);
        return Task.CompletedTask;
    }

    private void CreateTopicInternal(
        string topicName,
        int numPartitions,
        short replicationFactor,
        IDictionary<string, string>? topicConfig,
        TimeSpan? operationTimeout,
        CancellationToken cancellationToken)
    {
        IntPtr errstrPtr = IntPtr.Zero;
        IntPtr newTopicPtr = IntPtr.Zero;
        IntPtr queuePtr = IntPtr.Zero;
        IntPtr optionsPtr = IntPtr.Zero;

        try
        {
            errstrPtr = Marshal.AllocHGlobal(512);
            newTopicPtr = LibRdKafka.rd_kafka_NewTopic_new(topicName, numPartitions, replicationFactor, errstrPtr, (UIntPtr)512);
            if (newTopicPtr == IntPtr.Zero)
            {
                var message = Utf8Marshaller.PtrToStringUtf8(errstrPtr) ?? "Failed to create topic definition";
                throw new KafkaException(new Error(ErrorCode.InvalidArgument, message));
            }

            if (topicConfig != null)
            {
                foreach (var kvp in topicConfig)
                {
                    var result = LibRdKafka.rd_kafka_NewTopic_set_config(newTopicPtr, kvp.Key, kvp.Value);
                    if (result != ErrorCode.NoError)
                    {
                        throw new KafkaException(new Error(result, $"Failed to set topic config '{kvp.Key}'"));
                    }
                }
            }

            queuePtr = LibRdKafka.rd_kafka_queue_new(_handle.DangerousGetHandle());
            if (queuePtr == IntPtr.Zero)
            {
                throw new KafkaException(new Error(ErrorCode.Fail, "Failed to create admin queue"));
            }

            optionsPtr = LibRdKafka.rd_kafka_AdminOptions_new(_handle.DangerousGetHandle(), RdKafkaAdminOp.CreateTopics);
            if (optionsPtr == IntPtr.Zero)
            {
                throw new KafkaException(new Error(ErrorCode.Fail, "Failed to create admin options"));
            }

            if (operationTimeout.HasValue)
            {
                var timeoutMs = (int)Math.Ceiling(operationTimeout.Value.TotalMilliseconds);
                if (timeoutMs <= 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(operationTimeout), "Operation timeout must be positive.");
                }

                var optionsErr = LibRdKafka.rd_kafka_AdminOptions_set_operation_timeout(optionsPtr, timeoutMs, errstrPtr, (UIntPtr)512);
                if (optionsErr != ErrorCode.NoError)
                {
                    var message = Utf8Marshaller.PtrToStringUtf8(errstrPtr) ?? ErrorHandler.FromErrorCode(optionsErr).Reason;
                    throw new KafkaException(new Error(optionsErr, $"Failed to set operation timeout: {message}"));
                }
            }

            cancellationToken.ThrowIfCancellationRequested();

            var topics = new IntPtr[] { newTopicPtr };
            var handle = GCHandle.Alloc(topics, GCHandleType.Pinned);
            try
            {
                LibRdKafka.rd_kafka_CreateTopics(
                    _handle.DangerousGetHandle(),
                    handle.AddrOfPinnedObject(),
                    (UIntPtr)topics.Length,
                    optionsPtr,
                    queuePtr);
            }
            finally
            {
                handle.Free();
            }

            var deadline = operationTimeout.HasValue
                ? DateTime.UtcNow + operationTimeout.Value
                : DateTime.UtcNow + TimeSpan.FromSeconds(30);

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                int pollTimeoutMs = 100;
                if (operationTimeout.HasValue)
                {
                    var remaining = (int)(deadline - DateTime.UtcNow).TotalMilliseconds;
                    if (remaining <= 0)
                    {
                        throw new TimeoutException($"Timed out while waiting for topic '{topicName}' to be created.");
                    }

                    pollTimeoutMs = Math.Min(pollTimeoutMs, Math.Max(1, remaining));
                }

                var evtPtr = LibRdKafka.rd_kafka_queue_poll(queuePtr, pollTimeoutMs);
                if (evtPtr == IntPtr.Zero)
                {
                    continue;
                }

                try
                {
                    var eventType = (RdKafkaEventType)LibRdKafka.rd_kafka_event_type(evtPtr);
                if (eventType == RdKafkaEventType.Error)
                {
                    var errorCode = LibRdKafka.rd_kafka_event_error(evtPtr);
                    var errorPtr = LibRdKafka.rd_kafka_event_error_string(evtPtr);
                    var errorMessage = Utf8Marshaller.PtrToStringUtf8(errorPtr) ?? ErrorHandler.FromErrorCode(errorCode).Reason;
                    throw new KafkaException(new Error(errorCode, errorMessage));
                }

                if (eventType != RdKafkaEventType.CreateTopicsResult)
                {
                    continue;
                }

                var eventError = LibRdKafka.rd_kafka_event_error(evtPtr);
                    if (eventError != ErrorCode.NoError)
                    {
                        var eventErrorStrPtr = LibRdKafka.rd_kafka_event_error_string(evtPtr);
                        var eventMessage = Utf8Marshaller.PtrToStringUtf8(eventErrorStrPtr) ?? ErrorHandler.FromErrorCode(eventError).Reason;
                        throw new KafkaException(new Error(eventError, eventMessage));
                    }

                    var resultPtr = LibRdKafka.rd_kafka_event_CreateTopics_result(evtPtr);
                    if (resultPtr == IntPtr.Zero)
                    {
                        throw new KafkaException(new Error(ErrorCode.Fail, "CreateTopics returned an empty result."));
                    }

                    var topicsPtr = LibRdKafka.rd_kafka_CreateTopics_result_topics(resultPtr, out var count);
                    var total = checked((int)count.ToUInt64());

                    for (var i = 0; i < total; i++)
                    {
                        var topicResultPtr = Marshal.ReadIntPtr(topicsPtr, i * IntPtr.Size);
                        var topicError = LibRdKafka.rd_kafka_topic_result_error(topicResultPtr);

                        if (topicError != ErrorCode.NoError)
                        {
                            var namePtr = LibRdKafka.rd_kafka_topic_result_name(topicResultPtr);
                            var name = Utf8Marshaller.PtrToStringUtf8(namePtr) ?? topicName;

                            var errPtr = LibRdKafka.rd_kafka_topic_result_error_string(topicResultPtr);
                            var errMessage = Utf8Marshaller.PtrToStringUtf8(errPtr) ?? ErrorHandler.FromErrorCode(topicError).Reason;

                            throw new KafkaException(new Error(topicError, $"Failed to create topic '{name}': {errMessage}"));
                        }
                    }

                    break;
                }
                finally
                {
                    LibRdKafka.rd_kafka_event_destroy(evtPtr);
                }
            }
        }
        finally
        {
            if (optionsPtr != IntPtr.Zero)
            {
                LibRdKafka.rd_kafka_AdminOptions_destroy(optionsPtr);
            }

            if (queuePtr != IntPtr.Zero)
            {
                LibRdKafka.rd_kafka_queue_destroy(queuePtr);
            }

            if (newTopicPtr != IntPtr.Zero)
            {
                LibRdKafka.rd_kafka_NewTopic_destroy(newTopicPtr);
            }

            if (errstrPtr != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(errstrPtr);
            }
        }
    }

    /// <summary>
    /// Release native resources.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _handle.Dispose();
        _disposed = true;
    }
}




