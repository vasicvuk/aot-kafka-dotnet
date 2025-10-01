using System.Runtime.InteropServices;
using AotKafka.Core;
using AotKafka.Native;
using AotKafka.Serializers;

namespace AotKafka;

/// <summary>
/// High-level Kafka producer
/// </summary>
public class Producer<TKey, TValue> : IDisposable
{
    private readonly SafeKafkaHandle _handle;
    private readonly ISerializer<TKey> _keySerializer;
    private readonly ISerializer<TValue> _valueSerializer;
    private bool _disposed;

    /// <summary>
    /// Gets the producer name
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Creates a new producer
    /// </summary>
    public Producer(
        ProducerConfig config,
        ISerializer<TKey>? keySerializer = null,
        ISerializer<TValue>? valueSerializer = null)
    {
        ArgumentNullException.ThrowIfNull(config);

        _keySerializer = keySerializer ?? GetDefaultSerializer<TKey>();
        _valueSerializer = valueSerializer ?? GetDefaultSerializer<TValue>();

        var nativeConfig = config.ToNativeConfig();
        var confHandle = LibRdKafka.rd_kafka_conf_new();

        try
        {
            foreach (var kvp in nativeConfig)
            {
                var errstr = Marshal.AllocHGlobal(512);
                try
                {
                    var result = LibRdKafka.rd_kafka_conf_set(confHandle, kvp.Key, kvp.Value, errstr, (UIntPtr)512);
                    if (result != ErrorCode.NoError)
                    {
                        var error = Utf8Marshaller.PtrToStringUtf8(errstr);
                        throw new ArgumentException($"Failed to set config '{kvp.Key}': {error}");
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
                var handle = LibRdKafka.rd_kafka_new(RdKafkaType.Producer, confHandle, errstrCreate, (UIntPtr)512);
                if (handle == IntPtr.Zero)
                {
                    var error = Utf8Marshaller.PtrToStringUtf8(errstrCreate);
                    throw new KafkaException(new Error(ErrorCode.Fail, error ?? "Failed to create producer"));
                }

                _handle = new SafeKafkaHandle();
                Marshal.InitHandle(_handle, handle);
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
    /// Asynchronously produce a message
    /// </summary>
    public Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        string topic,
        Message<TKey, TValue> message,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(message);
        ObjectDisposedException.ThrowIf(_disposed, this);

        // For MVP, we'll implement synchronous produce and wrap in Task
        // Full async implementation would use delivery callbacks
        var result = ProduceInternal(topic, message);
        return Task.FromResult(result);
    }

    /// <summary>
    /// Synchronously produce a message with optional delivery handler
    /// </summary>
    public void Produce(
        string topic,
        Message<TKey, TValue> message,
        Action<DeliveryResult<TKey, TValue>>? deliveryHandler = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(message);
        ObjectDisposedException.ThrowIf(_disposed, this);

        var result = ProduceInternal(topic, message);
        deliveryHandler?.Invoke(result);
    }

    private DeliveryResult<TKey, TValue> ProduceInternal(string topic, Message<TKey, TValue> message)
    {
        var context = new SerializationContext { Topic = topic, Partition = Partition.Any };
        var keyBytes = _keySerializer.Serialize(message.Key!, context);
        var valueBytes = _valueSerializer.Serialize(message.Value!, context);

        IntPtr keyPtr = IntPtr.Zero;
        IntPtr valuePtr = IntPtr.Zero;
        IntPtr topicHandle = IntPtr.Zero;

        try
        {
            if (keyBytes.Length > 0)
            {
                keyPtr = Marshal.AllocHGlobal(keyBytes.Length);
                Marshal.Copy(keyBytes, 0, keyPtr, keyBytes.Length);
            }

            if (valueBytes.Length > 0)
            {
                valuePtr = Marshal.AllocHGlobal(valueBytes.Length);
                Marshal.Copy(valueBytes, 0, valuePtr, valueBytes.Length);
            }

            topicHandle = LibRdKafka.rd_kafka_topic_new(_handle.DangerousGetHandle(), topic, IntPtr.Zero);
            if (topicHandle == IntPtr.Zero)
            {
                throw new KafkaException(new Error(ErrorCode.UnknownTopic, $"Failed to create topic handle for '{topic}'"));
            }

            var result = LibRdKafka.rd_kafka_produce(
                topicHandle,
                -1, // RD_KAFKA_PARTITION_UA (unassigned)
                0,  // msgflags
                valuePtr,
                (UIntPtr)valueBytes.Length,
                keyPtr,
                (UIntPtr)keyBytes.Length,
                IntPtr.Zero);

            if (result != ErrorCode.NoError)
            {
                var error = ErrorHandler.FromErrorCode(result);
                throw new ProduceException<TKey, TValue>(error);
            }

            // Flush to ensure message is sent
            LibRdKafka.rd_kafka_flush(_handle.DangerousGetHandle(), 10000);

            return new DeliveryResult<TKey, TValue>
            {
                Message = message,
                Topic = topic,
                Partition = new Partition(0),
                Offset = new Offset(0),
                Status = PersistenceStatus.Persisted
            };
        }
        finally
        {
            if (topicHandle != IntPtr.Zero)
            {
                LibRdKafka.rd_kafka_topic_destroy(topicHandle);
            }
            if (keyPtr != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(keyPtr);
            }
            if (valuePtr != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(valuePtr);
            }
        }
    }

    /// <summary>
    /// Wait for all outstanding produce requests to complete
    /// </summary>
    public int Flush(TimeSpan timeout)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var result = LibRdKafka.rd_kafka_flush(_handle.DangerousGetHandle(), (int)timeout.TotalMilliseconds);
        if (result != ErrorCode.NoError)
        {
            return LibRdKafka.rd_kafka_outq_len(_handle.DangerousGetHandle());
        }
        return 0;
    }

    /// <summary>
    /// Dispose the producer and release resources
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        Flush(TimeSpan.FromSeconds(10));
        _handle?.Dispose();
        _disposed = true;
    }

    private static ISerializer<T> GetDefaultSerializer<T>()
    {
        if (typeof(T) == typeof(string))
        {
            return (ISerializer<T>)(object)new StringSerializer();
        }
        if (typeof(T) == typeof(byte[]))
        {
            return (ISerializer<T>)(object)new ByteArraySerializer();
        }

        throw new ArgumentException($"No default serializer available for type {typeof(T).Name}. Please provide a custom serializer.");
    }
}