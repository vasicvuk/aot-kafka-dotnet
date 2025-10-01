using System.Runtime.InteropServices;
using AotKafka.Core;
using AotKafka.Native;
using AotKafka.Deserializers;

namespace AotKafka;

/// <summary>
/// High-level Kafka consumer
/// </summary>
public class Consumer<TKey, TValue> : IDisposable
{
    private readonly SafeKafkaHandle _handle;
    private readonly IDeserializer<TKey> _keyDeserializer;
    private readonly IDeserializer<TValue> _valueDeserializer;
    private bool _disposed;
    private bool _closed;
    private readonly List<string> _subscription = new();

    /// <summary>
    /// Gets the consumer name
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the consumer group member ID
    /// </summary>
    public string MemberID { get; private set; } = string.Empty;

    /// <summary>
    /// Gets the current subscription
    /// </summary>
    public IEnumerable<string> Subscription => _subscription.AsReadOnly();

    /// <summary>
    /// Creates a new consumer
    /// </summary>
    public Consumer(
        ConsumerConfig config,
        IDeserializer<TKey>? keyDeserializer = null,
        IDeserializer<TValue>? valueDeserializer = null)
    {
        ArgumentNullException.ThrowIfNull(config);

        _keyDeserializer = keyDeserializer ?? GetDefaultDeserializer<TKey>();
        _valueDeserializer = valueDeserializer ?? GetDefaultDeserializer<TValue>();

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
                var handle = LibRdKafka.rd_kafka_new(RdKafkaType.Consumer, confHandle, errstrCreate, (UIntPtr)512);
                if (handle == IntPtr.Zero)
                {
                    var error = Utf8Marshaller.PtrToStringUtf8(errstrCreate);
                    throw new KafkaException(new Error(ErrorCode.Fail, error ?? "Failed to create consumer"));
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
    /// Subscribe to a single topic
    /// </summary>
    public void Subscribe(string topic)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ObjectDisposedException.ThrowIf(_disposed || _closed, this);

        Subscribe(new[] { topic });
    }

    /// <summary>
    /// Subscribe to multiple topics
    /// </summary>
    public void Subscribe(IEnumerable<string> topics)
    {
        ArgumentNullException.ThrowIfNull(topics);
        ObjectDisposedException.ThrowIf(_disposed || _closed, this);

        var topicList = topics.ToList();
        if (topicList.Count == 0)
        {
            throw new ArgumentException("Topics list cannot be empty", nameof(topics));
        }

        if (topicList.Any(string.IsNullOrWhiteSpace))
        {
            throw new ArgumentException("Topics list contains null or empty entries", nameof(topics));
        }

        var partitionList = LibRdKafka.rd_kafka_topic_partition_list_new(topicList.Count);
        try
        {
            foreach (var topic in topicList)
            {
                LibRdKafka.rd_kafka_topic_partition_list_add(partitionList, topic, -1);
            }

            var result = LibRdKafka.rd_kafka_subscribe(_handle.DangerousGetHandle(), partitionList);
            if (result != ErrorCode.NoError)
            {
                var error = ErrorHandler.FromErrorCode(result);
                throw new KafkaException(error);
            }

            _subscription.Clear();
            _subscription.AddRange(topicList);
        }
        finally
        {
            LibRdKafka.rd_kafka_topic_partition_list_destroy(partitionList);
        }
    }

    /// <summary>
    /// Poll for next message
    /// </summary>
    public ConsumeResult<TKey, TValue> Consume(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed || _closed, this);

        if (_subscription.Count == 0)
        {
            throw new InvalidOperationException("Consumer is not subscribed to any topics. Call Subscribe first.");
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            IntPtr messagePtr;
            bool success = false;
            _handle.DangerousAddRef(ref success);
            try
            {
                if (!success)
                {
                    throw new ObjectDisposedException("Consumer handle has been disposed");
                }

                messagePtr = LibRdKafka.rd_kafka_consumer_poll(_handle.DangerousGetHandle(), 1000);
            }
            finally
            {
                if (success)
                {
                    _handle.DangerousRelease();
                }
            }

            if (messagePtr == IntPtr.Zero)
            {
                continue;
            }

            try
            {
                var nativeMsg = Marshal.PtrToStructure<RdKafkaMessage>(messagePtr);

                if (nativeMsg.Err != ErrorCode.NoError)
                {
                    if (nativeMsg.Err == ErrorCode.PartitionEof)
                    {
                        var topicName = nativeMsg.Rkt != IntPtr.Zero
                            ? (Utf8Marshaller.PtrToStringUtf8(LibRdKafka.rd_kafka_topic_name(nativeMsg.Rkt)) ?? "unknown")
                            : "unknown";
                        return new ConsumeResult<TKey, TValue>
                        {
                            Message = new Message<TKey, TValue>(),
                            Topic = topicName,
                            Partition = new Partition(nativeMsg.Partition),
                            Offset = new Offset(nativeMsg.Offset),
                            IsPartitionEOF = true
                        };
                    }

                    var error = ErrorHandler.FromErrorCode(nativeMsg.Err);
                    throw new ConsumeException(error);
                }

                var topic = nativeMsg.Rkt != IntPtr.Zero
                    ? (Utf8Marshaller.PtrToStringUtf8(LibRdKafka.rd_kafka_topic_name(nativeMsg.Rkt)) ?? "unknown")
                    : "unknown";
                var context = new SerializationContext { Topic = topic, Partition = new Partition(nativeMsg.Partition) };

                byte[] keyBytes = Array.Empty<byte>();
                if (nativeMsg.Key != IntPtr.Zero && nativeMsg.KeyLen != UIntPtr.Zero)
                {
                    keyBytes = new byte[(int)nativeMsg.KeyLen];
                    Marshal.Copy(nativeMsg.Key, keyBytes, 0, keyBytes.Length);
                }

                byte[] valueBytes = Array.Empty<byte>();
                if (nativeMsg.Payload != IntPtr.Zero && nativeMsg.Len != UIntPtr.Zero)
                {
                    valueBytes = new byte[(int)nativeMsg.Len];
                    Marshal.Copy(nativeMsg.Payload, valueBytes, 0, valueBytes.Length);
                }

                var key = _keyDeserializer.Deserialize(keyBytes, keyBytes.Length == 0, context);
                var value = _valueDeserializer.Deserialize(valueBytes, valueBytes.Length == 0, context);

                return new ConsumeResult<TKey, TValue>
                {
                    Message = new Message<TKey, TValue>
                    {
                        Key = key,
                        Value = value,
                        Timestamp = new Timestamp(0)
                    },
                    Topic = topic,
                    Partition = new Partition(nativeMsg.Partition),
                    Offset = new Offset(nativeMsg.Offset),
                    IsPartitionEOF = false
                };
            }
            finally
            {
                LibRdKafka.rd_kafka_message_destroy(messagePtr);
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
        throw new OperationCanceledException();
    }

    /// <summary>
    /// Commit offsets
    /// </summary>
    public void Commit()
    {
        ObjectDisposedException.ThrowIf(_disposed || _closed, this);

        var result = LibRdKafka.rd_kafka_commit(_handle.DangerousGetHandle(), IntPtr.Zero, 0);
        if (result != ErrorCode.NoError)
        {
            var error = ErrorHandler.FromErrorCode(result);
            throw new KafkaException(error);
        }
    }

    /// <summary>
    /// Commit specific message offset
    /// </summary>
    public void Commit(ConsumeResult<TKey, TValue> result)
    {
        ArgumentNullException.ThrowIfNull(result);
        ObjectDisposedException.ThrowIf(_disposed || _closed, this);

        // Simplified implementation - full implementation would construct offset list
        Commit();
    }

    /// <summary>
    /// Close the consumer
    /// </summary>
    public void Close()
    {
        if (_closed)
        {
            return;
        }

        LibRdKafka.rd_kafka_consumer_close(_handle.DangerousGetHandle());
        _closed = true;
    }

    /// <summary>
    /// Dispose the consumer and release resources
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        Close();
        _handle?.Dispose();
        _disposed = true;
    }

    private static IDeserializer<T> GetDefaultDeserializer<T>()
    {
        if (typeof(T) == typeof(string))
        {
            return (IDeserializer<T>)(object)new StringDeserializer();
        }
        if (typeof(T) == typeof(byte[]))
        {
            return (IDeserializer<T>)(object)new ByteArrayDeserializer();
        }

        throw new ArgumentException($"No default deserializer available for type {typeof(T).Name}. Please provide a custom deserializer.");
    }
}