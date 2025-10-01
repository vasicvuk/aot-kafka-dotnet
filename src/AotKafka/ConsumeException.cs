using AotKafka.Core;

namespace AotKafka;

/// <summary>
/// Exception thrown when message consumption fails
/// </summary>
public class ConsumeException : KafkaException
{
    /// <summary>
    /// Gets the consume result if available
    /// </summary>
    public ConsumeResult<byte[], byte[]>? ConsumeResult { get; }

    /// <summary>
    /// Initialize ConsumeException
    /// </summary>
    public ConsumeException(Error error, ConsumeResult<byte[], byte[]>? consumeResult = null)
        : base(error)
    {
        ConsumeResult = consumeResult;
    }
}