using AotKafka.Core;

namespace AotKafka;

/// <summary>
/// Base exception for Kafka operations
/// </summary>
public class KafkaException : Exception
{
    /// <summary>
    /// Gets the error information
    /// </summary>
    public Error Error { get; }

    /// <summary>
    /// Initialize KafkaException with error
    /// </summary>
    public KafkaException(Error error)
        : base(error.Reason)
    {
        Error = error;
    }

    /// <summary>
    /// Initialize KafkaException with error and inner exception
    /// </summary>
    public KafkaException(Error error, Exception innerException)
        : base(error.Reason, innerException)
    {
        Error = error;
    }
}