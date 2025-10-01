using AotKafka.Core;

namespace AotKafka;

/// <summary>
/// Exception thrown when message production fails
/// </summary>
public class ProduceException<TKey, TValue> : KafkaException
{
    /// <summary>
    /// Gets the delivery result
    /// </summary>
    public DeliveryResult<TKey, TValue>? DeliveryResult { get; }

    /// <summary>
    /// Initialize ProduceException
    /// </summary>
    public ProduceException(Error error, DeliveryResult<TKey, TValue>? deliveryResult = null)
        : base(error)
    {
        DeliveryResult = deliveryResult;
    }
}