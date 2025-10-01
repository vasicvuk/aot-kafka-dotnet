namespace AotKafka;

/// <summary>
/// Represents a Kafka message timestamp
/// </summary>
public readonly struct Timestamp : IEquatable<Timestamp>
{
    private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>
    /// Default timestamp (broker will assign)
    /// </summary>
    public static readonly Timestamp Default = new(0);

    /// <summary>
    /// Gets the Unix milliseconds value
    /// </summary>
    public long UnixTimestampMs { get; init; }

    /// <summary>
    /// Gets the UTC DateTime
    /// </summary>
    public DateTime UtcDateTime => UnixEpoch.AddMilliseconds(UnixTimestampMs);

    /// <summary>
    /// Creates a timestamp from Unix milliseconds
    /// </summary>
    public Timestamp(long unixTimestampMs)
    {
        UnixTimestampMs = unixTimestampMs;
    }

    /// <summary>
    /// Creates a timestamp from DateTime
    /// </summary>
    public Timestamp(DateTime dateTime)
    {
        UnixTimestampMs = (long)(dateTime.ToUniversalTime() - UnixEpoch).TotalMilliseconds;
    }

    /// <inheritdoc/>
    public bool Equals(Timestamp other) => UnixTimestampMs == other.UnixTimestampMs;
    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is Timestamp other && Equals(other);
    /// <inheritdoc/>
    public override int GetHashCode() => UnixTimestampMs.GetHashCode();
    /// <inheritdoc/>
    public override string ToString() => UtcDateTime.ToString("O");

    /// <summary>Equality operator</summary>
    public static bool operator ==(Timestamp left, Timestamp right) => left.Equals(right);
    /// <summary>Inequality operator</summary>
    public static bool operator !=(Timestamp left, Timestamp right) => !left.Equals(right);
}