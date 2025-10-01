namespace AotKafka;

/// <summary>
/// Represents a Kafka offset
/// </summary>
public readonly struct Offset : IEquatable<Offset>, IComparable<Offset>
{
    /// <summary>
    /// Unset offset
    /// </summary>
    public static readonly Offset Unset = new(-1001);

    /// <summary>
    /// Start consuming from beginning of partition
    /// </summary>
    public static readonly Offset Beginning = new(-2);

    /// <summary>
    /// Start consuming from end of partition
    /// </summary>
    public static readonly Offset End = new(-1);

    /// <summary>
    /// Gets the offset value
    /// </summary>
    public long Value { get; init; }

    /// <summary>
    /// Creates a new offset instance
    /// </summary>
    public Offset(long value)
    {
        Value = value;
    }

    /// <inheritdoc/>
    public bool Equals(Offset other) => Value == other.Value;
    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is Offset other && Equals(other);
    /// <inheritdoc/>
    public override int GetHashCode() => Value.GetHashCode();
    /// <inheritdoc/>
    public override string ToString() => Value.ToString();
    /// <inheritdoc/>
    public int CompareTo(Offset other) => Value.CompareTo(other.Value);

    /// <summary>Equality operator</summary>
    public static bool operator ==(Offset left, Offset right) => left.Equals(right);
    /// <summary>Inequality operator</summary>
    public static bool operator !=(Offset left, Offset right) => !left.Equals(right);
    /// <summary>Less than operator</summary>
    public static bool operator <(Offset left, Offset right) => left.Value < right.Value;
    /// <summary>Greater than operator</summary>
    public static bool operator >(Offset left, Offset right) => left.Value > right.Value;
    /// <summary>Less than or equal operator</summary>
    public static bool operator <=(Offset left, Offset right) => left.Value <= right.Value;
    /// <summary>Greater than or equal operator</summary>
    public static bool operator >=(Offset left, Offset right) => left.Value >= right.Value;

    /// <summary>Implicit conversion to long</summary>
    public static implicit operator long(Offset offset) => offset.Value;
    /// <summary>Implicit conversion from long</summary>
    public static implicit operator Offset(long value) => new(value);
}