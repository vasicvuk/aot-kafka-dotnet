namespace AotKafka;

/// <summary>
/// Represents a Kafka partition
/// </summary>
public readonly struct Partition : IEquatable<Partition>
{
    /// <summary>
    /// Unassigned partition
    /// </summary>
    public static readonly Partition Unassigned = new(-1);

    /// <summary>
    /// Any partition (used in produce)
    /// </summary>
    public static readonly Partition Any = new(-1);

    /// <summary>
    /// Gets the partition value
    /// </summary>
    public int Value { get; init; }

    /// <summary>
    /// Creates a new partition instance
    /// </summary>
    public Partition(int value)
    {
        Value = value;
    }

    /// <inheritdoc/>
    public bool Equals(Partition other) => Value == other.Value;
    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is Partition other && Equals(other);
    /// <inheritdoc/>
    public override int GetHashCode() => Value.GetHashCode();
    /// <inheritdoc/>
    public override string ToString() => Value.ToString();

    /// <summary>Equality operator</summary>
    public static bool operator ==(Partition left, Partition right) => left.Equals(right);
    /// <summary>Inequality operator</summary>
    public static bool operator !=(Partition left, Partition right) => !left.Equals(right);

    /// <summary>Implicit conversion to int</summary>
    public static implicit operator int(Partition partition) => partition.Value;
    /// <summary>Implicit conversion from int</summary>
    public static implicit operator Partition(int value) => new(value);
}