using System.Collections;

namespace AotKafka;

/// <summary>
/// Represents a message header
/// </summary>
public interface IHeader
{
    /// <summary>
    /// Header key
    /// </summary>
    public string Key { get; }
    /// <summary>
    /// Header value
    /// </summary>
    public byte[] Value { get; }
}

/// <summary>
/// Message header implementation
/// </summary>
public class Header : IHeader
{
    /// <summary>
    /// Header key
    /// </summary>
    public string Key { get; init; } = null!;
    /// <summary>
    /// Header value
    /// </summary>
    public byte[] Value { get; init; } = Array.Empty<byte>();
}

/// <summary>
/// Collection of message headers
/// </summary>
public class Headers : IEnumerable<IHeader>
{
    private readonly List<IHeader> _headers = new();
    private readonly object _lock = new();

    /// <summary>
    /// Add a header
    /// </summary>
    public void Add(string key, byte[] value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(value);

        lock (_lock)
        {
            _headers.Add(new Header { Key = key, Value = value });
        }
    }

    /// <summary>
    /// Add a header
    /// </summary>
    public void Add(IHeader header)
    {
        ArgumentNullException.ThrowIfNull(header);

        lock (_lock)
        {
            _headers.Add(header);
        }
    }

    /// <summary>
    /// Get enumerator
    /// </summary>
    public IEnumerator<IHeader> GetEnumerator()
    {
        lock (_lock)
        {
            return new List<IHeader>(_headers).GetEnumerator();
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}