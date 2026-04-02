namespace EDS.Core.Abstractions;

public interface ITracker : IAsyncDisposable
{
    /// <summary>Returns the value for the key, or null if not found.</summary>
    Task<string?> GetKeyAsync(string key, CancellationToken ct = default);

    /// <summary>Sets a key to a value.</summary>
    Task SetKeyAsync(string key, string value, CancellationToken ct = default);

    /// <summary>Sets multiple keys atomically.</summary>
    Task SetKeysAsync(IReadOnlyDictionary<string, string> pairs, CancellationToken ct = default);

    /// <summary>Deletes one or more keys.</summary>
    Task DeleteKeyAsync(string key, CancellationToken ct = default);

    /// <summary>Deletes all keys that start with the given prefix. Returns number deleted.</summary>
    Task<int> DeleteKeysWithPrefixAsync(string prefix, CancellationToken ct = default);
}
