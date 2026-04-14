using EDS.Core.Models;

namespace EDS.Core.Abstractions;

/// <summary>
/// Persists CDC events that could not be delivered to the destination after all
/// retry attempts, so operators can inspect and replay them.
/// </summary>
public interface IDlqWriter
{
    Task PushAsync(
        IEnumerable<DbChangeEvent> events,
        string error,
        int retryCount,
        CancellationToken ct = default);
}
