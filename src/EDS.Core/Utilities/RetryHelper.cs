using Microsoft.Extensions.Logging;
using System.Net.Sockets;

namespace EDS.Core.Utilities;

/// <summary>
/// Executes an async operation with exponential backoff retry for transient errors.
/// Retries on network errors (HttpRequestException, SocketException, IO failures).
/// Never retries on cancellation or non-transient HTTP errors (4xx except 429).
/// </summary>
public static class RetryHelper
{
    private static readonly TimeSpan[] Delays =
    [
        TimeSpan.FromSeconds(2),
        TimeSpan.FromSeconds(4),
        TimeSpan.FromSeconds(8),
        TimeSpan.FromSeconds(16),
        TimeSpan.FromSeconds(30),
    ];

    public static async Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> action,
        ILogger? logger = null,
        string operationName = "operation",
        int maxAttempts = 5,
        CancellationToken ct = default)
    {
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                return await action(ct);
            }
            catch (Exception ex) when (IsTransient(ex) && attempt < maxAttempts && !ct.IsCancellationRequested)
            {
                var delay = Delays[Math.Min(attempt - 1, Delays.Length - 1)];
                logger?.LogWarning("[retry] {Op} failed (attempt {Attempt}/{Max}): {Error}. Retrying in {Delay}s.",
                    operationName, attempt, maxAttempts, ex.Message, (int)delay.TotalSeconds);
                await Task.Delay(delay, ct);
            }
        }
    }

    public static Task ExecuteAsync(
        Func<CancellationToken, Task> action,
        ILogger? logger = null,
        string operationName = "operation",
        int maxAttempts = 5,
        CancellationToken ct = default) =>
        ExecuteAsync<object?>(async innerCt => { await action(innerCt); return null; },
            logger, operationName, maxAttempts, ct);

    public static bool IsTransient(Exception ex) => ex switch
    {
        HttpRequestException                                         => true,
        SocketException                                              => true,
        IOException                                                  => true,
        TaskCanceledException { InnerException: TimeoutException }   => true,
        _                                                            => false,
    };
}
