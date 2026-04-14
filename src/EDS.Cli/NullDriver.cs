using EDS.Core.Abstractions;
using EDS.Core.Models;
using Microsoft.Extensions.Logging;

namespace EDS.Cli;

/// <summary>
/// No-op driver used in <c>--dry-run</c> mode.
/// Every event is logged at INFO level but nothing is written to a destination.
/// Flush is also a no-op — the pending count is reset and logged so operators can
/// verify event flow and schema decode without touching a real database or storage backend.
/// </summary>
internal sealed class NullDriver : IDriver
{
    private int _pending;

    public int MaxBatchSize => -1;

    public Task StopAsync(CancellationToken ct = default) => Task.CompletedTask;

    public Task<bool> ProcessAsync(ILogger logger, DbChangeEvent evt, CancellationToken ct = default)
    {
        Interlocked.Increment(ref _pending);
        logger.LogInformation(
            "[dry-run] Would process: op={Op} table={Table} id={Id} company={Company}",
            evt.Operation, evt.Table, evt.Id, evt.CompanyId);
        return Task.FromResult(false);
    }

    public Task FlushAsync(ILogger logger, CancellationToken ct = default)
    {
        var count = Interlocked.Exchange(ref _pending, 0);
        logger.LogInformation("[dry-run] Would flush {Count} event(s) — no writes performed.", count);
        return Task.CompletedTask;
    }

    public Task TestAsync(ILogger logger, string url, CancellationToken ct = default) => Task.CompletedTask;

    public IReadOnlyList<DriverField> Configuration() => [];

    public (string url, IReadOnlyList<FieldError> errors) Validate(Dictionary<string, object?> values) => ("", []);
}
