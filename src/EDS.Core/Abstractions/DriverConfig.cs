using Microsoft.Extensions.Logging;

namespace EDS.Core.Abstractions;

/// <summary>
/// Configuration passed to a driver at startup via IDriverLifecycle.StartAsync.
/// </summary>
public sealed class DriverConfig
{
    public required string Url { get; init; }
    public required ILogger Logger { get; init; }
    public required ISchemaRegistry SchemaRegistry { get; init; }
    public required ITracker Tracker { get; init; }
    public required string DataDir { get; init; }
}
