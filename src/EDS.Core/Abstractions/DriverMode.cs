namespace EDS.Core.Abstractions;

/// <summary>
/// Determines how SQL drivers write incoming CDC events.
/// </summary>
public enum DriverMode
{
    /// <summary>
    /// Default: each event is upserted (or deleted) in a mirror of the source table.
    /// The destination always reflects the latest state of the source.
    /// </summary>
    Upsert,

    /// <summary>
    /// Append-only: every CDC event is inserted as a new row into a fixed-schema
    /// events table (e.g. eds_events.{table}_events). No rows are ever updated or deleted.
    /// Automatically maintained views provide a current-state snapshot and full audit history.
    /// </summary>
    TimeSeries,
}
