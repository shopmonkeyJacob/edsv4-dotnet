using EDS.Core.Models;

namespace EDS.Core.Abstractions;

public interface ISchemaRegistry : IAsyncDisposable
{
    /// <summary>Returns the latest schema for all known tables.</summary>
    Task<SchemaMap> GetLatestSchemaAsync(CancellationToken ct = default);

    /// <summary>Returns the schema for a specific table and model version.</summary>
    Task<Schema?> GetSchemaAsync(string table, string version, CancellationToken ct = default);

    /// <summary>
    /// Gets the currently tracked schema version for a table.
    /// Returns (found, version).
    /// </summary>
    Task<(bool found, string version)> GetTableVersionAsync(string table, CancellationToken ct = default);

    /// <summary>Persists the current schema version for a table.</summary>
    Task SetTableVersionAsync(string table, string version, CancellationToken ct = default);
}
