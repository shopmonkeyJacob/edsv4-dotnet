using EDS.Core.Abstractions;
using EDS.Core.Models;
using Microsoft.Extensions.Logging;
using System.Text.Json;

using CoreSchema = EDS.Core.Models.Schema;

namespace EDS.Infrastructure.Schema;

/// <summary>
/// Schema registry backed by the SQLite tracker.
/// Stores table → schema version mappings persistently and
/// loads full schema definitions from embedded JSON at startup.
/// </summary>
public sealed class SqliteSchemaRegistry : ISchemaRegistry
{
    private readonly ITracker _tracker;
    private readonly ILogger<SqliteSchemaRegistry> _logger;
    private readonly SchemaMap _schemas;

    private const string TableVersionPrefix = "schema:version:";

    public SqliteSchemaRegistry(
        ITracker tracker,
        ILogger<SqliteSchemaRegistry> logger,
        SchemaMap? preloadedSchemas = null)
    {
        _tracker = tracker;
        _logger = logger;
        _schemas = preloadedSchemas ?? new SchemaMap();
    }

    /// <summary>
    /// Loads schema definitions from a directory of JSON files.
    /// Each file should be named {table}.json and contain a Schema object.
    /// </summary>
    public static SchemaMap LoadSchemasFromDirectory(string directory, ILogger? logger = null)
    {
        var map = new SchemaMap();
        if (!Directory.Exists(directory))
            return map;

        foreach (var file in Directory.EnumerateFiles(directory, "*.json"))
        {
            try
            {
                var json = File.ReadAllText(file);
                var schema = JsonSerializer.Deserialize<CoreSchema>(json);
                if (schema is not null)
                    map[schema.Table] = schema;
            }
            catch (Exception ex)
            {
                // Log and skip bad schema files
                logger?.LogWarning(ex, "Failed to load schema from {File}", file);
            }
        }
        return map;
    }

    public Task<SchemaMap> GetLatestSchemaAsync(CancellationToken ct = default)
    {
        return Task.FromResult(_schemas);
    }

    public Task<CoreSchema?> GetSchemaAsync(string table, string version, CancellationToken ct = default)
    {
        if (_schemas.TryGetValue(table, out var schema) && schema.ModelVersion == version)
            return Task.FromResult<CoreSchema?>(schema);
        return Task.FromResult<CoreSchema?>(null);
    }

    public async Task<(bool found, string version)> GetTableVersionAsync(string table, CancellationToken ct = default)
    {
        var value = await _tracker.GetKeyAsync(TableVersionPrefix + table, ct);
        return value is null ? (false, string.Empty) : (true, value);
    }

    public Task SetTableVersionAsync(string table, string version, CancellationToken ct = default)
    {
        return _tracker.SetKeyAsync(TableVersionPrefix + table, version, ct);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
