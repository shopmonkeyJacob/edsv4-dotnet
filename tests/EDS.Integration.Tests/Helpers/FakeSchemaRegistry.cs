using EDS.Core.Abstractions;
using EDS.Core.Models;

namespace EDS.Integration.Tests.Helpers;

/// <summary>
/// In-memory ISchemaRegistry backed by a fixed SchemaMap.
/// Used in driver tests to avoid a real API or SQLite dependency.
/// </summary>
internal sealed class FakeSchemaRegistry : ISchemaRegistry
{
    private readonly SchemaMap _schemas;

    public FakeSchemaRegistry(SchemaMap schemas) => _schemas = schemas;

    public Task<SchemaMap> GetLatestSchemaAsync(CancellationToken ct = default) =>
        Task.FromResult(_schemas);

    public Task<Schema?> GetSchemaAsync(string table, string version, CancellationToken ct = default)
    {
        _schemas.TryGetValue(table, out var schema);
        return Task.FromResult(schema);
    }

    public Task<(bool found, string version)> GetTableVersionAsync(string table, CancellationToken ct = default) =>
        Task.FromResult((false, string.Empty));

    public Task SetTableVersionAsync(string table, string version, CancellationToken ct = default) =>
        Task.CompletedTask;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
