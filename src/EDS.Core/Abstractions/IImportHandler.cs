using EDS.Core.Models;

namespace EDS.Core.Abstractions;

public interface IImportHandler
{
    /// <summary>Called once before any rows arrive for a table.</summary>
    Task CreateDatasourceAsync(Schema schema, CancellationToken ct = default);

    /// <summary>Called for each row during import.</summary>
    Task ImportEventAsync(DbChangeEvent evt, Schema schema, CancellationToken ct = default);

    /// <summary>Called after all rows for a table have been delivered.</summary>
    Task ImportCompletedAsync(string table, long rowCount, CancellationToken ct = default);
}
