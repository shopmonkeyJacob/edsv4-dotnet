using EDS.Core.Models;
using Microsoft.Extensions.Logging;

namespace EDS.Core.Abstractions;

/// <summary>
/// Thrown when an operation is called after the driver has been stopped.
/// </summary>
public sealed class DriverStoppedException : InvalidOperationException
{
    public DriverStoppedException() : base("Driver has been stopped.") { }
}

public enum DriverFieldType { String, Number, Boolean }
public enum DriverFieldFormat { None, Password }

public sealed class DriverField
{
    public required string Name { get; init; }
    public DriverFieldType Type { get; init; } = DriverFieldType.String;
    public DriverFieldFormat Format { get; init; } = DriverFieldFormat.None;
    public string? Default { get; init; }
    public required string Description { get; init; }
    public bool Required { get; init; }
}

public sealed class FieldError
{
    public required string Field { get; init; }
    public required string Message { get; init; }
    public override string ToString() => $"{Field}: {Message}";
}

public sealed class ValidationResult
{
    public bool IsValid => Errors.Count == 0;
    public IReadOnlyList<FieldError> Errors { get; init; } = [];

    public static ValidationResult Success { get; } = new() { Errors = [] };
    public static ValidationResult Failure(IReadOnlyList<FieldError> errors) => new() { Errors = errors };
    public static ValidationResult FailureForField(string field, string message) =>
        Failure([new FieldError { Field = field, Message = message }]);
}

/// <summary>
/// Core driver interface — must be implemented by every driver.
/// </summary>
public interface IDriver
{
    /// <summary>Stops the driver and releases any resources.</summary>
    Task StopAsync(CancellationToken ct = default);

    /// <summary>
    /// Maximum number of events before Flush should be called. Return -1 for no limit.
    /// </summary>
    int MaxBatchSize { get; }

    /// <summary>
    /// Process a single event. Returns true if Flush should be called immediately.
    /// Throw to signal a NAK on the NATS message.
    /// </summary>
    Task<bool> ProcessAsync(ILogger logger, DbChangeEvent evt, CancellationToken ct = default);

    /// <summary>
    /// Commit all pending events. Throw to NAK all pending messages.
    /// </summary>
    Task FlushAsync(ILogger logger, CancellationToken ct = default);

    /// <summary>Test connectivity with the given URL.</summary>
    Task TestAsync(ILogger logger, string url, CancellationToken ct = default);

    /// <summary>Returns the configuration field definitions for this driver.</summary>
    IReadOnlyList<DriverField> Configuration();

    /// <summary>
    /// Validates a configuration map. Returns the constructed URL and any field errors.
    /// </summary>
    (string url, IReadOnlyList<FieldError> errors) Validate(Dictionary<string, object?> values);
}

/// <summary>
/// Optional lifecycle hook — called once before the first event is processed.
/// </summary>
public interface IDriverLifecycle
{
    Task StartAsync(DriverConfig config, CancellationToken ct = default);
}

/// <summary>
/// Optional help/metadata — used by the CLI help system.
/// </summary>
public interface IDriverHelp
{
    string Name { get; }
    string Description { get; }
    string ExampleUrl { get; }
    string Help { get; }
}

/// <summary>
/// Optional schema migration support.
/// </summary>
public interface IDriverMigration
{
    Task MigrateNewTableAsync(ILogger logger, Schema schema, CancellationToken ct = default);
    Task MigrateNewColumnsAsync(ILogger logger, Schema schema, IReadOnlyList<string> newColumns, CancellationToken ct = default);

    /// <summary>
    /// Alters the type of columns that have changed in the HQ schema.
    /// Default: no-op (override in drivers that support ALTER COLUMN).
    /// </summary>
    Task MigrateChangedColumnsAsync(ILogger logger, Schema schema, IReadOnlyList<string> changedColumns, CancellationToken ct = default) =>
        Task.CompletedTask;

    /// <summary>
    /// Drops columns that have been removed from the HQ schema.
    /// Default: no-op (override in drivers that support DROP COLUMN).
    /// </summary>
    Task MigrateRemovedColumnsAsync(ILogger logger, Schema schema, IReadOnlyList<string> removedColumns, CancellationToken ct = default) =>
        Task.CompletedTask;
}

/// <summary>
/// Optional additional URL scheme aliases for a driver.
/// </summary>
public interface IDriverAlias
{
    IReadOnlyList<string> Aliases { get; }
}

/// <summary>
/// Optional — allows the driver to receive the current session ID.
/// </summary>
public interface IDriverSessionHandler
{
    void SetSessionId(string sessionId);
}

/// <summary>
/// Optional — driver supports bulk import from CRDB NDJSON export files.
/// Implement alongside <see cref="IImportHandler"/> to enable the import command.
/// </summary>
public interface IDriverImport : IImportHandler
{
    /// <summary>
    /// Initialises the driver for import use (opens connection, reflects schema)
    /// without running the full startup table-creation scan.
    /// </summary>
    Task InitForImportAsync(
        ILogger logger,
        ISchemaRegistry registry,
        string url,
        DriverMode mode = DriverMode.Upsert,
        string eventsSchema = "eds_events",
        CancellationToken ct = default);

    /// <summary>
    /// Drops tables that exist in the destination database but are not in
    /// <paramref name="knownTables"/> (the current HQ schema). Called before
    /// table recreation on a fresh (non-resume) import so the DB is fully in sync.
    /// Default: no-op.
    /// </summary>
    Task DropOrphanTablesAsync(ILogger logger, IReadOnlySet<string> knownTables, CancellationToken ct = default) =>
        Task.CompletedTask;
}

/// <summary>
/// Optional — driver can receive raw CRDB export files (.ndjson.gz) directly,
/// bypassing the row-by-row parse pipeline. Implement on storage drivers (File, S3)
/// where the export format is already the natural destination format.
/// </summary>
public interface IDriverDirectImport
{
    /// <summary>
    /// Prepares the driver for direct file transfer (e.g. opens credentials,
    /// resolves the destination path) using the same URL format as normal operation.
    /// </summary>
    Task InitForDirectImportAsync(ILogger logger, string url, CancellationToken ct = default);

    /// <summary>
    /// Transfers each file in <paramref name="files"/> to the driver's destination
    /// as-is, preserving the CRDB filename. Each entry is a (tableName, absoluteFilePath)
    /// pair already filtered by --only. No parsing or transformation is performed.
    /// </summary>
    Task ImportFilesAsync(
        ILogger logger,
        IReadOnlyList<(string Table, string FilePath)> files,
        CancellationToken ct = default);
}
