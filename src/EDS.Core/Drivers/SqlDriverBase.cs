using EDS.Core.Abstractions;
using EDS.Core.Helpers;
using EDS.Core.Models;
using Microsoft.Extensions.Logging;
using System.Data.Common;
using System.Text;
using System.Text.Json;

namespace EDS.Core.Drivers;

/// <summary>
/// Abstract base class for all relational SQL drivers.
/// Provides shared implementations of ProcessAsync, FlushAsync, EnsureTablesAsync,
/// RefreshSchemaAsync, MigrateNewTableAsync, and MigrateNewColumnsAsync.
/// Derived classes supply dialect-specific quoting, type mapping, and SQL syntax.
/// </summary>
public abstract class SqlDriverBase : IDriver, IDriverLifecycle, IDriverMigration, IDriverImport
{
    // ── Shared state ──────────────────────────────────────────────────────────
    protected ISchemaRegistry? Registry { get; private set; }
    protected ILogger? Logger { get; private set; }
    protected DatabaseSchema DbSchema = new();

    private readonly StringBuilder _pending = new();
    private int _count;
    private string _dbName = string.Empty;

    public int MaxBatchSize => -1;

    // ── Abstract: dialect-specific ────────────────────────────────────────────

    /// <summary>Opens and returns a ready-to-use connection.</summary>
    protected abstract Task<DbConnection> OpenConnectionAsync(CancellationToken ct);

    /// <summary>SQL to retrieve the current database name (e.g. SELECT DATABASE()).</summary>
    protected abstract string GetDatabaseNameSql();

    /// <summary>SQL to query information_schema for TABLE_NAME, COLUMN_NAME, DATA_TYPE filtered by @db.</summary>
    protected abstract string GetSchemaInfoSql();

    /// <summary>Adds the @db parameter to a schema-query command.</summary>
    protected abstract void AddDbNameParameter(DbCommand cmd, string dbName);

    /// <summary>Builds the full INSERT/UPSERT/MERGE or DELETE SQL for one event.</summary>
    protected abstract string BuildSql(DbChangeEvent evt, Schema schema);

    /// <summary>CREATE TABLE (IF NOT EXISTS) DDL for a schema — used by EnsureTablesAsync.</summary>
    protected abstract string BuildCreateTableIfNotExistsSql(Schema schema);

    /// <summary>DROP + CREATE DDL for a schema — used by MigrateNewTableAsync.</summary>
    protected abstract string BuildDropAndCreateTableSql(Schema schema);

    /// <summary>ALTER TABLE … ADD COLUMN DDL — used by MigrateNewColumnsAsync.</summary>
    protected abstract string BuildAlterAddColumnSql(string table, string col, string sqlType);

    /// <summary>Maps a SchemaProperty to its SQL column type for this dialect.</summary>
    protected abstract string PropToSqlType(SchemaProperty prop, bool isPrimaryKey = false);

    /// <summary>Quotes an identifier (table or column name) for this dialect.</summary>
    protected abstract string QuoteId(string name);

    /// <summary>Tests connectivity with the given URL.</summary>
    public abstract Task TestAsync(ILogger logger, string url, CancellationToken ct = default);

    /// <summary>Returns the configuration field definitions for this driver.</summary>
    public abstract IReadOnlyList<DriverField> Configuration();

    /// <summary>Validates a configuration map and returns the constructed URL and any field errors.</summary>
    public abstract (string url, IReadOnlyList<FieldError> errors) Validate(Dictionary<string, object?> values);

    // ── Virtual: dialect overrides ────────────────────────────────────────────

    /// <summary>Driver-specific initialisation (e.g. build connection string / data source).</summary>
    protected virtual void InitialiseDriver(DriverConfig config) { }

    /// <summary>SQL literal for boolean TRUE. Override for dialects that use TRUE/FALSE.</summary>
    protected virtual string SqlTrue => "1";

    /// <summary>SQL literal for boolean FALSE. Override for dialects that use TRUE/FALSE.</summary>
    protected virtual string SqlFalse => "0";

    /// <summary>Wraps a string value as a SQL literal. Override for Unicode prefixes (N'...').</summary>
    protected virtual string QuoteString(string value) =>
        "'" + value.Replace("'", "''") + "'";

    // ── IDriverLifecycle ──────────────────────────────────────────────────────

    public async Task StartAsync(DriverConfig config, CancellationToken ct = default)
    {
        Logger = config.Logger;
        Registry = config.SchemaRegistry;
        InitialiseDriver(config);
        await using var conn = await OpenConnectionAsync(ct);
        await RefreshSchemaAsync(conn, ct);
        await EnsureTablesAsync(conn, ct);
    }

    public virtual Task StopAsync(CancellationToken ct = default) => Task.CompletedTask;

    // ── IDriver ───────────────────────────────────────────────────────────────

    public async Task<bool> ProcessAsync(ILogger logger, DbChangeEvent evt, CancellationToken ct = default)
    {
        var schema = await Registry!.GetSchemaAsync(evt.Table, evt.ModelVersion, ct)
            ?? throw new InvalidOperationException($"Schema not found for {evt.Table} v{evt.ModelVersion}");
        var sql = BuildSql(evt, schema);
        logger.LogDebug("[{Driver}] Queuing {Op} on {Table}", GetType().Name, evt.Operation, evt.Table);
        _pending.Append(sql);
        _count++;
        return false;
    }

    public async Task FlushAsync(ILogger logger, CancellationToken ct = default)
    {
        if (_count == 0) return;
        var sql = _pending.ToString();
        logger.LogInformation("[{Driver}] Flushing {Count} statement(s)", GetType().Name, _count);

        await using var conn = await OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        try
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Transaction = tx;
            await cmd.ExecuteNonQueryAsync(ct);
            await tx.CommitAsync(ct);
            logger.LogInformation("[{Driver}] Committed {Count} statement(s)", GetType().Name, _count);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{Driver}] Transaction failed, rolling back. SQL: {Sql}",
                GetType().Name, sql.Length > 500 ? sql[..500] + "…" : sql);
            await tx.RollbackAsync(ct);
            throw;
        }
        finally
        {
            _pending.Clear();
            _count = 0;
        }
    }

    // ── IDriverMigration ──────────────────────────────────────────────────────

    public async Task MigrateNewTableAsync(ILogger logger, Schema schema, CancellationToken ct = default)
    {
        logger.LogWarning("[{Driver}] Dropping and recreating table {Table} — existing data will be lost.",
            GetType().Name, schema.Table);
        await using var conn = await OpenConnectionAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = BuildDropAndCreateTableSql(schema);
        await cmd.ExecuteNonQueryAsync(ct);
        await RefreshSchemaAsync(conn, ct);
    }

    public async Task MigrateNewColumnsAsync(ILogger logger, Schema schema, IReadOnlyList<string> newColumns, CancellationToken ct = default)
    {
        await using var conn = await OpenConnectionAsync(ct);
        foreach (var col in newColumns)
        {
            if (DbSchema.TryGetValue(schema.Table, out var cols) && cols.ContainsKey(col)) continue;
            var prop = schema.Properties[col];
            var sql = BuildAlterAddColumnSql(schema.Table, col, PropToSqlType(prop, isPrimaryKey: false));
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync(ct);
        }
        await RefreshSchemaAsync(conn, ct);
    }

    // ── Shared internals ──────────────────────────────────────────────────────

    protected async Task RefreshSchemaAsync(DbConnection conn, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(_dbName))
        {
            await using var nameCmd = conn.CreateCommand();
            nameCmd.CommandText = GetDatabaseNameSql();
            _dbName = (string)(await nameCmd.ExecuteScalarAsync(ct) ?? string.Empty);
        }

        DbSchema = new DatabaseSchema();
        await using var schCmd = conn.CreateCommand();
        schCmd.CommandText = GetSchemaInfoSql();
        AddDbNameParameter(schCmd, _dbName);
        await using var reader = await schCmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var table = reader.GetString(0);
            var col   = reader.GetString(1);
            var type  = reader.GetString(2);
            if (!DbSchema.ContainsKey(table)) DbSchema[table] = new Dictionary<string, string>();
            DbSchema[table][col] = type;
        }
    }

    private async Task EnsureTablesAsync(DbConnection conn, CancellationToken ct)
    {
        if (Registry is null) return;
        var schemas = await Registry.GetLatestSchemaAsync(ct);
        if (schemas.Count == 0) return;

        // Batch all CREATE IF NOT EXISTS into a single round trip
        var sb = new StringBuilder();
        int toCreate = 0;
        foreach (var (_, schema) in schemas)
        {
            if (DbSchema.ContainsKey(schema.Table)) continue;
            sb.AppendLine(BuildCreateTableIfNotExistsSql(schema));
            toCreate++;
        }

        if (toCreate > 0)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sb.ToString();
            await cmd.ExecuteNonQueryAsync(ct);
            await RefreshSchemaAsync(conn, ct);
            Logger!.LogInformation("[{Driver}] Created {Count} table(s) from Shopmonkey schema.",
                GetType().Name, toCreate);
        }
        else
        {
            Logger!.LogInformation("[{Driver}] All {Count} schema tables already exist.",
                GetType().Name, schemas.Count);
        }
    }

    // ── IDriverImport / IImportHandler ────────────────────────────────────────

    private const int ImportBatchSize = 500;

    /// <summary>
    /// Initialises the driver for bulk import: opens a connection and reflects the
    /// current database schema, but skips the startup table-creation sweep.
    /// </summary>
    public async Task InitForImportAsync(
        ILogger logger, ISchemaRegistry registry, string url, CancellationToken ct = default)
    {
        Logger   = logger;
        Registry = registry;
        InitialiseDriver(new DriverConfig
        {
            Url            = url,
            Logger         = logger,
            SchemaRegistry = registry,
            Tracker        = null!,   // not used during import
            DataDir        = string.Empty,
        });
        await using var conn = await OpenConnectionAsync(ct);
        await RefreshSchemaAsync(conn, ct);
    }

    /// <summary>Drops and recreates the table — called once per table before rows arrive.</summary>
    public Task CreateDatasourceAsync(Schema schema, CancellationToken ct = default) =>
        MigrateNewTableAsync(Logger!, schema, ct);

    /// <summary>
    /// Appends one event to the pending batch and flushes when <see cref="ImportBatchSize"/> is reached.
    /// Builds SQL directly from the supplied schema to avoid a registry round-trip per row.
    /// </summary>
    public async Task ImportEventAsync(DbChangeEvent evt, Schema schema, CancellationToken ct = default)
    {
        _pending.Append(BuildSql(evt, schema));
        _count++;
        if (_count >= ImportBatchSize)
            await FlushAsync(Logger!, ct);
    }

    /// <summary>Flushes any remaining rows after all events for a table have been delivered.</summary>
    public async Task ImportCompletedAsync(string table, long rowCount, CancellationToken ct = default)
    {
        if (_count > 0)
            await FlushAsync(Logger!, ct);
        Logger!.LogInformation("[{Driver}] Imported {Rows} row(s) into {Table}.",
            GetType().Name, rowCount, table);
    }

    // ── Common SQL helpers available to all derived drivers ───────────────────

    protected string QuoteLiteral(string value) => QuoteString(value);

    protected string QuoteJsonElement(JsonElement el) => el.ValueKind switch
    {
        JsonValueKind.Null or JsonValueKind.Undefined => "NULL",
        JsonValueKind.True                            => SqlTrue,
        JsonValueKind.False                           => SqlFalse,
        JsonValueKind.Number                          => SqlHelpers.IsValidNumericLiteral(el.GetRawText())
                                                            ? el.GetRawText()
                                                            : "NULL",
        JsonValueKind.String                          => QuoteString(el.GetString() ?? string.Empty),
        _                                             => QuoteString(el.GetRawText())
    };

    protected static string BuildColumnList(IReadOnlyList<string> cols, Func<string, string> quoteId) =>
        string.Join(", ", cols.Select(quoteId));

    protected string BuildCreateTableBody(Schema schema)
    {
        var sb = new StringBuilder();
        foreach (var col in schema.Columns())
        {
            var prop   = schema.Properties[col];
            var isPk   = schema.PrimaryKeys.Contains(col);
            var notNull = schema.Required.Contains(col) && prop.IsNotNull ? " NOT NULL" : string.Empty;
            sb.AppendLine($"  {QuoteId(col)} {PropToSqlType(prop, isPk)}{notNull},");
        }
        if (schema.PrimaryKeys.Count > 0)
        {
            var pks = string.Join(", ", schema.PrimaryKeys.Select(QuoteId));
            sb.Append($"  PRIMARY KEY ({pks})");
        }
        else
        {
            sb.Append($"  PRIMARY KEY ({QuoteId("id")})");
        }
        return sb.ToString();
    }
}
