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

    /// <summary>How this driver instance writes events.</summary>
    protected DriverMode Mode { get; private set; } = DriverMode.Upsert;

    /// <summary>Database schema name for time-series events tables.</summary>
    protected string EventsSchema { get; private set; } = "eds_events";

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

    /// <summary>Builds the full INSERT/UPSERT/MERGE or DELETE SQL for one event (upsert mode).</summary>
    protected abstract string BuildSql(DbChangeEvent evt, Schema schema);

    /// <summary>CREATE TABLE (IF NOT EXISTS) DDL for a schema — used by EnsureTablesAsync.</summary>
    protected abstract string BuildCreateTableIfNotExistsSql(Schema schema);

    /// <summary>DROP + CREATE DDL for a schema — used by MigrateNewTableAsync.</summary>
    protected abstract string BuildDropAndCreateTableSql(Schema schema);

    /// <summary>ALTER TABLE … ADD COLUMN DDL — used by MigrateNewColumnsAsync.</summary>
    protected abstract string BuildAlterAddColumnSql(string table, string col, string sqlType);

    /// <summary>
    /// ALTER TABLE … ALTER/MODIFY COLUMN DDL — used by MigrateChangedColumnsAsync.
    /// Return an empty string to skip this dialect (default: no-op).
    /// </summary>
    protected virtual string BuildAlterColumnTypeSql(string table, string col, string sqlType) =>
        string.Empty;

    /// <summary>
    /// ALTER TABLE … DROP COLUMN DDL — used by MigrateRemovedColumnsAsync.
    /// Default: standard SQL syntax shared by MySQL and PostgreSQL.
    /// </summary>
    protected virtual string BuildDropColumnSql(string table, string col) =>
        $"ALTER TABLE {QuoteId(table)} DROP COLUMN {QuoteId(col)};";

    /// <summary>
    /// DROP TABLE DDL — used by DropOrphanTablesAsync.
    /// Default: standard IF NOT EXISTS syntax. Override for dialects that differ (e.g. SQL Server).
    /// </summary>
    protected virtual string BuildDropTableSql(string table) =>
        $"DROP TABLE IF EXISTS {QuoteId(table)};";

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

    // ── Virtual: time-series dialect overrides ────────────────────────────────

    /// <summary>
    /// SQL to ensure the events schema exists.
    /// Return an empty string for dialects that have no schema concept (e.g. MySQL).
    /// </summary>
    protected virtual string GetEnsureEventsSchemaSql(string schemaName) =>
        $"CREATE SCHEMA IF NOT EXISTS {QuoteId(schemaName)}";

    /// <summary>Qualifies an events table name with the events schema.</summary>
    protected virtual string QualifyEventsTable(string table, string eventsSchema) =>
        $"{QuoteId(eventsSchema)}.{QuoteId(table + "_events")}";

    /// <summary>Qualifies a view name with the events schema.</summary>
    protected virtual string QualifyEventsView(string viewName, string eventsSchema) =>
        $"{QuoteId(eventsSchema)}.{QuoteId(viewName)}";

    /// <summary>
    /// Extracts a JSON field value as text.
    /// Default uses PostgreSQL/standard JSONB ->> operator. Override for other dialects.
    /// </summary>
    protected virtual string JsonExtract(string column, string field) =>
        $"{column}->>{QuoteString(field)}";

    /// <summary>SQL column definition for the auto-increment _seq primary key.</summary>
    protected virtual string GetAutoIncrementPkDef() => "BIGSERIAL PRIMARY KEY";

    /// <summary>SQL column type for JSON/JSONB blob storage.</summary>
    protected virtual string GetJsonColumnType() => "JSONB";

    /// <summary>
    /// Builds a CREATE OR REPLACE VIEW statement.
    /// Override for dialects that use different syntax (e.g. SQL Server's CREATE OR ALTER VIEW).
    /// </summary>
    protected virtual string BuildCreateOrReplaceViewSql(string qualifiedViewName, string selectSql) =>
        $"CREATE OR REPLACE VIEW {qualifiedViewName} AS\n{selectSql}";

    /// <summary>
    /// DDL to ensure the events table exists with the fixed events schema.
    /// Override for dialects that lack CREATE TABLE IF NOT EXISTS (e.g. SQL Server).
    /// </summary>
    protected virtual string BuildEnsureEventsTableSql(string table)
    {
        var qt = QualifyEventsTable(table, EventsSchema);
        var jt = GetJsonColumnType();
        var pk = GetAutoIncrementPkDef();
        return $"""
            CREATE TABLE IF NOT EXISTS {qt} (
              _seq         {pk},
              _event_id    TEXT,
              _operation   TEXT NOT NULL,
              _entity_id   TEXT,
              _timestamp   BIGINT,
              _mvcc_ts     TEXT,
              _company_id  TEXT,
              _location_id TEXT,
              _model_ver   TEXT,
              _diff        TEXT,
              _before      {jt},
              _after       {jt}
            )
            """;
    }

    // ── IDriverLifecycle ──────────────────────────────────────────────────────

    public async Task StartAsync(DriverConfig config, CancellationToken ct = default)
    {
        Logger       = config.Logger;
        Registry     = config.SchemaRegistry;
        Mode         = config.Mode;
        EventsSchema = config.EventsSchema;
        InitialiseDriver(config);
        await using var conn = await OpenConnectionAsync(ct);
        await RefreshSchemaAsync(conn, ct);
        await EnsureTablesAsync(conn, ct);
    }

    public virtual Task StopAsync(CancellationToken ct = default) => Task.CompletedTask;

    // ── IDriver ───────────────────────────────────────────────────────────────

    public async Task<bool> ProcessAsync(ILogger logger, DbChangeEvent evt, CancellationToken ct = default)
    {
        string sql;
        if (Mode == DriverMode.TimeSeries)
        {
            sql = BuildInsertEventSql(evt);
        }
        else
        {
            var schema = await Registry!.GetSchemaAsync(evt.Table, evt.ModelVersion ?? string.Empty, ct)
                ?? throw new InvalidOperationException($"Schema not found for {evt.Table} v{evt.ModelVersion}");
            sql = BuildSql(evt, schema);
        }
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
        if (Mode == DriverMode.TimeSeries)
        {
            // Events table schema is fixed — just ensure it exists and refresh views.
            await using var conn = await OpenConnectionAsync(ct);
            await ExecuteEventsTableAndViewsAsync(conn, schema, ct);
            return;
        }

        logger.LogWarning("[{Driver}] Dropping and recreating table {Table} — existing data will be lost.",
            GetType().Name, schema.Table);
        await using var conn2 = await OpenConnectionAsync(ct);
        await using var cmd = conn2.CreateCommand();
        cmd.CommandText = BuildDropAndCreateTableSql(schema);
        await cmd.ExecuteNonQueryAsync(ct);
        await RefreshSchemaAsync(conn2, ct);
    }

    public async Task MigrateNewColumnsAsync(ILogger logger, Schema schema, IReadOnlyList<string> newColumns, CancellationToken ct = default)
    {
        if (Mode == DriverMode.TimeSeries)
        {
            // Events table schema is fixed — just recreate views to include any new columns.
            await using var conn = await OpenConnectionAsync(ct);
            await using var curViewCmd = conn.CreateCommand();
            curViewCmd.CommandText = BuildCurrentViewSql(schema.Table);
            await curViewCmd.ExecuteNonQueryAsync(ct);
            await using var histViewCmd = conn.CreateCommand();
            histViewCmd.CommandText = BuildHistoryViewSql(schema.Table, schema);
            await histViewCmd.ExecuteNonQueryAsync(ct);
            return;
        }

        await using var conn2 = await OpenConnectionAsync(ct);
        foreach (var col in newColumns)
        {
            if (DbSchema.TryGetValue(schema.Table, out var cols) && cols.ContainsKey(col)) continue;
            var prop = schema.Properties[col];
            var sql = BuildAlterAddColumnSql(schema.Table, col, PropToSqlType(prop, isPrimaryKey: false));
            await using var cmd = conn2.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync(ct);
        }
        await RefreshSchemaAsync(conn2, ct);
    }

    public async Task MigrateChangedColumnsAsync(ILogger logger, Schema schema, IReadOnlyList<string> changedColumns, CancellationToken ct = default)
    {
        if (Mode == DriverMode.TimeSeries) return;

        await using var conn = await OpenConnectionAsync(ct);
        foreach (var col in changedColumns)
        {
            if (!schema.Properties.TryGetValue(col, out var prop)) continue;
            var sql = BuildAlterColumnTypeSql(schema.Table, col, PropToSqlType(prop, isPrimaryKey: false));
            if (string.IsNullOrEmpty(sql)) continue;
            logger.LogInformation("[{Driver}] Altering column type {Column} on {Table}.", GetType().Name, col, schema.Table);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync(ct);
        }
        if (changedColumns.Count > 0)
            await RefreshSchemaAsync(conn, ct);
    }

    public async Task MigrateRemovedColumnsAsync(ILogger logger, Schema schema, IReadOnlyList<string> removedColumns, CancellationToken ct = default)
    {
        if (Mode == DriverMode.TimeSeries) return;

        await using var conn = await OpenConnectionAsync(ct);
        foreach (var col in removedColumns)
        {
            if (DbSchema.TryGetValue(schema.Table, out var dbCols) && !dbCols.ContainsKey(col)) continue;
            var sql = BuildDropColumnSql(schema.Table, col);
            logger.LogInformation("[{Driver}] Dropping removed column {Column} from {Table}.", GetType().Name, col, schema.Table);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync(ct);
        }
        if (removedColumns.Count > 0)
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

        if (Mode == DriverMode.TimeSeries)
        {
            await EnsureEventsTablesAsync(conn, schemas, ct);
            return;
        }

        // Upsert mode: batch all CREATE IF NOT EXISTS into a single round trip
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

    private async Task EnsureEventsTablesAsync(DbConnection conn, SchemaMap schemas, CancellationToken ct)
    {
        // 1. Ensure the events schema exists (no-op for dialects without schemas, e.g. MySQL)
        var schemaSql = GetEnsureEventsSchemaSql(EventsSchema);
        if (!string.IsNullOrEmpty(schemaSql))
        {
            await using var schemaCmd = conn.CreateCommand();
            schemaCmd.CommandText = schemaSql;
            await schemaCmd.ExecuteNonQueryAsync(ct);
        }

        // 2. Ensure each events table and its views exist
        foreach (var (_, schema) in schemas)
            await ExecuteEventsTableAndViewsAsync(conn, schema, ct);

        Logger!.LogInformation("[{Driver}] Ensured {Count} events table(s) and views in schema '{Schema}'.",
            GetType().Name, schemas.Count, EventsSchema);
    }

    /// <summary>
    /// Ensures the events table exists for the given source table and recreates both views.
    /// Each DDL is executed as a separate command so that dialects with per-statement
    /// batch restrictions (e.g. SQL Server's CREATE VIEW must be first in batch) are handled.
    /// </summary>
    private async Task ExecuteEventsTableAndViewsAsync(DbConnection conn, Schema schema, CancellationToken ct)
    {
        await using var tableCmd = conn.CreateCommand();
        tableCmd.CommandText = BuildEnsureEventsTableSql(schema.Table);
        await tableCmd.ExecuteNonQueryAsync(ct);

        await using var curViewCmd = conn.CreateCommand();
        curViewCmd.CommandText = BuildCurrentViewSql(schema.Table);
        await curViewCmd.ExecuteNonQueryAsync(ct);

        await using var histViewCmd = conn.CreateCommand();
        histViewCmd.CommandText = BuildHistoryViewSql(schema.Table, schema);
        await histViewCmd.ExecuteNonQueryAsync(ct);
    }

    // ── Time-series SQL builders ──────────────────────────────────────────────

    private string BuildInsertEventSql(DbChangeEvent evt)
    {
        var qualifiedTable = QualifyEventsTable(evt.Table, EventsSchema);
        var entityId = evt.GetPrimaryKey();
        var diff     = evt.Diff is { Length: > 0 }
            ? QuoteString(JsonSerializer.Serialize(evt.Diff))
            : "NULL";
        var before   = evt.Before.HasValue ? QuoteString(evt.Before.Value.GetRawText()) : "NULL";
        var after    = evt.After.HasValue  ? QuoteString(evt.After.Value.GetRawText())  : "NULL";
        var mvccTs   = string.IsNullOrEmpty(evt.MvccTimestamp) ? "NULL" : QuoteString(evt.MvccTimestamp);

        return $"INSERT INTO {qualifiedTable} " +
               $"(_event_id, _operation, _entity_id, _timestamp, _mvcc_ts, _company_id, _location_id, _model_ver, _diff, _before, _after) " +
               $"VALUES ({QuoteString(evt.Id)}, {QuoteString(evt.Operation)}, {QuoteString(entityId)}, " +
               $"{evt.Timestamp}, {mvccTs}, " +
               $"{(evt.CompanyId  is null ? "NULL" : QuoteString(evt.CompanyId))}, " +
               $"{(evt.LocationId is null ? "NULL" : QuoteString(evt.LocationId))}, " +
               $"{QuoteString(evt.ModelVersion ?? string.Empty)}, {diff}, {before}, {after});\n";
    }

    private string BuildCurrentViewSql(string table)
    {
        var qualifiedTable = QualifyEventsTable(table, EventsSchema);
        var qualifiedView  = QualifyEventsView("current_" + table, EventsSchema);
        var selectSql      = $"""
            SELECT * FROM (
              SELECT *,
                ROW_NUMBER() OVER (PARTITION BY _entity_id ORDER BY _timestamp DESC, _seq DESC) AS rn
              FROM {qualifiedTable}
            ) ranked
            WHERE rn = 1 AND _operation <> 'DELETE'
            """;
        return BuildCreateOrReplaceViewSql(qualifiedView, selectSql);
    }

    private string BuildHistoryViewSql(string table, Schema schema)
    {
        var qualifiedTable = QualifyEventsTable(table, EventsSchema);
        var qualifiedView  = QualifyEventsView(table + "_history", EventsSchema);

        var fieldProjections = schema.Columns()
            .Select(col =>
                $"  COALESCE({JsonExtract("_after", col)}, {JsonExtract("_before", col)}) AS {QuoteId(col)}")
            .ToList();

        var selectSql =
            "SELECT\n  _seq, _event_id, _operation, _entity_id, _timestamp, _mvcc_ts,\n" +
            "  _company_id, _location_id, _model_ver,\n" +
            string.Join(",\n", fieldProjections) + "\n" +
            $"FROM {qualifiedTable}";

        return BuildCreateOrReplaceViewSql(qualifiedView, selectSql);
    }

    // ── IDriverImport / IImportHandler ────────────────────────────────────────

    private const int ImportBatchSize = 500;

    /// <summary>
    /// Initialises the driver for bulk import: opens a connection and reflects the
    /// current database schema, but skips the startup table-creation sweep.
    /// </summary>
    public async Task InitForImportAsync(
        ILogger logger,
        ISchemaRegistry registry,
        string url,
        DriverMode mode = DriverMode.Upsert,
        string eventsSchema = "eds_events",
        CancellationToken ct = default)
    {
        Logger       = logger;
        Registry     = registry;
        Mode         = mode;
        EventsSchema = eventsSchema;
        InitialiseDriver(new DriverConfig
        {
            Url            = url,
            Logger         = logger,
            SchemaRegistry = registry,
            Tracker        = null!,   // not used during import
            DataDir        = string.Empty,
            Mode           = mode,
            EventsSchema   = eventsSchema,
        });
        await using var conn = await OpenConnectionAsync(ct);
        await RefreshSchemaAsync(conn, ct);
    }

    /// <summary>Drops tables in the DB that are not in <paramref name="knownTables"/>.</summary>
    public async Task DropOrphanTablesAsync(ILogger logger, IReadOnlySet<string> knownTables, CancellationToken ct = default)
    {
        var orphans = DbSchema.Keys
            .Where(t => !knownTables.Contains(t))
            .ToList();

        if (orphans.Count == 0) return;

        logger.LogInformation("[{Driver}] Dropping {Count} orphan table(s) not in current HQ schema: {Tables}",
            GetType().Name, orphans.Count, string.Join(", ", orphans));

        await using var conn = await OpenConnectionAsync(ct);
        foreach (var table in orphans)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = BuildDropTableSql(table);
            await cmd.ExecuteNonQueryAsync(ct);
            logger.LogDebug("[{Driver}] Dropped orphan table {Table}.", GetType().Name, table);
        }
        await RefreshSchemaAsync(conn, ct);
    }

    /// <summary>Drops and recreates the table — called once per table before rows arrive.</summary>
    public Task CreateDatasourceAsync(Schema schema, CancellationToken ct = default) =>
        MigrateNewTableAsync(Logger!, schema, ct);

    /// <summary>
    /// Appends one event to the pending batch and flushes when <see cref="ImportBatchSize"/> is reached.
    /// In upsert mode, builds SQL directly from the supplied schema to avoid a registry round-trip per row.
    /// Import always upserts into the standard mirror table regardless of driver mode.
    /// The bulk import is a point-in-time snapshot; time-series events tables are
    /// populated by the live CDC stream once the server starts.
    /// </summary>
    public async Task ImportEventAsync(DbChangeEvent evt, Schema schema, CancellationToken ct = default)
    {
        // Guard against schema drift: only INSERT columns that actually exist in the DB.
        // On a resumed import the DB table may lack columns that were added to the HQ
        // schema after the table was originally created. Skip unknown columns rather than
        // failing the entire batch — the schema reconciliation in NdjsonGzImporter will
        // have added them, but this acts as a safety net.
        Schema effectiveSchema = schema;
        if (DbSchema.TryGetValue(schema.Table, out var dbCols))
        {
            var unknown = schema.Columns().Where(c => !dbCols.ContainsKey(c)).ToList();
            if (unknown.Count > 0)
            {
                Logger?.LogDebug(
                    "[import] {Table}: skipping {N} column(s) not yet in DB: {Cols}",
                    schema.Table, unknown.Count, string.Join(", ", unknown));
                effectiveSchema = new Schema
                {
                    Table        = schema.Table,
                    ModelVersion = schema.ModelVersion,
                    PrimaryKeys  = schema.PrimaryKeys,
                    Required     = schema.Required,
                    Properties   = schema.Properties
                        .Where(kv => dbCols.ContainsKey(kv.Key))
                        .ToDictionary(kv => kv.Key, kv => kv.Value),
                };
            }
        }
        var sql = BuildSql(evt, effectiveSchema);
        _pending.Append(sql);
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
