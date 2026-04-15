using EDS.Core.Abstractions;
using EDS.Core.Helpers;
using EDS.Core.Models;
using Microsoft.Extensions.Logging;
using Snowflake.Data.Client;
using System.Text;
using System.Text.Json;

[assembly: System.Runtime.CompilerServices.InternalsVisibleTo("EDS.Integration.Tests")]

namespace EDS.Drivers.Snowflake;

/// <summary>
/// Streams CDC events into Snowflake using MERGE statements.
/// Mirrors the Go snowflake driver with session ID correlation and batch limiting.
/// </summary>
public sealed class SnowflakeDriver : IDriver, IDriverLifecycle, IDriverHelp, IDriverSessionHandler, IDriverMigration, IDriverImport
{
    private ISchemaRegistry? _registry;
    private ITracker?        _tracker;
    private string?          _connectionString;
    private ILogger?         _logger;
    private string           _sessionId = string.Empty;
    private DatabaseSchema   _dbSchema  = new();
    private readonly List<(DbChangeEvent evt, Schema schema)> _pending = new();

    public string Name        => "Snowflake";
    public string Description => "Streams EDS messages into a Snowflake database.";
    public string ExampleUrl  => "snowflake://user:password@account/database/schema?warehouse=COMPUTE_WH";
    public string Help        => "Events are merged into Snowflake tables using MERGE statements. " +
                                 "The Snowflake account identifier must be in the form account.region.cloud.";

    public int MaxBatchSize => 200;

    public void SetSessionId(string sessionId) => _sessionId = sessionId;

    public async Task StartAsync(DriverConfig config, CancellationToken ct = default)
    {
        _logger           = config.Logger;
        _registry         = config.SchemaRegistry;
        _tracker          = config.Tracker;
        _connectionString = BuildConnectionString(config.Url);

        // Pre-load the schema so MigrateNewColumns can check column existence without
        // issuing a live INFORMATION_SCHEMA query on each migration call.
        using var conn = new SnowflakeDbConnection { ConnectionString = _connectionString };
        await conn.OpenAsync(ct);
        await RefreshSchemaAsync(conn, ct);
    }

    public async Task<bool> ProcessAsync(ILogger logger, DbChangeEvent evt, CancellationToken ct = default)
    {
        var schema = await _registry!.GetSchemaAsync(evt.Table, evt.ModelVersion ?? string.Empty, ct)
            ?? throw new InvalidOperationException($"Schema not found for {evt.Table} v{evt.ModelVersion}");
        logger.LogDebug("[SnowflakeDriver] Queuing {Op} on {Table}", evt.Operation, evt.Table);
        _pending.Add((evt, schema));
        return _pending.Count >= MaxBatchSize;
    }

    public async Task FlushAsync(ILogger logger, CancellationToken ct = default)
    {
        if (_pending.Count == 0) return;

        logger.LogInformation("[SnowflakeDriver] Flushing {Count} statement(s)", _pending.Count);

        using var conn = new SnowflakeDbConnection { ConnectionString = _connectionString };
        await conn.OpenAsync(ct);

        // Group by table to merge per-table
        var byTable = _pending.GroupBy(p => p.evt.Table);
        foreach (var group in byTable)
        {
            var schema = group.First().schema;
            var events = group.Select(g => g.evt).ToList();
            await MergeEventsAsync(conn, schema, events, logger, ct);
        }

        logger.LogInformation("[SnowflakeDriver] Committed {Count} statement(s)", _pending.Count);
        _pending.Clear();
    }

    public async Task TestAsync(ILogger logger, string url, CancellationToken ct = default)
    {
        var connStr = BuildConnectionString(url);
        using var conn = new SnowflakeDbConnection { ConnectionString = connStr };
        await conn.OpenAsync(ct);
    }

    public Task StopAsync(CancellationToken ct = default)
    {
        _pending.Clear();
        return Task.CompletedTask;
    }

    public IReadOnlyList<DriverField> Configuration() =>
    [
        DriverFieldHelpers.RequiredString("Account",   "Snowflake account identifier (e.g. myorg-myaccount)"),
        DriverFieldHelpers.RequiredString("Database",  "Database name"),
        DriverFieldHelpers.RequiredString("Schema",    "Schema name", "public"),
        DriverFieldHelpers.RequiredString("Warehouse", "Warehouse name"),
        DriverFieldHelpers.OptionalString("Username",  "Username"),
        DriverFieldHelpers.OptionalPassword("Password","Password"),
        DriverFieldHelpers.OptionalString("Role",      "Role to use")
    ];

    public (string url, IReadOnlyList<FieldError> errors) Validate(Dictionary<string, object?> values)
    {
        try
        {
            var account   = DriverFieldHelpers.GetRequiredString("Account",   values);
            var database  = DriverFieldHelpers.GetRequiredString("Database",  values);
            var schema    = DriverFieldHelpers.GetOptionalString("Schema",    "public",        values);
            var warehouse = DriverFieldHelpers.GetRequiredString("Warehouse", values);
            var username  = DriverFieldHelpers.GetOptionalString("Username",  string.Empty,    values);
            var password  = DriverFieldHelpers.GetOptionalString("Password",  string.Empty,    values);
            var url = $"snowflake://{username}:{password}@{account}/{database}/{schema}?warehouse={Uri.EscapeDataString(warehouse)}";
            return (url, []);
        }
        catch (Exception ex)
        {
            return (string.Empty, [new FieldError { Field = "Account", Message = ex.Message }]);
        }
    }

    // ── IDriverMigration ──────────────────────────────────────────────────────

    /// <summary>
    /// Creates or replaces the table with the new schema using a single atomic
    /// <c>CREATE OR REPLACE TABLE</c> statement (mirrors Go MigrateNewTable).
    /// Also clears any Snowflake insert-dedup tracker keys for this table.
    /// </summary>
    public async Task MigrateNewTableAsync(ILogger logger, Schema schema, CancellationToken ct = default)
    {
        if (_dbSchema.ContainsKey(schema.Table))
        {
            logger.LogInformation("[Snowflake] Table {Table} already exists — dropping and recreating.", schema.Table);

            // Clear insert-dedup tracker keys for this table (mirrors Go's tracker.DeleteKeysWithPrefix).
            if (_tracker is not null)
            {
                var prefix = $"snowflake:{schema.Table}:";
                var deleted = await _tracker.DeleteKeysWithPrefixAsync(prefix, ct);
                logger.LogDebug("[Snowflake] Deleted {Count} tracker cache key(s) for {Table}.", deleted, schema.Table);
            }
        }

        using var conn = new SnowflakeDbConnection { ConnectionString = _connectionString };
        await conn.OpenAsync(ct);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = BuildCreateOrReplaceTableSql(schema);
        logger.LogDebug("[Snowflake] MigrateNewTable SQL: {Sql}", cmd.CommandText);
        await cmd.ExecuteNonQueryAsync(ct);
        await RefreshSchemaAsync(conn, ct);
    }

    /// <summary>
    /// Adds any columns that appear in <paramref name="newColumns"/> but do not yet
    /// exist in the destination table. Uses the cached <see cref="_dbSchema"/> to
    /// avoid a live INFORMATION_SCHEMA query (mirrors Go MigrateNewColumns).
    /// </summary>
    public async Task MigrateNewColumnsAsync(ILogger logger, Schema schema, IReadOnlyList<string> newColumns, CancellationToken ct = default)
    {
        using var conn = new SnowflakeDbConnection { ConnectionString = _connectionString };
        await conn.OpenAsync(ct);

        foreach (var col in newColumns)
        {
            // Use cached schema — same approach as Go's addNewColumnsSQL with dbschema check.
            if (_dbSchema.TryGetValue(schema.Table, out var existing) && existing.ContainsKey(col))
            {
                logger.LogWarning("[Snowflake] Skipping migration for column {Column} on {Table} — already exists.", col, schema.Table);
                continue;
            }

            var prop = schema.Properties[col];
            var sqlType = PropToSnowflakeSqlType(prop);
            using var altCmd = conn.CreateCommand();
            altCmd.CommandText = $"ALTER TABLE {QuoteSnowId(schema.Table)} ADD COLUMN {QuoteSnowId(col)} {sqlType};";
            logger.LogDebug("[Snowflake] MigrateNewColumns SQL: {Sql}", altCmd.CommandText);
            await altCmd.ExecuteNonQueryAsync(ct);
            logger.LogInformation("[Snowflake] Added column {Column} ({Type}) to {Table}.", col, sqlType, schema.Table);
        }

        await RefreshSchemaAsync(conn, ct);
    }

    public async Task MigrateChangedColumnsAsync(ILogger logger, Schema schema, IReadOnlyList<string> changedColumns, CancellationToken ct = default)
    {
        using var conn = new SnowflakeDbConnection { ConnectionString = _connectionString };
        await conn.OpenAsync(ct);

        foreach (var col in changedColumns)
        {
            if (!schema.Properties.TryGetValue(col, out var prop)) continue;
            var sqlType = PropToSnowflakeSqlType(prop);
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"ALTER TABLE {QuoteSnowId(schema.Table)} ALTER COLUMN {QuoteSnowId(col)} SET DATA TYPE {sqlType};";
            logger.LogInformation("[Snowflake] Altering column type {Column} on {Table} to {Type}.", col, schema.Table, sqlType);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        if (changedColumns.Count > 0)
            await RefreshSchemaAsync(conn, ct);
    }

    public async Task MigrateRemovedColumnsAsync(ILogger logger, Schema schema, IReadOnlyList<string> removedColumns, CancellationToken ct = default)
    {
        using var conn = new SnowflakeDbConnection { ConnectionString = _connectionString };
        await conn.OpenAsync(ct);

        foreach (var col in removedColumns)
        {
            if (_dbSchema.TryGetValue(schema.Table, out var existing) && !existing.ContainsKey(col)) continue;
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"ALTER TABLE {QuoteSnowId(schema.Table)} DROP COLUMN {QuoteSnowId(col)};";
            logger.LogInformation("[Snowflake] Dropping removed column {Column} from {Table}.", col, schema.Table);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        if (removedColumns.Count > 0)
            await RefreshSchemaAsync(conn, ct);
    }

    // ── IDriverImport ─────────────────────────────────────────────────────────

    private const int ImportBatchSize = 200; // matches MaxBatchSize

    public async Task InitForImportAsync(
        ILogger logger,
        ISchemaRegistry registry,
        string url,
        DriverMode mode = DriverMode.Upsert,
        string eventsSchema = "eds_events",
        CancellationToken ct = default)
    {
        _logger           = logger;
        _registry         = registry;
        _connectionString = BuildConnectionString(url);

        using var conn = new SnowflakeDbConnection { ConnectionString = _connectionString };
        await conn.OpenAsync(ct);
        await RefreshSchemaAsync(conn, ct);
    }

    public async Task DropOrphanTablesAsync(ILogger logger, IReadOnlySet<string> knownTables, CancellationToken ct = default)
    {
        var orphans = _dbSchema.Keys.Where(t => !knownTables.Contains(t)).ToList();
        if (orphans.Count == 0) return;

        logger.LogInformation("[Snowflake] Dropping {Count} orphan table(s) not in current HQ schema: {Tables}",
            orphans.Count, string.Join(", ", orphans));

        using var conn = new SnowflakeDbConnection { ConnectionString = _connectionString };
        await conn.OpenAsync(ct);
        foreach (var table in orphans)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"DROP TABLE IF EXISTS {QuoteSnowId(table)};";
            await cmd.ExecuteNonQueryAsync(ct);
            logger.LogDebug("[Snowflake] Dropped orphan table {Table}.", table);
        }
        await RefreshSchemaAsync(conn, ct);
    }

    public Task CreateDatasourceAsync(Schema schema, CancellationToken ct = default) =>
        MigrateNewTableAsync(_logger!, schema, ct);

    public async Task ImportEventAsync(DbChangeEvent evt, Schema schema, CancellationToken ct = default)
    {
        _pending.Add((evt, schema));
        if (_pending.Count >= ImportBatchSize)
            await FlushAsync(_logger!, ct);
    }

    public async Task ImportCompletedAsync(string table, long rowCount, CancellationToken ct = default)
    {
        if (_pending.Count > 0)
            await FlushAsync(_logger!, ct);
        _logger!.LogInformation("[Snowflake] Imported {Rows} row(s) into {Table}.", rowCount, table);
    }

    // ── SQL generation ────────────────────────────────────────────────────────

    /// <summary>
    /// Builds a <c>CREATE OR REPLACE TABLE</c> statement for the given schema.
    /// Mirrors Go's <c>createSQL</c> function exactly.
    /// </summary>
    private static string BuildCreateOrReplaceTableSql(Schema schema)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"CREATE OR REPLACE TABLE {QuoteSnowId(schema.Table)} (");
        foreach (var col in schema.Columns())
        {
            var prop    = schema.Properties[col];
            var notNull = schema.Required.Contains(col) && prop.IsNotNull ? " NOT NULL" : string.Empty;
            sb.AppendLine($"\t{QuoteSnowId(col)} {PropToSnowflakeSqlType(prop)}{notNull},");
        }
        if (schema.PrimaryKeys.Count > 0)
        {
            var pks = string.Join(", ", schema.PrimaryKeys.Select(QuoteSnowId));
            sb.AppendLine($"\tPRIMARY KEY ({pks})");
        }
        sb.Append(");");
        return sb.ToString();
    }

    /// <summary>
    /// Maps a <see cref="SchemaProperty"/> to its Snowflake SQL type.
    /// Mirrors Go's <c>propTypeToSQLType</c> exactly — no special handling for primary keys.
    /// </summary>
    private static string PropToSnowflakeSqlType(SchemaProperty prop) => prop.Type switch
    {
        "string"  when prop.Format == "date-time"  => "TIMESTAMP_NTZ",
        "string"                                   => "STRING",
        "integer"                                  => "INTEGER",
        "number"                                   => "FLOAT",
        "boolean"                                  => "BOOLEAN",
        "object"                                   => "STRING",
        "array"   when prop.Items?.Enum is not null => "STRING",
        "array"                                    => "VARIANT",
        _                                          => "STRING"
    };

    /// <summary>
    /// Queries INFORMATION_SCHEMA.COLUMNS and refreshes the in-memory <see cref="_dbSchema"/> cache.
    /// Called at startup and after each migration (mirrors Go's <c>refreshSchema</c>).
    /// </summary>
    private async Task RefreshSchemaAsync(SnowflakeDbConnection conn, CancellationToken ct)
    {
        var schema = new DatabaseSchema();
        using var cmd = conn.CreateCommand();
        cmd.CommandText =
            "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE " +
            "FROM INFORMATION_SCHEMA.COLUMNS " +
            "WHERE TABLE_CATALOG = CURRENT_DATABASE() AND TABLE_SCHEMA = CURRENT_SCHEMA()";
        using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var table = reader.GetString(0);
            var col   = reader.GetString(1);
            var type  = reader.GetString(2);
            if (!schema.ContainsKey(table)) schema[table] = new Dictionary<string, string>();
            schema[table][col] = type;
        }
        _dbSchema = schema;
    }

    // ─── SQL generation ───────────────────────────────────────────────────────

    /// <summary>
    /// Builds the SQL statement for a single CDC event (DELETE or MERGE).
    /// Extracted for testability — called by <see cref="MergeEventsAsync"/>.
    /// </summary>
    internal static string BuildEventSql(Schema schema, DbChangeEvent evt)
    {
        if (evt.Operation.Equals("DELETE", StringComparison.OrdinalIgnoreCase))
        {
            var where = string.Join(" AND ",
                schema.PrimaryKeys.Select((pk, i) =>
                    $"{QuoteSnowId(pk)} = {QuoteLiteral(evt.Key.Length > i + 1 ? evt.Key[i + 1] : string.Empty)}"));
            return $"DELETE FROM {QuoteSnowId(schema.Table)} WHERE {where};";
        }

        var obj = evt.After.HasValue
            ? JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(evt.After.Value.GetRawText()) ?? []
            : [];

        var cols = schema.Columns();
        var colList = string.Join(", ", cols.Select(c => QuoteSnowId(c)));
        var valList = string.Join(", ", cols.Select(c =>
            obj.TryGetValue(c, out var v) ? QuoteJsonElement(v) : "NULL"));

        var pks = string.Join(" AND ",
            schema.PrimaryKeys.Select(pk => $"t.{QuoteSnowId(pk)} = s.{QuoteSnowId(pk)}"));

        var updates = string.Join(", ", cols.Where(c => !schema.PrimaryKeys.Contains(c))
            .Select(c => $"t.{QuoteSnowId(c)} = s.{QuoteSnowId(c)}"));

        return $"""
            MERGE INTO {QuoteSnowId(schema.Table)} t
            USING (SELECT {valList}) s ({colList})
            ON {pks}
            WHEN MATCHED THEN UPDATE SET {updates}
            WHEN NOT MATCHED THEN INSERT ({colList}) VALUES ({valList});
            """;
    }

    private static async Task MergeEventsAsync(
        SnowflakeDbConnection conn,
        Schema schema,
        IReadOnlyList<DbChangeEvent> events,
        ILogger logger,
        CancellationToken ct)
    {
        if (events.Count == 0) return;

        // Build all SQL for this table's events, then execute inside a single transaction
        // so the entire batch commits or rolls back atomically — and to reduce round-trips
        // compared to auto-committing each statement individually.
        var sb = new StringBuilder();
        foreach (var evt in events)
            sb.AppendLine(BuildEventSql(schema, evt));

        var sql = sb.ToString();
        using var tx  = conn.BeginTransaction();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.Transaction = tx;
        // Tell Snowflake we are submitting multiple statements in one request.
        var p = cmd.CreateParameter();
        p.ParameterName = "multi_statement_count";
        p.Value         = events.Count;
        cmd.Parameters.Add(p);
        try
        {
            await cmd.ExecuteNonQueryAsync(ct);
            tx.Commit();
        }
        catch (Exception ex)
        {
            tx.Rollback();
            logger.LogError(ex, "[SnowflakeDriver] Transaction failed, rolling back. SQL: {Sql}",
                sql.Length > 500 ? sql[..500] + "…" : sql);
            if (sql.Length > 500)
                logger.LogDebug("[SnowflakeDriver] Full SQL: {Sql}", sql);
            throw;
        }
    }

    /// <summary>Quotes a Snowflake identifier: wraps in double-quotes and escapes any embedded double-quotes as "".</summary>
    private static string QuoteSnowId(string name) => $"\"{name.Replace("\"", "\"\"")}\"";

    private static string QuoteLiteral(string value) => "'" + value.Replace("'", "''") + "'";

    private static string QuoteJsonElement(JsonElement el) => el.ValueKind switch
    {
        JsonValueKind.Null or JsonValueKind.Undefined => "NULL",
        JsonValueKind.True                            => "TRUE",
        JsonValueKind.False                           => "FALSE",
        JsonValueKind.Number => SqlHelpers.IsValidNumericLiteral(el.GetRawText()) ? el.GetRawText() : "NULL",
        JsonValueKind.String => QuoteLiteral(el.GetString() ?? string.Empty),
        _                    => QuoteLiteral(el.GetRawText())
    };

    private static string BuildConnectionString(string url)
    {
        var uri      = new Uri(url);
        var qs       = System.Web.HttpUtility.ParseQueryString(uri.Query);
        var parts    = uri.AbsolutePath.TrimStart('/').Split('/');
        var database = parts.Length > 0 ? parts[0] : string.Empty;
        var schema   = parts.Length > 1 ? parts[1] : "public";
        var userInfo = uri.UserInfo.Split(':', 2);

        return new StringBuilder()
            .Append($"account={uri.Host};")
            .Append($"user={Uri.UnescapeDataString(userInfo.Length > 0 ? userInfo[0] : string.Empty)};")
            .Append($"password={Uri.UnescapeDataString(userInfo.Length > 1 ? userInfo[1] : string.Empty)};")
            .Append($"db={database};")
            .Append($"schema={schema};")
            .Append(qs["warehouse"] is { } wh   ? $"warehouse={wh};"   : string.Empty)
            .Append(qs["role"]      is { } role ? $"role={role};"      : string.Empty)
            .ToString();
    }
}
