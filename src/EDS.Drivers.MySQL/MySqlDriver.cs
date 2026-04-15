using EDS.Core.Abstractions;
using EDS.Core.Drivers;
using EDS.Core.Helpers;
using EDS.Core.Models;
using Microsoft.Extensions.Logging;
using MySqlConnector;
using System.Data.Common;
using System.Text;
using System.Text.Json;

namespace EDS.Drivers.MySQL;

public sealed class MySqlDriver : SqlDriverBase, IDriverHelp
{
    private MySqlDataSource? _dataSource;

    // IDriverHelp
    public string Name        => "MySQL";
    public string Description => "Streams EDS messages into a MySQL database.";
    public string ExampleUrl  => "mysql://user:password@localhost:3306/mydb";
    public string Help        => "The database schema mirrors the Shopmonkey transactional database schema.";

    // ── SqlDriverBase: time-series overrides ──────────────────────────────────

    // MySQL has no schema concept within a database; use a table-name prefix instead.
    protected override string GetEnsureEventsSchemaSql(string schemaName) => string.Empty;

    protected override string QualifyEventsTable(string table, string eventsSchema) =>
        QuoteId(table + "__events");

    protected override string QualifyEventsView(string viewName, string eventsSchema) =>
        QuoteId(viewName);

    protected override string JsonExtract(string column, string field) =>
        $"JSON_UNQUOTE(JSON_EXTRACT({column}, '$.{field.Replace("'", "\\'")}'))";

    protected override string GetAutoIncrementPkDef() => "BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY";

    protected override string GetJsonColumnType() => "JSON";

    // ── SqlDriverBase: driver initialisation ──────────────────────────────────

    protected override void InitialiseDriver(DriverConfig config) =>
        _dataSource = CreateDataSource(config.Url);

    protected override async Task<DbConnection> OpenConnectionAsync(CancellationToken ct) =>
        await _dataSource!.OpenConnectionAsync(ct);

    public override async Task StopAsync(CancellationToken ct = default)
    {
        if (_dataSource is not null)
        {
            await _dataSource.DisposeAsync();
            _dataSource = null;
        }
    }

    // ── SqlDriverBase: schema reflection ─────────────────────────────────────

    protected override string GetDatabaseNameSql() => "SELECT DATABASE()";

    protected override string GetSchemaInfoSql() =>
        "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE " +
        "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = @db";

    protected override void AddDbNameParameter(DbCommand cmd, string dbName)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = "@db";
        p.Value = dbName;
        cmd.Parameters.Add(p);
    }

    // ── SqlDriverBase: SQL generation ─────────────────────────────────────────

    protected override string BuildSql(DbChangeEvent evt, Schema schema)
    {
        if (evt.Operation.Equals("DELETE", StringComparison.OrdinalIgnoreCase))
        {
            var where = string.Join(" AND ",
                schema.PrimaryKeys.Select((pk, i) =>
                    $"{QuoteId(pk)} = {QuoteLiteral(evt.Key.Length > i + 1 ? evt.Key[i + 1] : string.Empty)}"));
            return $"DELETE FROM {QuoteId(evt.Table)} WHERE {where};\n";
        }

        var obj = evt.After.HasValue
            ? JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(evt.After.Value.GetRawText()) ?? []
            : [];

        var cols    = schema.Columns();
        var colList = string.Join(", ", cols.Select(c => QuoteId(c)));
        var valList = string.Join(", ", cols.Select(c =>
        {
            if (!obj.TryGetValue(c, out var v)) return "NULL";
            // MySQL TIMESTAMP rejects ISO 8601 T/Z — reformat to 'YYYY-MM-DD HH:mm:ss.ffffff'
            if (v.ValueKind == JsonValueKind.String
                && schema.Properties.TryGetValue(c, out var prop)
                && prop.Type == "string" && prop.Format == "date-time")
            {
                var s = v.GetString();
                if (s is not null && DateTimeOffset.TryParse(s, out var dt))
                    return QuoteLiteral(dt.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss.ffffff"));
            }
            return QuoteJsonElement(v);
        }));

        var updates = cols.Where(c => c != "id")
            .Select(c => $"{QuoteId(c)} = VALUES({QuoteId(c)})");

        return $"INSERT INTO {QuoteId(evt.Table)} ({colList}) VALUES ({valList}) " +
               $"ON DUPLICATE KEY UPDATE {string.Join(", ", updates)};\n";
    }

    protected override string BuildCreateTableIfNotExistsSql(Schema schema) =>
        BuildCreateTableSql(schema, ifNotExists: true);

    protected override string BuildDropAndCreateTableSql(Schema schema) =>
        $"DROP TABLE IF EXISTS {QuoteId(schema.Table)};\n{BuildCreateTableSql(schema)}";

    protected override string BuildAlterAddColumnSql(string table, string col, string sqlType) =>
        $"ALTER TABLE {QuoteId(table)} ADD COLUMN {QuoteId(col)} {sqlType};";

    protected override string BuildAlterColumnTypeSql(string table, string col, string sqlType) =>
        $"ALTER TABLE {QuoteId(table)} MODIFY COLUMN {QuoteId(col)} {sqlType};";

    protected override string BuildDropColumnSql(string table, string col) =>
        $"ALTER TABLE {QuoteId(table)} DROP COLUMN {QuoteId(col)};";

    protected override string BuildDropTableSql(string table) =>
        $"DROP TABLE IF EXISTS {QuoteId(table)};";

    protected override string PropToSqlType(SchemaProperty prop, bool isPrimaryKey = false) => prop.Type switch
    {
        "string" when isPrimaryKey                => "VARCHAR(64)",
        "string" when prop.Format == "date-time"  => "TIMESTAMP",
        "string"                                  => "TEXT",
        "integer"                                 => "BIGINT",
        "number"                                  => "FLOAT",
        "boolean"                                 => "BOOLEAN",
        "array" when prop.Items?.Enum is not null  => "VARCHAR(64)",
        "object" or "array"                       => "JSON",
        _                                         => "TEXT"
    };

    protected override string QuoteId(string name) => $"`{name.Replace("`", "``")}`";

    // MySQL interprets \n, \t, \\, etc. as escape sequences inside single-quoted string
    // literals. JSON payloads routinely contain sequences like \n (backslash + n) — without
    // escaping the backslash first, MySQL converts it to a literal 0x0A newline before the
    // JSON validator runs, producing "Invalid encoding in string" errors on JSON columns.
    protected override string QuoteString(string value) =>
        "'" + value.Replace("\\", "\\\\").Replace("'", "''") + "'";

    // ── IDriverHelp: test + config ────────────────────────────────────────────

    public override async Task TestAsync(ILogger logger, string url, CancellationToken ct = default)
    {
        var ds = CreateDataSource(url);
        await using var conn = await ds.OpenConnectionAsync(ct);
    }

    public override IReadOnlyList<DriverField> Configuration() =>
        DriverFieldHelpers.DatabaseFields(defaultPort: 3306);

    public override (string url, IReadOnlyList<FieldError> errors) Validate(Dictionary<string, object?> values)
    {
        try { return (DriverFieldHelpers.BuildDatabaseUrl("mysql", 3306, values), []); }
        catch (Exception ex) { return (string.Empty, [new FieldError { Field = "Hostname", Message = ex.Message }]); }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private string BuildCreateTableSql(Schema schema, bool ifNotExists = false)
    {
        var keyword = ifNotExists ? "CREATE TABLE IF NOT EXISTS" : "CREATE TABLE";
        return $"{keyword} {QuoteId(schema.Table)} (\n{BuildCreateTableBody(schema)}\n) CHARACTER SET=utf8mb4;";
    }

    private static MySqlDataSource CreateDataSource(string url)
    {
        var uri = new Uri(url);
        var builder = new MySqlConnectionStringBuilder
        {
            Server        = uri.Host,
            Port          = (uint)(uri.Port > 0 ? uri.Port : 3306),
            Database      = uri.AbsolutePath.TrimStart('/'),
            UserID        = Uri.UnescapeDataString(uri.UserInfo.Split(':')[0]),
            Password      = uri.UserInfo.Contains(':')
                                ? Uri.UnescapeDataString(uri.UserInfo.Split(':', 2)[1])
                                : string.Empty,
            CharacterSet  = "utf8mb4"
        };
        return new MySqlDataSource(builder.ConnectionString);
    }
}
