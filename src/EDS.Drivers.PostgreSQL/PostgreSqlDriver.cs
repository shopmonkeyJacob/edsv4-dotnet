using EDS.Core.Abstractions;
using EDS.Core.Drivers;
using EDS.Core.Helpers;
using EDS.Core.Models;
using Microsoft.Extensions.Logging;
using Npgsql;
using System.Data.Common;
using System.Text;
using System.Text.Json;

namespace EDS.Drivers.PostgreSQL;

public sealed class PostgreSqlDriver : SqlDriverBase, IDriverHelp, IDriverAlias
{
    private NpgsqlDataSource? _dataSource;

    // IDriverAlias
    public IReadOnlyList<string> Aliases => ["postgresql"];

    // IDriverHelp
    public string Name        => "PostgreSQL";
    public string Description => "Streams EDS messages into a PostgreSQL database.";
    public string ExampleUrl  => "postgres://user:password@localhost:5432/mydb";
    public string Help        => "The database schema mirrors the Shopmonkey transactional database schema. " +
                                 "Each table is created with appropriate column types and a primary key.";

    // ── SqlDriverBase: bool literals (PostgreSQL uses TRUE/FALSE) ─────────────

    protected override string SqlTrue  => "TRUE";
    protected override string SqlFalse => "FALSE";

    // ── SqlDriverBase: driver initialisation ──────────────────────────────────

    protected override void InitialiseDriver(DriverConfig config) =>
        _dataSource = NpgsqlDataSource.Create(config.Url);

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

    protected override string GetDatabaseNameSql() => "SELECT current_database()";

    protected override string GetSchemaInfoSql() =>
        """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_catalog = @db AND table_schema = 'public'
        """;

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
            obj.TryGetValue(c, out var v) ? QuoteJsonElement(v) : "NULL"));

        // For UPDATEs, only update columns listed in evt.Diff (partial update optimisation)
        var updates = new List<string>();
        if (evt.Operation.Equals("UPDATE", StringComparison.OrdinalIgnoreCase) && evt.Diff?.Length > 0)
        {
            foreach (var col in evt.Diff)
            {
                if (col == "id" || !cols.Contains(col)) continue;
                updates.Add($"{QuoteId(col)} = {(obj.TryGetValue(col, out var v) ? QuoteJsonElement(v) : "NULL")}");
            }
        }
        else
        {
            foreach (var col in cols.Where(c => c != "id"))
                updates.Add($"{QuoteId(col)} = {(obj.TryGetValue(col, out var v) ? QuoteJsonElement(v) : "NULL")}");
        }

        var onConflict = updates.Count == 0
            ? "NOTHING"
            : $"UPDATE SET {string.Join(", ", updates)}";

        return $"INSERT INTO {QuoteId(evt.Table)} ({colList}) VALUES ({valList}) ON CONFLICT (id) DO {onConflict};\n";
    }

    protected override string BuildCreateTableIfNotExistsSql(Schema schema) =>
        $"CREATE TABLE IF NOT EXISTS {QuoteId(schema.Table)} (\n{BuildCreateTableBody(schema)}\n);";

    protected override string BuildDropAndCreateTableSql(Schema schema) =>
        $"DROP TABLE IF EXISTS {QuoteId(schema.Table)};\n" +
        $"CREATE TABLE {QuoteId(schema.Table)} (\n{BuildCreateTableBody(schema)}\n);";

    protected override string BuildAlterAddColumnSql(string table, string col, string sqlType) =>
        $"ALTER TABLE {QuoteId(table)} ADD COLUMN {QuoteId(col)} {sqlType};";

    protected override string PropToSqlType(SchemaProperty prop, bool isPrimaryKey = false) => prop.Type switch
    {
        "string" when prop.Format == "date-time"  => "TIMESTAMP WITH TIME ZONE",
        "string"                                  => "TEXT",
        "integer"                                 => "BIGINT",
        "number"                                  => "DOUBLE PRECISION",
        "boolean"                                 => "BOOLEAN",
        "object"                                  => "JSONB",
        "array" when prop.Items?.Enum is not null  => "VARCHAR(64)",
        "array"                                   => "JSONB",
        _                                         => "TEXT"
    };

    protected override string QuoteId(string name) => $"\"{name.Replace("\"", "\"\"")}\"";

    // ── IDriverHelp: test + config ────────────────────────────────────────────

    public override async Task TestAsync(ILogger logger, string url, CancellationToken ct = default)
    {
        var ds = NpgsqlDataSource.Create(url);
        await using var conn = await ds.OpenConnectionAsync(ct);
    }

    public override IReadOnlyList<DriverField> Configuration() =>
        DriverFieldHelpers.DatabaseFields(defaultPort: 5432);

    public override (string url, IReadOnlyList<FieldError> errors) Validate(Dictionary<string, object?> values)
    {
        try { return (DriverFieldHelpers.BuildDatabaseUrl("postgres", 5432, values), []); }
        catch (Exception ex) { return (string.Empty, [new FieldError { Field = "Hostname", Message = ex.Message }]); }
    }
}
