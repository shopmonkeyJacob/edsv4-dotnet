using EDS.Core.Abstractions;
using EDS.Core.Drivers;
using EDS.Core.Helpers;
using EDS.Core.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System.Data.Common;
using System.Text;
using System.Text.Json;

namespace EDS.Drivers.SqlServer;

public sealed class SqlServerDriver : SqlDriverBase, IDriverHelp
{
    private string? _connectionString;

    // IDriverHelp
    public string Name        => "SQL Server";
    public string Description => "Streams EDS messages into a Microsoft SQL Server database.";
    public string ExampleUrl  => "sqlserver://user:password@localhost?database=mydb";
    public string Help        => "The database schema mirrors the Shopmonkey transactional database schema.";

    // ── SqlDriverBase: Unicode string literals (SQL Server requires N'...') ───

    protected override string QuoteString(string value) =>
        "N'" + value.Replace("'", "''") + "'";

    // ── SqlDriverBase: driver initialisation ──────────────────────────────────

    protected override void InitialiseDriver(DriverConfig config) =>
        _connectionString = NormalizeUrl(config.Url);

    protected override async Task<DbConnection> OpenConnectionAsync(CancellationToken ct)
    {
        var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);
        return conn;
    }

    // ── SqlDriverBase: schema reflection ─────────────────────────────────────

    protected override string GetDatabaseNameSql() => "SELECT DB_NAME()";

    protected override string GetSchemaInfoSql() =>
        "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE " +
        "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG = @db";

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

        var cols      = schema.Columns();
        var colList   = string.Join(", ", cols.Select(c => QuoteId(c)));
        var valList   = string.Join(", ", cols.Select(c =>
            obj.TryGetValue(c, out var v) ? QuoteJsonElement(v) : "NULL"));
        var updateSet = string.Join(", ", cols.Where(c => c != "id")
            .Select(c => $"target.{QuoteId(c)} = source.{QuoteId(c)}"));
        var matchOn   = string.Join(" AND ",
            schema.PrimaryKeys.Select(pk => $"target.{QuoteId(pk)} = source.{QuoteId(pk)}"));

        return $"""
            MERGE {QuoteId(evt.Table)} AS target
            USING (SELECT {valList}) AS source ({colList})
            ON {matchOn}
            WHEN MATCHED THEN UPDATE SET {updateSet}
            WHEN NOT MATCHED THEN INSERT ({colList}) VALUES ({valList});

            """;
    }

    protected override string BuildCreateTableIfNotExistsSql(Schema schema)
    {
        var body = BuildCreateTableBody(schema);
        var escapedTable = schema.Table.Replace("'", "''");  // escape for N'...' literal
        return $"""
            IF OBJECT_ID(N'{escapedTable}', N'U') IS NULL
            BEGIN
            CREATE TABLE {QuoteId(schema.Table)} (
            {body}
            )
            END
            """;
    }

    protected override string BuildDropAndCreateTableSql(Schema schema)
    {
        var escapedTable = schema.Table.Replace("'", "''");  // escape for N'...' literal
        return $"IF OBJECT_ID(N'{escapedTable}', N'U') IS NOT NULL DROP TABLE {QuoteId(schema.Table)};\n" +
               $"CREATE TABLE {QuoteId(schema.Table)} (\n{BuildCreateTableBody(schema)}\n);";
    }

    // SQL Server uses ADD (not ADD COLUMN) and requires explicit NULL
    protected override string BuildAlterAddColumnSql(string table, string col, string sqlType) =>
        $"ALTER TABLE {QuoteId(table)} ADD {QuoteId(col)} {sqlType} NULL;";

    protected override string PropToSqlType(SchemaProperty prop, bool isPrimaryKey = false) => prop.Type switch
    {
        "string" when isPrimaryKey                => "NVARCHAR(255)",
        "string" when prop.Format == "date-time"  => "DATETIMEOFFSET",
        "string"                                  => "NVARCHAR(MAX)",
        "integer"                                 => "BIGINT",
        "number"                                  => "FLOAT",
        "boolean"                                 => "BIT",
        "object" or "array"                       => "NVARCHAR(MAX)",
        _                                         => "NVARCHAR(MAX)"
    };

    protected override string QuoteId(string name) => $"[{name.Replace("]", "]]")}]";

    // ── IDriverHelp: test + config ────────────────────────────────────────────

    public override async Task TestAsync(ILogger logger, string url, CancellationToken ct = default)
    {
        await using var conn = new SqlConnection(NormalizeUrl(url));
        await conn.OpenAsync(ct);
    }

    public override IReadOnlyList<DriverField> Configuration()
    {
        var fields = new List<DriverField>(DriverFieldHelpers.DatabaseFields(defaultPort: 0))
        {
            DriverFieldHelpers.OptionalNumber("Port", "The port number (default 1433)")
        };
        return fields;
    }

    public override (string url, IReadOnlyList<FieldError> errors) Validate(Dictionary<string, object?> values)
    {
        try
        {
            var hostname = DriverFieldHelpers.GetRequiredString("Hostname", values);
            var database = DriverFieldHelpers.GetRequiredString("Database", values);
            var username = DriverFieldHelpers.GetOptionalString("Username", string.Empty, values);
            var password = DriverFieldHelpers.GetOptionalString("Password", string.Empty, values);
            var port     = DriverFieldHelpers.GetOptionalInt("Port", 1433, values);
            var url      = $"sqlserver://{hostname}:{port}?database={Uri.EscapeDataString(database)}" +
                           (username.Length > 0
                               ? $"&user id={Uri.EscapeDataString(username)}&password={Uri.EscapeDataString(password)}"
                               : string.Empty);
            return (url, []);
        }
        catch (Exception ex)
        {
            return (string.Empty, [new FieldError { Field = "Hostname", Message = ex.Message }]);
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private static string NormalizeUrl(string url)
    {
        if (!url.StartsWith("sqlserver://", StringComparison.OrdinalIgnoreCase))
            return url;

        var uri = new Uri(url);
        var qs  = System.Web.HttpUtility.ParseQueryString(uri.Query);
        return new SqlConnectionStringBuilder
        {
            DataSource             = uri.Host + (uri.Port > 0 ? $",{uri.Port}" : string.Empty),
            InitialCatalog         = qs["database"] ?? string.Empty,
            UserID                 = qs["user id"]  ?? string.Empty,
            Password               = qs["password"] ?? string.Empty,
            TrustServerCertificate = true
        }.ConnectionString;
    }
}
