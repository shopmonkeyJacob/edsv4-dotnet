using EDS.Core.Abstractions;
using EDS.Core.Helpers;
using EDS.Core.Models;
using Microsoft.Extensions.Logging;
using Snowflake.Data.Client;
using System.Text;
using System.Text.Json;

namespace EDS.Drivers.Snowflake;

/// <summary>
/// Streams CDC events into Snowflake using MERGE statements.
/// Mirrors the Go snowflake driver with session ID correlation and batch limiting.
/// </summary>
public sealed class SnowflakeDriver : IDriver, IDriverLifecycle, IDriverHelp, IDriverSessionHandler
{
    private ISchemaRegistry? _registry;
    private string? _connectionString;
    private ILogger? _logger;
    private string _sessionId = string.Empty;
    private readonly List<(DbChangeEvent evt, Schema schema)> _pending = new();

    public string Name => "Snowflake";
    public string Description => "Streams EDS messages into a Snowflake database.";
    public string ExampleUrl => "snowflake://user:password@account/database/schema?warehouse=COMPUTE_WH";
    public string Help => "Events are merged into Snowflake tables using MERGE statements. " +
                          "The Snowflake account identifier must be in the form account.region.cloud.";

    public int MaxBatchSize => 200;

    public void SetSessionId(string sessionId) => _sessionId = sessionId;

    public Task StartAsync(DriverConfig config, CancellationToken ct = default)
    {
        _logger = config.Logger;
        _registry = config.SchemaRegistry;
        _connectionString = BuildConnectionString(config.Url);
        return Task.CompletedTask;
    }

    public async Task<bool> ProcessAsync(ILogger logger, DbChangeEvent evt, CancellationToken ct = default)
    {
        var schema = await _registry!.GetSchemaAsync(evt.Table, evt.ModelVersion, ct)
            ?? throw new InvalidOperationException($"Schema not found for {evt.Table} v{evt.ModelVersion}");
        _pending.Add((evt, schema));
        return _pending.Count >= MaxBatchSize;
    }

    public async Task FlushAsync(ILogger logger, CancellationToken ct = default)
    {
        if (_pending.Count == 0) return;

        using var conn = new SnowflakeDbConnection { ConnectionString = _connectionString };
        await conn.OpenAsync(ct);

        // Group by table to merge per-table
        var byTable = _pending.GroupBy(p => p.evt.Table);
        foreach (var group in byTable)
        {
            var schema = group.First().schema;
            var events = group.Select(g => g.evt).ToList();
            await MergeEventsAsync(conn, schema, events, ct);
        }

        logger.LogDebug("Flushed {Count} records to Snowflake.", _pending.Count);
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
        DriverFieldHelpers.RequiredString("Account", "Snowflake account identifier (e.g. myorg-myaccount)"),
        DriverFieldHelpers.RequiredString("Database", "Database name"),
        DriverFieldHelpers.RequiredString("Schema", "Schema name", "public"),
        DriverFieldHelpers.RequiredString("Warehouse", "Warehouse name"),
        DriverFieldHelpers.OptionalString("Username", "Username"),
        DriverFieldHelpers.OptionalPassword("Password", "Password"),
        DriverFieldHelpers.OptionalString("Role", "Role to use")
    ];

    public (string url, IReadOnlyList<FieldError> errors) Validate(Dictionary<string, object?> values)
    {
        try
        {
            var account = DriverFieldHelpers.GetRequiredString("Account", values);
            var database = DriverFieldHelpers.GetRequiredString("Database", values);
            var schema = DriverFieldHelpers.GetOptionalString("Schema", "public", values);
            var warehouse = DriverFieldHelpers.GetRequiredString("Warehouse", values);
            var username = DriverFieldHelpers.GetOptionalString("Username", string.Empty, values);
            var password = DriverFieldHelpers.GetOptionalString("Password", string.Empty, values);
            var url = $"snowflake://{username}:{password}@{account}/{database}/{schema}?warehouse={Uri.EscapeDataString(warehouse)}";
            return (url, []);
        }
        catch (Exception ex)
        {
            return (string.Empty, [new FieldError { Field = "Account", Message = ex.Message }]);
        }
    }

    // ─── SQL generation ───────────────────────────────────────────────────────

    private static async Task MergeEventsAsync(
        SnowflakeDbConnection conn,
        Schema schema,
        IReadOnlyList<DbChangeEvent> events,
        CancellationToken ct)
    {
        foreach (var evt in events)
        {
            string sql;
            if (evt.Operation.Equals("DELETE", StringComparison.OrdinalIgnoreCase))
            {
                var where = string.Join(" AND ",
                    schema.PrimaryKeys.Select((pk, i) =>
                        $"{QuoteSnowId(pk)} = {QuoteLiteral(evt.Key.Length > i + 1 ? evt.Key[i + 1] : string.Empty)}"));
                sql = $"DELETE FROM {QuoteSnowId(schema.Table)} WHERE {where};";
            }
            else
            {
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

                sql = $"""
                    MERGE INTO {QuoteSnowId(schema.Table)} t
                    USING (SELECT {valList}) s ({colList})
                    ON {pks}
                    WHEN MATCHED THEN UPDATE SET {updates}
                    WHEN NOT MATCHED THEN INSERT ({colList}) VALUES ({valList});
                    """;
            }

            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    /// <summary>Quotes a Snowflake identifier: wraps in double-quotes and escapes any embedded double-quotes as "".</summary>
    private static string QuoteSnowId(string name) => $"\"{name.Replace("\"", "\"\"")}\"";

    private static string QuoteLiteral(string value) => "'" + value.Replace("'", "''") + "'";

    private static string QuoteJsonElement(JsonElement el) => el.ValueKind switch
    {
        JsonValueKind.Null or JsonValueKind.Undefined => "NULL",
        JsonValueKind.True  => "TRUE",
        JsonValueKind.False => "FALSE",
        JsonValueKind.Number => SqlHelpers.IsValidNumericLiteral(el.GetRawText()) ? el.GetRawText() : "NULL",
        JsonValueKind.String => QuoteLiteral(el.GetString() ?? string.Empty),
        _                    => QuoteLiteral(el.GetRawText())
    };

    private static string BuildConnectionString(string url)
    {
        var uri = new Uri(url);
        var qs = System.Web.HttpUtility.ParseQueryString(uri.Query);
        var parts = uri.AbsolutePath.TrimStart('/').Split('/');
        var database = parts.Length > 0 ? parts[0] : string.Empty;
        var schema = parts.Length > 1 ? parts[1] : "public";
        var userInfo = uri.UserInfo.Split(':', 2);

        return new StringBuilder()
            .Append($"account={uri.Host};")
            .Append($"user={Uri.UnescapeDataString(userInfo.Length > 0 ? userInfo[0] : string.Empty)};")
            .Append($"password={Uri.UnescapeDataString(userInfo.Length > 1 ? userInfo[1] : string.Empty)};")
            .Append($"db={database};")
            .Append($"schema={schema};")
            .Append(qs["warehouse"] is { } wh ? $"warehouse={wh};" : string.Empty)
            .Append(qs["role"] is { } role ? $"role={role};" : string.Empty)
            .ToString();
    }
}
