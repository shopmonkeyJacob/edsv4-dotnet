using EDS.Core.Abstractions;
using EDS.Core.Models;
using EDS.Drivers.MySQL;
using EDS.Drivers.PostgreSQL;
using EDS.Drivers.SqlServer;
using EDS.Integration.Tests.Helpers;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using MySqlConnector;
using Npgsql;

namespace EDS.Integration.Tests.Drivers;

// ══════════════════════════════════════════════════════════════════════════════
// PostgreSQL time-series tests
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Integration tests for SQL drivers operating in TimeSeries mode against a real
/// PostgreSQL container. Verifies events table creation, INSERT behaviour, and the
/// three auto-maintained views: current_{table}, {table}_history, {table}_unified.
/// </summary>
[Trait("Category", "Integration")]
public sealed class PostgreSqlTimeSeriesTests : IClassFixture<PostgreSqlFixture>
{
    private readonly string _connectionString;
    private static readonly Schema    Schema    = DriverTestHelpers.OrdersSchema("ts_pg_orders");
    private static readonly SchemaMap SchemaMap = DriverTestHelpers.ToSchemaMap(Schema);

    private const string EventsSchema = "eds_ts_pg_test";
    private const string EventsTable  = $"{EventsSchema}.ts_pg_orders_events";
    private const string CurrentView  = $"{EventsSchema}.current_ts_pg_orders";
    private const string HistoryView  = $"{EventsSchema}.ts_pg_orders_history";

    public PostgreSqlTimeSeriesTests(PostgreSqlFixture fixture) =>
        _connectionString = fixture.ConnectionString;

    private async Task<PostgreSqlDriver> StartDriverAsync()
    {
        // Drop the events schema so each test starts from a clean state.
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            $"DROP SCHEMA IF EXISTS \"{EventsSchema}\" CASCADE", conn);
        await cmd.ExecuteNonQueryAsync();

        var driver = new PostgreSqlDriver();
        await driver.StartAsync(new DriverConfig
        {
            Url            = _connectionString,
            Logger         = NullLogger.Instance,
            SchemaRegistry = new FakeSchemaRegistry(SchemaMap),
            Tracker        = null!,
            DataDir        = string.Empty,
            Mode           = DriverMode.TimeSeries,
            EventsSchema   = EventsSchema,
        });
        return driver;
    }

    private async Task<T?> QueryScalarAsync<T>(string sql)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(sql, conn);
        var result = await cmd.ExecuteScalarAsync();
        return result is DBNull ? default : (T?)result;
    }

    // ── Events table and view creation ────────────────────────────────────────

    [Fact]
    public async Task TimeSeries_EventsTable_IsCreated()
    {
        await StartDriverAsync();

        var count = await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM information_schema.tables " +
            $"WHERE table_schema = '{EventsSchema}' AND table_name = 'ts_pg_orders_events'");
        Assert.True(count > 0, "Events table should exist after StartAsync in TimeSeries mode.");
    }

    [Fact]
    public async Task TimeSeries_CurrentView_IsCreated()
    {
        await StartDriverAsync();

        var count = await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM information_schema.views " +
            $"WHERE table_schema = '{EventsSchema}' AND table_name = 'current_ts_pg_orders'");
        Assert.True(count > 0, "current_{table} view should exist after StartAsync in TimeSeries mode.");
    }

    [Fact]
    public async Task TimeSeries_HistoryView_IsCreated()
    {
        await StartDriverAsync();

        var count = await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM information_schema.views " +
            $"WHERE table_schema = '{EventsSchema}' AND table_name = 'ts_pg_orders_history'");
        Assert.True(count > 0, "{table}_history view should exist after StartAsync in TimeSeries mode.");
    }

    // ── Event INSERT and current view ─────────────────────────────────────────

    [Fact]
    public async Task TimeSeries_Insert_AppearsInEventsTable()
    {
        var driver   = await StartDriverAsync();
        var entityId = Guid.NewGuid().ToString();
        var evt      = DriverTestHelpers.MakeInsert(Schema.Table, entityId, new { id = entityId, name = "Acme" });

        await driver.ProcessAsync(NullLogger.Instance, evt);
        await driver.FlushAsync(NullLogger.Instance);

        var count = await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM \"{EventsTable}\" WHERE _entity_id = '{entityId}'");
        Assert.Equal(1L, count);
    }

    [Fact]
    public async Task TimeSeries_CurrentView_ShowsLatestInsert()
    {
        var driver   = await StartDriverAsync();
        var entityId = Guid.NewGuid().ToString();
        var evt      = DriverTestHelpers.MakeInsert(Schema.Table, entityId, new { id = entityId, name = "Widget" });

        await driver.ProcessAsync(NullLogger.Instance, evt);
        await driver.FlushAsync(NullLogger.Instance);

        var count = await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM \"{CurrentView}\" WHERE _entity_id = '{entityId}'");
        Assert.Equal(1L, count);
    }

    [Fact]
    public async Task TimeSeries_MultipleEventsForSameEntity_CurrentView_ShowsOnlyOne()
    {
        var driver   = await StartDriverAsync();
        var entityId = Guid.NewGuid().ToString();

        // INSERT then UPDATE — current view must still show exactly one row.
        await driver.ProcessAsync(NullLogger.Instance,
            DriverTestHelpers.MakeInsert(Schema.Table, entityId, new { id = entityId, name = "v1" }));
        await driver.ProcessAsync(NullLogger.Instance,
            DriverTestHelpers.MakeUpdate(Schema.Table, entityId, new { id = entityId, name = "v2" }));
        await driver.FlushAsync(NullLogger.Instance);

        var count = await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM \"{CurrentView}\" WHERE _entity_id = '{entityId}'");
        Assert.Equal(1L, count);

        var histCount = await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM \"{EventsTable}\" WHERE _entity_id = '{entityId}'");
        Assert.Equal(2L, histCount);
    }

    [Fact]
    public async Task TimeSeries_Delete_DisappearsFromCurrentView()
    {
        var driver   = await StartDriverAsync();
        var entityId = Guid.NewGuid().ToString();

        await driver.ProcessAsync(NullLogger.Instance,
            DriverTestHelpers.MakeInsert(Schema.Table, entityId, new { id = entityId, name = "Gone" }));
        await driver.FlushAsync(NullLogger.Instance);

        var before = await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM \"{CurrentView}\" WHERE _entity_id = '{entityId}'");
        Assert.Equal(1L, before);

        await driver.ProcessAsync(NullLogger.Instance,
            DriverTestHelpers.MakeDelete(Schema.Table, entityId));
        await driver.FlushAsync(NullLogger.Instance);

        var after = await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM \"{CurrentView}\" WHERE _entity_id = '{entityId}'");
        Assert.Equal(0L, after);
    }

    [Fact]
    public async Task TimeSeries_HistoryView_ProjectsJsonColumns()
    {
        var driver   = await StartDriverAsync();
        var entityId = Guid.NewGuid().ToString();
        var evt      = DriverTestHelpers.MakeInsert(Schema.Table, entityId, new { id = entityId, name = "HistTest" });

        await driver.ProcessAsync(NullLogger.Instance, evt);
        await driver.FlushAsync(NullLogger.Instance);

        // The history view should expose the "name" column extracted from _after JSON.
        var name = await QueryScalarAsync<string>(
            $"SELECT name FROM \"{HistoryView}\" WHERE _entity_id = '{entityId}' LIMIT 1");
        Assert.Equal("HistTest", name);
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// MySQL time-series tests
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Integration tests for the MySQL driver in TimeSeries mode.
/// MySQL has no schema concept, so events tables use the <c>__events</c> suffix
/// in the same database.
/// </summary>
[Trait("Category", "Integration")]
public sealed class MySqlTimeSeriesTests : IClassFixture<MySqlFixture>
{
    private readonly string _mysqlUrl;
    private static readonly Schema    Schema    = DriverTestHelpers.OrdersSchema("ts_my_orders");
    private static readonly SchemaMap SchemaMap = DriverTestHelpers.ToSchemaMap(Schema);

    // MySQL uses __events suffix in same DB (no schema concept).
    private const string EventsTable = "ts_my_orders__events";
    private const string CurrentView = "current_ts_my_orders";
    private const string HistoryView = "ts_my_orders_history";

    public MySqlTimeSeriesTests(MySqlFixture fixture) => _mysqlUrl = fixture.MySqlUrl;

    private string MysqlConnectionString()
    {
        var uri      = new Uri(_mysqlUrl);
        var userInfo = uri.UserInfo.Split(':', 2);
        return new MySqlConnectionStringBuilder
        {
            Server   = uri.Host,
            Port     = (uint)(uri.Port > 0 ? uri.Port : 3306),
            Database = uri.AbsolutePath.TrimStart('/'),
            UserID   = Uri.UnescapeDataString(userInfo[0]),
            Password = userInfo.Length > 1 ? Uri.UnescapeDataString(userInfo[1]) : string.Empty,
        }.ConnectionString;
    }

    private async Task<MySqlDriver> StartDriverAsync()
    {
        // Drop the events table and views before each test.
        await using var conn = new MySqlConnection(MysqlConnectionString());
        await conn.OpenAsync();
        foreach (var obj in new[] { $"DROP VIEW IF EXISTS `{CurrentView}`",
                                    $"DROP VIEW IF EXISTS `{HistoryView}`",
                                    $"DROP TABLE IF EXISTS `{EventsTable}`" })
        {
            await using var cmd = new MySqlCommand(obj, conn);
            await cmd.ExecuteNonQueryAsync();
        }

        var driver = new MySqlDriver();
        await driver.StartAsync(new DriverConfig
        {
            Url            = _mysqlUrl,
            Logger         = NullLogger.Instance,
            SchemaRegistry = new FakeSchemaRegistry(SchemaMap),
            Tracker        = null!,
            DataDir        = string.Empty,
            Mode           = DriverMode.TimeSeries,
            EventsSchema   = "eds_events",   // ignored for MySQL (no schema concept)
        });
        return driver;
    }

    private async Task<T?> QueryScalarAsync<T>(string sql)
    {
        await using var conn = new MySqlConnection(MysqlConnectionString());
        await conn.OpenAsync();
        await using var cmd = new MySqlCommand(sql, conn);
        var result = await cmd.ExecuteScalarAsync();
        if (result is DBNull || result is null) return default;
        return (T?)Convert.ChangeType(result, typeof(T));
    }

    // ── Events table and view creation ────────────────────────────────────────

    [Fact]
    public async Task MySql_TimeSeries_EventsTable_IsCreated()
    {
        await StartDriverAsync();

        var count = await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM information_schema.tables " +
            $"WHERE table_schema = DATABASE() AND table_name = '{EventsTable}'");
        Assert.True(count > 0, "Events table should exist after StartAsync in TimeSeries mode.");
    }

    [Fact]
    public async Task MySql_TimeSeries_CurrentView_IsCreated()
    {
        await StartDriverAsync();

        var count = await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM information_schema.views " +
            $"WHERE table_schema = DATABASE() AND table_name = '{CurrentView}'");
        Assert.True(count > 0, "current_{table} view should exist after StartAsync in TimeSeries mode.");
    }

    // ── Event INSERT and current view ─────────────────────────────────────────

    [Fact]
    public async Task MySql_TimeSeries_Insert_AppearsInEventsTable()
    {
        var driver   = await StartDriverAsync();
        var entityId = Guid.NewGuid().ToString();
        var evt      = DriverTestHelpers.MakeInsert(Schema.Table, entityId, new { id = entityId, name = "Acme" });

        await driver.ProcessAsync(NullLogger.Instance, evt);
        await driver.FlushAsync(NullLogger.Instance);

        var count = await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM `{EventsTable}` WHERE _entity_id = '{entityId}'");
        Assert.Equal(1L, count);
    }

    [Fact]
    public async Task MySql_TimeSeries_CurrentView_ShowsLatestInsert()
    {
        var driver   = await StartDriverAsync();
        var entityId = Guid.NewGuid().ToString();
        var evt      = DriverTestHelpers.MakeInsert(Schema.Table, entityId, new { id = entityId, name = "Widget" });

        await driver.ProcessAsync(NullLogger.Instance, evt);
        await driver.FlushAsync(NullLogger.Instance);

        var count = await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM `{CurrentView}` WHERE _entity_id = '{entityId}'");
        Assert.Equal(1L, count);
    }

    [Fact]
    public async Task MySql_TimeSeries_Delete_DisappearsFromCurrentView()
    {
        var driver   = await StartDriverAsync();
        var entityId = Guid.NewGuid().ToString();

        await driver.ProcessAsync(NullLogger.Instance,
            DriverTestHelpers.MakeInsert(Schema.Table, entityId, new { id = entityId, name = "Gone" }));
        await driver.FlushAsync(NullLogger.Instance);

        await driver.ProcessAsync(NullLogger.Instance,
            DriverTestHelpers.MakeDelete(Schema.Table, entityId));
        await driver.FlushAsync(NullLogger.Instance);

        var after = await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM `{CurrentView}` WHERE _entity_id = '{entityId}'");
        Assert.Equal(0L, after);
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// SQL Server time-series tests
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Integration tests for the SQL Server driver in TimeSeries mode.
/// </summary>
[Trait("Category", "Integration")]
public sealed class SqlServerTimeSeriesTests : IClassFixture<SqlServerFixture>
{
    private readonly string _connectionString;
    private readonly string _driverUrl;
    private static readonly Schema    Schema    = DriverTestHelpers.OrdersSchema("ts_ss_orders");
    private static readonly SchemaMap SchemaMap = DriverTestHelpers.ToSchemaMap(Schema);

    private const string EventsSchema = "eds_ts_ss_test";
    private const string EventsTable  = $"[{EventsSchema}].[ts_ss_orders_events]";
    private const string CurrentView  = $"[{EventsSchema}].[current_ts_ss_orders]";
    private const string HistoryView  = $"[{EventsSchema}].[ts_ss_orders_history]";

    public SqlServerTimeSeriesTests(SqlServerFixture fixture)
    {
        _connectionString = fixture.ConnectionString;
        _driverUrl        = ToDriverUrl(_connectionString);
    }

    private async Task<SqlServerDriver> StartDriverAsync()
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(
            $"IF SCHEMA_ID('{EventsSchema}') IS NOT NULL " +
            $"  BEGIN " +
            $"    IF OBJECT_ID(N'[{EventsSchema}].[current_ts_ss_orders]', N'V') IS NOT NULL DROP VIEW [{EventsSchema}].[current_ts_ss_orders];" +
            $"    IF OBJECT_ID(N'[{EventsSchema}].[ts_ss_orders_history]', N'V') IS NOT NULL DROP VIEW [{EventsSchema}].[ts_ss_orders_history];" +
            $"    IF OBJECT_ID(N'[{EventsSchema}].[ts_ss_orders_unified]', N'V') IS NOT NULL DROP VIEW [{EventsSchema}].[ts_ss_orders_unified];" +
            $"    IF OBJECT_ID(N'[{EventsSchema}].[ts_ss_orders_events]', N'U')  IS NOT NULL DROP TABLE [{EventsSchema}].[ts_ss_orders_events];" +
            $"    DROP SCHEMA [{EventsSchema}]" +
            $"  END", conn);
        await cmd.ExecuteNonQueryAsync();

        var driver = new SqlServerDriver();
        await driver.StartAsync(new DriverConfig
        {
            Url            = _driverUrl,
            Logger         = NullLogger.Instance,
            SchemaRegistry = new FakeSchemaRegistry(SchemaMap),
            Tracker        = null!,
            DataDir        = string.Empty,
            Mode           = DriverMode.TimeSeries,
            EventsSchema   = EventsSchema,
        });
        return driver;
    }

    private async Task<T?> QueryScalarAsync<T>(string sql)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        var result = await cmd.ExecuteScalarAsync();
        if (result is DBNull || result is null) return default;
        return (T?)Convert.ChangeType(result, typeof(T));
    }

    private static string ToDriverUrl(string connStr)
    {
        var parts = connStr.Split(';')
            .Select(p => p.Split('=', 2))
            .Where(p => p.Length == 2)
            .ToDictionary(p => p[0].Trim(), p => p[1].Trim(), StringComparer.OrdinalIgnoreCase);
        var server   = parts.GetValueOrDefault("Server", "localhost");
        var hostPort = server.Split(',');
        var host     = hostPort[0];
        var port     = hostPort.Length > 1 ? hostPort[1] : "1433";
        var user     = Uri.EscapeDataString(parts.GetValueOrDefault("User Id", "sa"));
        var pwd      = Uri.EscapeDataString(parts.GetValueOrDefault("Password", ""));
        return $"sqlserver://{host}:{port}?database=master" +
               $"&user id={user}&password={pwd}&trust-server-certificate=true";
    }

    // ── Events table and view creation ────────────────────────────────────────

    [Fact]
    public async Task SqlServer_TimeSeries_EventsTable_IsCreated()
    {
        await StartDriverAsync();

        var count = await QueryScalarAsync<int>(
            $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES " +
            $"WHERE TABLE_SCHEMA = '{EventsSchema}' AND TABLE_NAME = 'ts_ss_orders_events'");
        Assert.True(count > 0, "Events table should exist after StartAsync in TimeSeries mode.");
    }

    [Fact]
    public async Task SqlServer_TimeSeries_CurrentView_IsCreated()
    {
        await StartDriverAsync();

        var count = await QueryScalarAsync<int>(
            $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS " +
            $"WHERE TABLE_SCHEMA = '{EventsSchema}' AND TABLE_NAME = 'current_ts_ss_orders'");
        Assert.True(count > 0, "current_{table} view should exist after StartAsync in TimeSeries mode.");
    }

    // ── Event INSERT and current view ─────────────────────────────────────────

    [Fact]
    public async Task SqlServer_TimeSeries_Insert_AppearsInEventsTable()
    {
        var driver   = await StartDriverAsync();
        var entityId = Guid.NewGuid().ToString();
        var evt      = DriverTestHelpers.MakeInsert(Schema.Table, entityId, new { id = entityId, name = "Acme" });

        await driver.ProcessAsync(NullLogger.Instance, evt);
        await driver.FlushAsync(NullLogger.Instance);

        var count = await QueryScalarAsync<int>(
            $"SELECT COUNT(*) FROM {EventsTable} WHERE _entity_id = '{entityId}'");
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task SqlServer_TimeSeries_CurrentView_ShowsLatestInsert()
    {
        var driver   = await StartDriverAsync();
        var entityId = Guid.NewGuid().ToString();
        var evt      = DriverTestHelpers.MakeInsert(Schema.Table, entityId, new { id = entityId, name = "Widget" });

        await driver.ProcessAsync(NullLogger.Instance, evt);
        await driver.FlushAsync(NullLogger.Instance);

        var count = await QueryScalarAsync<int>(
            $"SELECT COUNT(*) FROM {CurrentView} WHERE _entity_id = '{entityId}'");
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task SqlServer_TimeSeries_MultipleEventsForSameEntity_CurrentView_ShowsOnlyOne()
    {
        var driver   = await StartDriverAsync();
        var entityId = Guid.NewGuid().ToString();

        await driver.ProcessAsync(NullLogger.Instance,
            DriverTestHelpers.MakeInsert(Schema.Table, entityId, new { id = entityId, name = "v1" }));
        await driver.ProcessAsync(NullLogger.Instance,
            DriverTestHelpers.MakeUpdate(Schema.Table, entityId, new { id = entityId, name = "v2" }));
        await driver.FlushAsync(NullLogger.Instance);

        var currentCount = await QueryScalarAsync<int>(
            $"SELECT COUNT(*) FROM {CurrentView} WHERE _entity_id = '{entityId}'");
        Assert.Equal(1, currentCount);

        var eventsCount = await QueryScalarAsync<int>(
            $"SELECT COUNT(*) FROM {EventsTable} WHERE _entity_id = '{entityId}'");
        Assert.Equal(2, eventsCount);
    }

    [Fact]
    public async Task SqlServer_TimeSeries_Delete_DisappearsFromCurrentView()
    {
        var driver   = await StartDriverAsync();
        var entityId = Guid.NewGuid().ToString();

        await driver.ProcessAsync(NullLogger.Instance,
            DriverTestHelpers.MakeInsert(Schema.Table, entityId, new { id = entityId, name = "Gone" }));
        await driver.FlushAsync(NullLogger.Instance);

        var before = await QueryScalarAsync<int>(
            $"SELECT COUNT(*) FROM {CurrentView} WHERE _entity_id = '{entityId}'");
        Assert.Equal(1, before);

        await driver.ProcessAsync(NullLogger.Instance,
            DriverTestHelpers.MakeDelete(Schema.Table, entityId));
        await driver.FlushAsync(NullLogger.Instance);

        var after = await QueryScalarAsync<int>(
            $"SELECT COUNT(*) FROM {CurrentView} WHERE _entity_id = '{entityId}'");
        Assert.Equal(0, after);
    }

    [Fact]
    public async Task SqlServer_TimeSeries_HistoryView_ProjectsJsonColumns()
    {
        var driver   = await StartDriverAsync();
        var entityId = Guid.NewGuid().ToString();
        var evt      = DriverTestHelpers.MakeInsert(Schema.Table, entityId, new { id = entityId, name = "HistTest" });

        await driver.ProcessAsync(NullLogger.Instance, evt);
        await driver.FlushAsync(NullLogger.Instance);

        var name = await QueryScalarAsync<string>(
            $"SELECT TOP 1 name FROM {HistoryView} WHERE _entity_id = '{entityId}'");
        Assert.Equal("HistTest", name);
    }
}
