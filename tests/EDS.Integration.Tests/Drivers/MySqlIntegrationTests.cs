using EDS.Core.Abstractions;
using EDS.Core.Models;
using EDS.Drivers.MySQL;
using EDS.Integration.Tests.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using MySqlConnector;
using Testcontainers.MySql;

namespace EDS.Integration.Tests.Drivers;

/// <summary>
/// Integration tests that spin up a real MySQL container via TestContainers,
/// push DbChangeEvents through the full ProcessAsync → FlushAsync pipeline, and
/// verify the persisted values are exactly correct.
/// </summary>
[Trait("Category", "Integration")]
public sealed class MySqlIntegrationTests : IClassFixture<MySqlFixture>
{
    private readonly string _mysqlUrl;
    private static readonly Schema Schema = DriverTestHelpers.OrdersSchema("eds_mysql_orders");
    private static readonly SchemaMap SchemaMap = DriverTestHelpers.ToSchemaMap(Schema);

    public MySqlIntegrationTests(MySqlFixture fixture) => _mysqlUrl = fixture.MySqlUrl;

    // ── Driver lifecycle helpers ──────────────────────────────────────────────

    private async Task<MySqlDriver> StartDriverAsync()
    {
        var driver = new MySqlDriver();
        await driver.StartAsync(new DriverConfig
        {
            Url            = _mysqlUrl,
            Logger         = NullLogger.Instance,
            SchemaRegistry = new FakeSchemaRegistry(SchemaMap),
            Tracker        = null!,
            DataDir        = string.Empty,
        });
        return driver;
    }

    private async Task<T?> QueryScalarAsync<T>(string sql)
    {
        var connString = MysqlUrlToConnectionString(_mysqlUrl);
        await using var conn = new MySqlConnection(connString);
        await conn.OpenAsync();
        await using var cmd = new MySqlCommand(sql, conn);
        var result = await cmd.ExecuteScalarAsync();
        if (result is DBNull || result is null) return default;
        return (T?)Convert.ChangeType(result, typeof(T));
    }

    private static string MysqlUrlToConnectionString(string url)
    {
        var uri      = new Uri(url);
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

    private async Task<bool> RowExistsAsync(string id) =>
        await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM eds_mysql_orders WHERE id = '{id.Replace("'", "''")}'"
        ) > 0;

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Insert_BasicRow_AppearsInDatabase()
    {
        var id     = Guid.NewGuid().ToString();
        var evt    = DriverTestHelpers.MakeInsert(Schema.Table, id, new { id, name = "Widget" });
        var driver = await StartDriverAsync();
        await driver.ProcessAsync(NullLogger.Instance, evt);
        await driver.FlushAsync(NullLogger.Instance);
        await driver.StopAsync();

        Assert.True(await RowExistsAsync(id));
    }

    [Fact]
    public async Task Insert_StringWithSingleQuote_StoredCorrectly()
    {
        var id     = Guid.NewGuid().ToString();
        var name   = "O'Brien's Shop";
        var evt    = DriverTestHelpers.MakeInsert(Schema.Table, id, new { id, name });
        var driver = await StartDriverAsync();
        await driver.ProcessAsync(NullLogger.Instance, evt);
        await driver.FlushAsync(NullLogger.Instance);
        await driver.StopAsync();

        var stored = await QueryScalarAsync<string>(
            $"SELECT name FROM eds_mysql_orders WHERE id = '{id}'");
        Assert.Equal(name, stored);
    }

    [Fact]
    public async Task Insert_SqlInjectionInValue_StoredLiterally()
    {
        var id        = Guid.NewGuid().ToString();
        var injection = "'; DROP TABLE eds_mysql_orders; --";
        var evt       = DriverTestHelpers.MakeInsert(Schema.Table, id, new { id, name = injection });
        var driver    = await StartDriverAsync();
        await driver.ProcessAsync(NullLogger.Instance, evt);
        await driver.FlushAsync(NullLogger.Instance);   // must not throw
        await driver.StopAsync();

        var stored = await QueryScalarAsync<string>(
            $"SELECT name FROM eds_mysql_orders WHERE id = '{id}'");
        Assert.Equal(injection, stored);
    }

    [Fact]
    public async Task Insert_UnicodeValue_StoredCorrectly()
    {
        var id     = Guid.NewGuid().ToString();
        var name   = "こんにちは 🎉 Ünïcödé";
        var evt    = DriverTestHelpers.MakeInsert(Schema.Table, id, new { id, name });
        var driver = await StartDriverAsync();
        await driver.ProcessAsync(NullLogger.Instance, evt);
        await driver.FlushAsync(NullLogger.Instance);
        await driver.StopAsync();

        var stored = await QueryScalarAsync<string>(
            $"SELECT name FROM eds_mysql_orders WHERE id = '{id}'");
        Assert.Equal(name, stored);
    }

    [Fact]
    public async Task Insert_DateTimeValue_StoredCorrectly()
    {
        var id      = Guid.NewGuid().ToString();
        var isoDate = "2024-03-15T14:30:00Z";
        var evt     = DriverTestHelpers.MakeInsert(Schema.Table, id, new { id, createdAt = isoDate });
        var driver  = await StartDriverAsync();
        await driver.ProcessAsync(NullLogger.Instance, evt);
        await driver.FlushAsync(NullLogger.Instance);
        await driver.StopAsync();

        var stored = await QueryScalarAsync<DateTime>(
            $"SELECT createdAt FROM eds_mysql_orders WHERE id = '{id}'");
        Assert.Equal(new DateTime(2024, 3, 15, 14, 30, 0, DateTimeKind.Utc), stored);
    }

    [Fact]
    public async Task Insert_NullValue_ColumnIsNullInDatabase()
    {
        var id     = Guid.NewGuid().ToString();
        var evt    = DriverTestHelpers.MakeInsert(Schema.Table, id, new { id, name = (string?)null });
        var driver = await StartDriverAsync();
        await driver.ProcessAsync(NullLogger.Instance, evt);
        await driver.FlushAsync(NullLogger.Instance);
        await driver.StopAsync();

        var stored = await QueryScalarAsync<string>(
            $"SELECT name FROM eds_mysql_orders WHERE id = '{id}'");
        Assert.Null(stored);
    }

    [Fact]
    public async Task Update_ExistingRow_ValueIsUpdated()
    {
        var id     = Guid.NewGuid().ToString();
        var driver = await StartDriverAsync();

        await driver.ProcessAsync(NullLogger.Instance,
            DriverTestHelpers.MakeInsert(Schema.Table, id, new { id, name = "Original" }));
        await driver.FlushAsync(NullLogger.Instance);

        await driver.ProcessAsync(NullLogger.Instance,
            DriverTestHelpers.MakeUpdate(Schema.Table, id, new { id, name = "Updated" }));
        await driver.FlushAsync(NullLogger.Instance);
        await driver.StopAsync();

        var stored = await QueryScalarAsync<string>(
            $"SELECT name FROM eds_mysql_orders WHERE id = '{id}'");
        Assert.Equal("Updated", stored);
    }

    [Fact]
    public async Task Delete_ExistingRow_RowIsRemoved()
    {
        var id     = Guid.NewGuid().ToString();
        var driver = await StartDriverAsync();

        await driver.ProcessAsync(NullLogger.Instance,
            DriverTestHelpers.MakeInsert(Schema.Table, id, new { id, name = "To Delete" }));
        await driver.FlushAsync(NullLogger.Instance);

        Assert.True(await RowExistsAsync(id));

        await driver.ProcessAsync(NullLogger.Instance,
            DriverTestHelpers.MakeDelete(Schema.Table, id));
        await driver.FlushAsync(NullLogger.Instance);
        await driver.StopAsync();

        Assert.False(await RowExistsAsync(id));
    }

    [Fact]
    public async Task Upsert_SameId_NoDuplicateRow()
    {
        var id     = Guid.NewGuid().ToString();
        var driver = await StartDriverAsync();

        await driver.ProcessAsync(NullLogger.Instance,
            DriverTestHelpers.MakeInsert(Schema.Table, id, new { id, name = "First" }));
        await driver.FlushAsync(NullLogger.Instance);

        await driver.ProcessAsync(NullLogger.Instance,
            DriverTestHelpers.MakeInsert(Schema.Table, id, new { id, name = "Second" }));
        await driver.FlushAsync(NullLogger.Instance);
        await driver.StopAsync();

        var name  = await QueryScalarAsync<string>($"SELECT name     FROM eds_mysql_orders WHERE id = '{id}'");
        var count = await QueryScalarAsync<long>  ($"SELECT COUNT(*) FROM eds_mysql_orders WHERE id = '{id}'");

        Assert.Equal("Second", name);
        Assert.Equal(1L, count);
    }
}

// ── TestContainers fixture ────────────────────────────────────────────────────

public sealed class MySqlFixture : IAsyncLifetime
{
    private const string DbName   = "testdb";
    private const string User     = "root";
    private const string Password = "testpass";

    private readonly MySqlContainer _container = new MySqlBuilder()
        .WithImage("mysql:8.0")
        .WithDatabase(DbName)
        .WithUsername(User)
        .WithPassword(Password)
        .Build();

    public string MySqlUrl { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        var port = _container.GetMappedPublicPort(3306);
        MySqlUrl = $"mysql://{User}:{Password}@{_container.Hostname}:{port}/{DbName}";
    }

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}
