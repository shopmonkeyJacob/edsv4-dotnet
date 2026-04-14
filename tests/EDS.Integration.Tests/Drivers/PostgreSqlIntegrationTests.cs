using EDS.Core.Abstractions;
using EDS.Core.Models;
using EDS.Drivers.PostgreSQL;
using EDS.Integration.Tests.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace EDS.Integration.Tests.Drivers;

/// <summary>
/// Integration tests that spin up a real PostgreSQL container via TestContainers,
/// push DbChangeEvents through the full ProcessAsync → FlushAsync pipeline, and
/// verify the persisted values are exactly correct.
/// </summary>
[Trait("Category", "Integration")]
public sealed class PostgreSqlIntegrationTests : IClassFixture<PostgreSqlFixture>
{
    private readonly string _connectionString;
    private static readonly Schema Schema = DriverTestHelpers.OrdersSchema();
    private static readonly SchemaMap SchemaMap = DriverTestHelpers.ToSchemaMap(Schema);

    public PostgreSqlIntegrationTests(PostgreSqlFixture fixture) =>
        _connectionString = fixture.ConnectionString;

    // ── Driver lifecycle helpers ──────────────────────────────────────────────

    private async Task<PostgreSqlDriver> StartDriverAsync()
    {
        var driver = new PostgreSqlDriver();
        await driver.StartAsync(new DriverConfig
        {
            Url            = _connectionString,
            Logger         = NullLogger.Instance,
            SchemaRegistry = new FakeSchemaRegistry(SchemaMap),
            Tracker        = null!,
            DataDir        = string.Empty,
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

    private async Task<bool> RowExistsAsync(string id) =>
        await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM eds_test_orders WHERE id = '{id.Replace("'", "''")}'"
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
            $"SELECT name FROM eds_test_orders WHERE id = '{id}'");
        Assert.Equal(name, stored);
    }

    [Fact]
    public async Task Insert_SqlInjectionInValue_StoredLiterally()
    {
        var id        = Guid.NewGuid().ToString();
        var injection = "'; DROP TABLE eds_test_orders; --";
        var evt       = DriverTestHelpers.MakeInsert(Schema.Table, id, new { id, name = injection });
        var driver    = await StartDriverAsync();
        await driver.ProcessAsync(NullLogger.Instance, evt);
        await driver.FlushAsync(NullLogger.Instance);   // must not throw
        await driver.StopAsync();

        // Table must still exist and row must contain the injection string literally
        var stored = await QueryScalarAsync<string>(
            $"SELECT name FROM eds_test_orders WHERE id = '{id}'");
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
            $"SELECT name FROM eds_test_orders WHERE id = '{id}'");
        Assert.Equal(name, stored);
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
            $"SELECT name FROM eds_test_orders WHERE id = '{id}'");
        Assert.Null(stored);
    }

    [Fact]
    public async Task Insert_NumericValues_StoredCorrectly()
    {
        var id     = Guid.NewGuid().ToString();
        var evt    = DriverTestHelpers.MakeInsert(Schema.Table, id,
                        new { id, amount = 123.45, qty = 7 });
        var driver = await StartDriverAsync();
        await driver.ProcessAsync(NullLogger.Instance, evt);
        await driver.FlushAsync(NullLogger.Instance);
        await driver.StopAsync();

        var amount = await QueryScalarAsync<double>(
            $"SELECT amount FROM eds_test_orders WHERE id = '{id}'");
        var qty = await QueryScalarAsync<long>(
            $"SELECT qty FROM eds_test_orders WHERE id = '{id}'");

        Assert.Equal(123.45, amount, precision: 4);
        Assert.Equal(7L, qty);
    }

    [Fact]
    public async Task Insert_BooleanValue_StoredCorrectly()
    {
        var id     = Guid.NewGuid().ToString();
        var evt    = DriverTestHelpers.MakeInsert(Schema.Table, id, new { id, active = true });
        var driver = await StartDriverAsync();
        await driver.ProcessAsync(NullLogger.Instance, evt);
        await driver.FlushAsync(NullLogger.Instance);
        await driver.StopAsync();

        var active = await QueryScalarAsync<bool>(
            $"SELECT active FROM eds_test_orders WHERE id = '{id}'");
        Assert.True(active);
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
            $"SELECT name FROM eds_test_orders WHERE id = '{id}'");
        Assert.Equal("Updated", stored);
    }

    [Fact]
    public async Task Update_WithDiff_OnlyNamedColumnChanges()
    {
        var id     = Guid.NewGuid().ToString();
        var driver = await StartDriverAsync();

        await driver.ProcessAsync(NullLogger.Instance,
            DriverTestHelpers.MakeInsert(Schema.Table, id, new { id, name = "Alice", amount = 100.0 }));
        await driver.FlushAsync(NullLogger.Instance);

        // Diff contains only "name" — amount in payload should be ignored
        await driver.ProcessAsync(NullLogger.Instance,
            DriverTestHelpers.MakeUpdate(Schema.Table, id,
                new { id, name = "Bob", amount = 999.0 },
                diff: ["name"]));
        await driver.FlushAsync(NullLogger.Instance);
        await driver.StopAsync();

        var name   = await QueryScalarAsync<string>($"SELECT name   FROM eds_test_orders WHERE id = '{id}'");
        var amount = await QueryScalarAsync<double>($"SELECT amount FROM eds_test_orders WHERE id = '{id}'");

        Assert.Equal("Bob",  name);
        Assert.Equal(100.0, amount, precision: 2);   // unchanged
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

        var name  = await QueryScalarAsync<string>($"SELECT name     FROM eds_test_orders WHERE id = '{id}'");
        var count = await QueryScalarAsync<long>  ($"SELECT COUNT(*) FROM eds_test_orders WHERE id = '{id}'");

        Assert.Equal("Second", name);
        Assert.Equal(1L, count);
    }

    [Fact]
    public async Task MultipleBatches_AllRowsCommitted()
    {
        var ids    = Enumerable.Range(0, 5).Select(_ => Guid.NewGuid().ToString()).ToList();
        var driver = await StartDriverAsync();

        foreach (var id in ids)
            await driver.ProcessAsync(NullLogger.Instance,
                DriverTestHelpers.MakeInsert(Schema.Table, id, new { id, name = $"row-{id}" }));

        await driver.FlushAsync(NullLogger.Instance);
        await driver.StopAsync();

        foreach (var id in ids)
            Assert.True(await RowExistsAsync(id), $"Row {id} not found");
    }

    [Fact]
    public async Task Insert_NewlineInValue_StoredCorrectly()
    {
        var id     = Guid.NewGuid().ToString();
        var name   = "line1\nline2\ttabbed";
        var evt    = DriverTestHelpers.MakeInsert(Schema.Table, id, new { id, name });
        var driver = await StartDriverAsync();
        await driver.ProcessAsync(NullLogger.Instance, evt);
        await driver.FlushAsync(NullLogger.Instance);
        await driver.StopAsync();

        var stored = await QueryScalarAsync<string>(
            $"SELECT name FROM eds_test_orders WHERE id = '{id}'");
        Assert.Equal(name, stored);
    }
}

// ── TestContainers fixture ────────────────────────────────────────────────────

public sealed class PostgreSqlFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:16-alpine")
        .Build();

    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        ConnectionString = _container.GetConnectionString();
    }

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}
