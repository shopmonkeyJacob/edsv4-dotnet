using EDS.Core.Abstractions;
using EDS.Core.Models;
using EDS.Drivers.SqlServer;
using EDS.Integration.Tests.Helpers;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace EDS.Integration.Tests.Drivers;

/// <summary>
/// Integration tests that spin up a real SQL Server container via TestContainers,
/// push DbChangeEvents through the full ProcessAsync → FlushAsync pipeline, and
/// verify the persisted values are exactly correct.
/// </summary>
[Trait("Category", "Integration")]
public sealed class SqlServerIntegrationTests : IClassFixture<SqlServerFixture>
{
    private readonly string _connectionString;
    private readonly string _driverUrl;
    private static readonly Schema    Schema    = DriverTestHelpers.OrdersSchema("eds_ss_orders");
    private static readonly SchemaMap SchemaMap = DriverTestHelpers.ToSchemaMap(Schema);

    public SqlServerIntegrationTests(SqlServerFixture fixture)
    {
        _connectionString = fixture.ConnectionString;
        _driverUrl        = ToDriverUrl(_connectionString);
    }

    // ── Driver lifecycle helpers ──────────────────────────────────────────────

    private async Task<SqlServerDriver> StartDriverAsync()
    {
        var driver = new SqlServerDriver();
        await driver.StartAsync(new DriverConfig
        {
            Url            = _driverUrl,
            Logger         = NullLogger.Instance,
            SchemaRegistry = new FakeSchemaRegistry(SchemaMap),
            Tracker        = null!,
            DataDir        = string.Empty,
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

    private async Task<bool> RowExistsAsync(string id) =>
        await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM eds_ss_orders WHERE id = '{id.Replace("'", "''")}'"
        ) > 0;

    /// <summary>
    /// Converts a TestContainers-style ADO.NET connection string to the sqlserver:// URL
    /// format expected by <see cref="SqlServerDriver"/>.
    /// <para>
    /// TestContainers format: <c>Server=localhost,1433;User Id=sa;Password=...;Database=master;TrustServerCertificate=True</c>
    /// </para>
    /// </summary>
    private static string ToDriverUrl(string connStr, string database = "master")
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

        return $"sqlserver://{host}:{port}?database={database}" +
               $"&user id={user}&password={pwd}&trust-server-certificate=true";
    }

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
        var id   = Guid.NewGuid().ToString();
        var name = "O'Brien's Shop";
        var evt  = DriverTestHelpers.MakeInsert(Schema.Table, id, new { id, name });
        var driver = await StartDriverAsync();
        await driver.ProcessAsync(NullLogger.Instance, evt);
        await driver.FlushAsync(NullLogger.Instance);
        await driver.StopAsync();

        var stored = await QueryScalarAsync<string>(
            $"SELECT name FROM eds_ss_orders WHERE id = '{id}'");
        Assert.Equal(name, stored);
    }

    [Fact]
    public async Task Insert_SqlInjectionInValue_StoredLiterally()
    {
        var id        = Guid.NewGuid().ToString();
        var injection = "'; DROP TABLE eds_ss_orders; --";
        var evt       = DriverTestHelpers.MakeInsert(Schema.Table, id, new { id, name = injection });
        var driver    = await StartDriverAsync();
        await driver.ProcessAsync(NullLogger.Instance, evt);
        await driver.FlushAsync(NullLogger.Instance);   // must not throw
        await driver.StopAsync();

        // Table must still exist and row must contain the injection string literally.
        var stored = await QueryScalarAsync<string>(
            $"SELECT name FROM eds_ss_orders WHERE id = '{id}'");
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

        // SQL Server stores unicode in NVARCHAR(MAX) — value must round-trip exactly.
        var stored = await QueryScalarAsync<string>(
            $"SELECT name FROM eds_ss_orders WHERE id = '{id}'");
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
            $"SELECT name FROM eds_ss_orders WHERE id = '{id}'");
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
            $"SELECT amount FROM eds_ss_orders WHERE id = '{id}'");
        var qty = await QueryScalarAsync<long>(
            $"SELECT qty FROM eds_ss_orders WHERE id = '{id}'");

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

        // SQL Server BIT columns are returned as bool via ADO.NET.
        var active = await QueryScalarAsync<bool>(
            $"SELECT active FROM eds_ss_orders WHERE id = '{id}'");
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
            $"SELECT name FROM eds_ss_orders WHERE id = '{id}'");
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

        // Diff contains only "name" — amount in payload should be ignored.
        await driver.ProcessAsync(NullLogger.Instance,
            DriverTestHelpers.MakeUpdate(Schema.Table, id,
                new { id, name = "Bob", amount = 999.0 },
                diff: ["name"]));
        await driver.FlushAsync(NullLogger.Instance);
        await driver.StopAsync();

        var name   = await QueryScalarAsync<string>($"SELECT name   FROM eds_ss_orders WHERE id = '{id}'");
        var amount = await QueryScalarAsync<double>($"SELECT amount FROM eds_ss_orders WHERE id = '{id}'");

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

        var name  = await QueryScalarAsync<string>($"SELECT name     FROM eds_ss_orders WHERE id = '{id}'");
        var count = await QueryScalarAsync<long>  ($"SELECT COUNT(*) FROM eds_ss_orders WHERE id = '{id}'");

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
}

// ── TestContainers fixture ────────────────────────────────────────────────────

public sealed class SqlServerFixture : IAsyncLifetime
{
    // Use a raw ContainerBuilder instead of MsSqlBuilder so we have full control over
    // the wait strategy. MsSqlBuilder always appends its own sqlcmd health-check
    // (UntilUnixCommandIsCompleted) on top of any custom wait strategy, which fails on
    // azure-sql-edge because sqlcmd lives at a different path than SQL Server 2022.
    private const string SaPassword = "yourStrong(!)Password";

    private readonly IContainer _container = new ContainerBuilder()
        .WithImage("mcr.microsoft.com/azure-sql-edge:latest")
        .WithPortBinding(1433, true)
        .WithEnvironment("ACCEPT_EULA", "Y")
        .WithEnvironment("SA_PASSWORD", SaPassword)
        .WithEnvironment("MSSQL_SA_PASSWORD", SaPassword)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(1433))
        .Build();

    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        // Allow the engine a moment to finish initialising after the port opens.
        await Task.Delay(TimeSpan.FromSeconds(5));
        var port = _container.GetMappedPublicPort(1433);
        ConnectionString = $"Server={_container.Hostname},{port};Database=master;" +
                           $"User Id=sa;Password={SaPassword};TrustServerCertificate=True;";
    }

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}
