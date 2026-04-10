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

// ── PostgreSQL migration tests ────────────────────────────────────────────────

/// <summary>
/// Integration tests for schema-migration methods (MigrateChangedColumnsAsync,
/// MigrateRemovedColumnsAsync, DropOrphanTablesAsync) using a real PostgreSQL container.
/// </summary>
[Trait("Category", "Integration")]
public sealed class PostgreSqlMigrationTests : IClassFixture<PostgreSqlFixture>
{
    private readonly string _connectionString;
    private static readonly Schema Schema     = DriverTestHelpers.OrdersSchema();
    private static readonly SchemaMap SchemaMap = DriverTestHelpers.ToSchemaMap(Schema);

    public PostgreSqlMigrationTests(PostgreSqlFixture fixture) =>
        _connectionString = fixture.ConnectionString;

    // ── Helpers ───────────────────────────────────────────────────────────────

    private async Task<PostgreSqlDriver> StartDriverAsync()
    {
        // Drop the table first so each test starts with a clean, fully-columned schema.
        await ExecuteNonQueryAsync($"DROP TABLE IF EXISTS \"{Schema.Table}\"");

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

    private async Task ExecuteNonQueryAsync(string sql)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<bool> ColumnExistsAsync(string table, string column)
    {
        var count = await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM information_schema.columns " +
            $"WHERE table_name = '{table}' AND column_name = '{column}'");
        return count > 0;
    }

    private async Task<bool> TableExistsAsync(string table)
    {
        var count = await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM information_schema.tables " +
            $"WHERE table_name = '{table}'");
        return count > 0;
    }

    // ── MigrateChangedColumnsAsync ────────────────────────────────────────────

    [Fact]
    public async Task MigrateChangedColumns_DoesNotThrow_AndColumnStillExists()
    {
        var driver = await StartDriverAsync();

        // Build a schema where "qty" has changed type from integer → number.
        var modifiedProps = new Dictionary<string, SchemaProperty>(Schema.Properties)
        {
            ["qty"] = new() { Type = "number", Nullable = true }
        };
        var newSchema = DriverTestHelpers.MakeSchema(Schema.Table, ["id"], modifiedProps);

        // Should not throw; PostgreSQL supports ALTER COLUMN TYPE.
        await driver.MigrateChangedColumnsAsync(NullLogger.Instance, newSchema, ["qty"],
            CancellationToken.None);

        // Column must still exist after the migration.
        Assert.True(await ColumnExistsAsync(Schema.Table, "qty"));
    }

    // ── MigrateRemovedColumnsAsync ────────────────────────────────────────────

    [Fact]
    public async Task MigrateRemovedColumns_DropsColumn()
    {
        var driver = await StartDriverAsync();

        // Verify "name" exists before removal.
        Assert.True(await ColumnExistsAsync(Schema.Table, "name"));

        // Build schema without "name".
        var reducedProps = Schema.Properties
            .Where(kv => kv.Key != "name")
            .ToDictionary(kv => kv.Key, kv => kv.Value);
        var reducedSchema = DriverTestHelpers.MakeSchema(Schema.Table, ["id"], reducedProps);

        await driver.MigrateRemovedColumnsAsync(NullLogger.Instance, reducedSchema, ["name"],
            CancellationToken.None);

        Assert.False(await ColumnExistsAsync(Schema.Table, "name"));
    }

    [Fact]
    public async Task MigrateRemovedColumns_InsertWithoutDroppedColumn_Succeeds()
    {
        var driver = await StartDriverAsync();

        // Build schema without "name" and drop it.
        var reducedProps = Schema.Properties
            .Where(kv => kv.Key != "name")
            .ToDictionary(kv => kv.Key, kv => kv.Value);
        var reducedSchema = DriverTestHelpers.MakeSchema(Schema.Table, ["id"], reducedProps);

        await driver.MigrateRemovedColumnsAsync(NullLogger.Instance, reducedSchema, ["name"],
            CancellationToken.None);

        // Insert a row without "name" — must not throw.
        var id  = Guid.NewGuid().ToString();
        var evt = DriverTestHelpers.MakeInsert(Schema.Table, id, new { id, amount = 42.0 });
        var reducedSchemaMap = DriverTestHelpers.ToSchemaMap(reducedSchema);
        var driver2 = new PostgreSqlDriver();
        await driver2.StartAsync(new DriverConfig
        {
            Url            = _connectionString,
            Logger         = NullLogger.Instance,
            SchemaRegistry = new FakeSchemaRegistry(reducedSchemaMap),
            Tracker        = null!,
            DataDir        = string.Empty,
        });
        await driver2.ProcessAsync(NullLogger.Instance, evt);
        await driver2.FlushAsync(NullLogger.Instance);   // must not throw
        await driver2.StopAsync();

        var count = await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM eds_test_orders WHERE id = '{id}'");
        Assert.Equal(1L, count);
    }

    // ── DropOrphanTablesAsync ─────────────────────────────────────────────────

    [Fact]
    public async Task DropOrphanTables_DropsTablesNotInKnownSet()
    {
        var driver = await StartDriverAsync();

        // Create an extra table directly so it becomes visible to the next schema refresh.
        await ExecuteNonQueryAsync(
            "CREATE TABLE IF NOT EXISTS eds_orphan_test (id TEXT PRIMARY KEY)");

        // Re-reflect DbSchema so the driver knows about the orphan table.
        var registry = new FakeSchemaRegistry(SchemaMap);
        await driver.InitForImportAsync(
            NullLogger.Instance, registry, _connectionString,
            ct: CancellationToken.None);

        Assert.True(await TableExistsAsync("eds_orphan_test"));

        // Drop everything except the known table.
        await driver.DropOrphanTablesAsync(
            NullLogger.Instance,
            new HashSet<string> { Schema.Table },
            CancellationToken.None);

        Assert.False(await TableExistsAsync("eds_orphan_test"));
        Assert.True(await TableExistsAsync(Schema.Table));
    }

    [Fact]
    public async Task DropOrphanTables_WhenAllTablesKnown_DoesNotDropAnything()
    {
        var driver = await StartDriverAsync();

        Assert.True(await TableExistsAsync(Schema.Table));

        await driver.DropOrphanTablesAsync(
            NullLogger.Instance,
            new HashSet<string> { Schema.Table },
            CancellationToken.None);

        Assert.True(await TableExistsAsync(Schema.Table));
    }
}

// ── MySQL migration tests ─────────────────────────────────────────────────────


/// <summary>
/// Integration tests for schema-migration methods (MigrateChangedColumnsAsync,
/// MigrateRemovedColumnsAsync, DropOrphanTablesAsync) using a real MySQL container.
/// </summary>
[Trait("Category", "Integration")]
public sealed class MySqlMigrationTests : IClassFixture<MySqlFixture>
{
    private readonly string _mysqlUrl;
    private static readonly Schema Schema     = DriverTestHelpers.OrdersSchema("eds_mysql_migration");
    private static readonly SchemaMap SchemaMap = DriverTestHelpers.ToSchemaMap(Schema);

    public MySqlMigrationTests(MySqlFixture fixture) => _mysqlUrl = fixture.MySqlUrl;

    // ── Helpers ───────────────────────────────────────────────────────────────

    private async Task<MySqlDriver> StartDriverAsync()
    {
        // Drop the table first so each test gets a clean, fully-columned schema.
        // Without this, a previous test that removed a column would leave the
        // shared database in a modified state and break precondition assertions.
        await ExecuteNonQueryAsync($"DROP TABLE IF EXISTS `{Schema.Table}`");

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

    private async Task<T?> QueryScalarAsync<T>(string sql)
    {
        await using var conn = new MySqlConnection(MysqlConnectionString());
        await conn.OpenAsync();
        await using var cmd = new MySqlCommand(sql, conn);
        var result = await cmd.ExecuteScalarAsync();
        if (result is DBNull || result is null) return default;
        return (T?)Convert.ChangeType(result, typeof(T));
    }

    private async Task ExecuteNonQueryAsync(string sql)
    {
        await using var conn = new MySqlConnection(MysqlConnectionString());
        await conn.OpenAsync();
        await using var cmd = new MySqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<bool> ColumnExistsAsync(string table, string column)
    {
        var count = await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM information_schema.columns " +
            $"WHERE table_name = '{table}' AND column_name = '{column}' AND table_schema = DATABASE()");
        return count > 0;
    }

    private async Task<bool> TableExistsAsync(string table)
    {
        var count = await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM information_schema.tables " +
            $"WHERE table_name = '{table}' AND table_schema = DATABASE()");
        return count > 0;
    }

    // ── MigrateChangedColumnsAsync ────────────────────────────────────────────

    [Fact]
    public async Task MigrateChangedColumns_DoesNotThrow_AndColumnStillExists()
    {
        var driver = await StartDriverAsync();

        // Build a schema where "qty" has changed type from integer → number.
        var modifiedProps = new Dictionary<string, SchemaProperty>(Schema.Properties)
        {
            ["qty"] = new() { Type = "number", Nullable = true }
        };
        var newSchema = DriverTestHelpers.MakeSchema(Schema.Table, ["id"], modifiedProps);

        // MySQL supports MODIFY COLUMN for type changes — should not throw.
        await driver.MigrateChangedColumnsAsync(NullLogger.Instance, newSchema, ["qty"],
            CancellationToken.None);

        // Column must still exist after migration.
        Assert.True(await ColumnExistsAsync(Schema.Table, "qty"));
    }

    [Fact]
    public async Task MigrateChangedColumns_UpdatesColumnDataType()
    {
        var driver = await StartDriverAsync();

        // Change "qty" from integer (BIGINT) → number (DOUBLE).
        var modifiedProps = new Dictionary<string, SchemaProperty>(Schema.Properties)
        {
            ["qty"] = new() { Type = "number", Nullable = true }
        };
        var newSchema = DriverTestHelpers.MakeSchema(Schema.Table, ["id"], modifiedProps);

        await driver.MigrateChangedColumnsAsync(NullLogger.Instance, newSchema, ["qty"],
            CancellationToken.None);

        // Verify the DATA_TYPE is now a floating-point type (DOUBLE or FLOAT).
        var dataType = await QueryScalarAsync<string>(
            $"SELECT DATA_TYPE FROM information_schema.columns " +
            $"WHERE table_name = '{Schema.Table}' AND column_name = 'qty' " +
            $"AND table_schema = DATABASE()");

        Assert.NotNull(dataType);
        Assert.Contains(dataType, new[] { "double", "float" }, StringComparer.OrdinalIgnoreCase);
    }

    // ── MigrateRemovedColumnsAsync ────────────────────────────────────────────

    [Fact]
    public async Task MigrateRemovedColumns_DropsColumn()
    {
        var driver = await StartDriverAsync();

        Assert.True(await ColumnExistsAsync(Schema.Table, "name"));

        var reducedProps = Schema.Properties
            .Where(kv => kv.Key != "name")
            .ToDictionary(kv => kv.Key, kv => kv.Value);
        var reducedSchema = DriverTestHelpers.MakeSchema(Schema.Table, ["id"], reducedProps);

        await driver.MigrateRemovedColumnsAsync(NullLogger.Instance, reducedSchema, ["name"],
            CancellationToken.None);

        Assert.False(await ColumnExistsAsync(Schema.Table, "name"));
    }

    [Fact]
    public async Task MigrateRemovedColumns_InsertWithoutDroppedColumn_Succeeds()
    {
        var driver = await StartDriverAsync();

        var reducedProps = Schema.Properties
            .Where(kv => kv.Key != "name")
            .ToDictionary(kv => kv.Key, kv => kv.Value);
        var reducedSchema = DriverTestHelpers.MakeSchema(Schema.Table, ["id"], reducedProps);

        await driver.MigrateRemovedColumnsAsync(NullLogger.Instance, reducedSchema, ["name"],
            CancellationToken.None);

        // Insert a row without "name" using a fresh driver pointed at the reduced schema.
        var id  = Guid.NewGuid().ToString();
        var evt = DriverTestHelpers.MakeInsert(Schema.Table, id, new { id, amount = 42.0 });
        var reducedSchemaMap = DriverTestHelpers.ToSchemaMap(reducedSchema);
        var driver2 = new MySqlDriver();
        await driver2.StartAsync(new DriverConfig
        {
            Url            = _mysqlUrl,
            Logger         = NullLogger.Instance,
            SchemaRegistry = new FakeSchemaRegistry(reducedSchemaMap),
            Tracker        = null!,
            DataDir        = string.Empty,
        });
        await driver2.ProcessAsync(NullLogger.Instance, evt);
        await driver2.FlushAsync(NullLogger.Instance);
        await driver2.StopAsync();

        var count = await QueryScalarAsync<long>(
            $"SELECT COUNT(*) FROM {Schema.Table} WHERE id = '{id}'");
        Assert.Equal(1L, count);
    }

    // ── DropOrphanTablesAsync ─────────────────────────────────────────────────

    [Fact]
    public async Task DropOrphanTables_DropsTablesNotInKnownSet()
    {
        var driver = await StartDriverAsync();

        // Create an extra table directly.
        await ExecuteNonQueryAsync(
            "CREATE TABLE IF NOT EXISTS eds_mysql_orphan_test (id VARCHAR(64) PRIMARY KEY)");

        // Re-reflect DbSchema so the driver knows about the orphan table.
        var registry = new FakeSchemaRegistry(SchemaMap);
        await driver.InitForImportAsync(
            NullLogger.Instance, registry, _mysqlUrl,
            ct: CancellationToken.None);

        Assert.True(await TableExistsAsync("eds_mysql_orphan_test"));

        await driver.DropOrphanTablesAsync(
            NullLogger.Instance,
            new HashSet<string> { Schema.Table },
            CancellationToken.None);

        Assert.False(await TableExistsAsync("eds_mysql_orphan_test"));
        Assert.True(await TableExistsAsync(Schema.Table));
    }

    [Fact]
    public async Task DropOrphanTables_WhenAllTablesKnown_DoesNotDropAnything()
    {
        var driver = await StartDriverAsync();

        Assert.True(await TableExistsAsync(Schema.Table));

        await driver.DropOrphanTablesAsync(
            NullLogger.Instance,
            new HashSet<string> { Schema.Table },
            CancellationToken.None);

        Assert.True(await TableExistsAsync(Schema.Table));
    }
}

// ── SQL Server migration tests ────────────────────────────────────────────────

/// <summary>
/// Integration tests for schema-migration methods (MigrateChangedColumnsAsync,
/// MigrateRemovedColumnsAsync, DropOrphanTablesAsync) using a real SQL Server container.
/// </summary>
[Trait("Category", "Integration")]
public sealed class SqlServerMigrationTests : IClassFixture<SqlServerFixture>
{
    private readonly string _connectionString;
    private readonly string _driverUrl;
    private static readonly Schema Schema      = DriverTestHelpers.OrdersSchema("eds_ss_migration");
    private static readonly SchemaMap SchemaMap = DriverTestHelpers.ToSchemaMap(Schema);

    public SqlServerMigrationTests(SqlServerFixture fixture)
    {
        _connectionString = fixture.ConnectionString;
        _driverUrl        = ToDriverUrl(_connectionString);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private async Task<SqlServerDriver> StartDriverAsync()
    {
        // Drop the table first so each test starts with a clean, fully-columned schema.
        await ExecuteNonQueryAsync(
            $"IF OBJECT_ID(N'[{Schema.Table}]', N'U') IS NOT NULL DROP TABLE [{Schema.Table}]");

        var driver = new SqlServerDriver();
        await driver.StartAsync(new DriverConfig
        {
            Url            = _driverUrl,
            Logger         = NullLogger.Instance,
            SchemaRegistry = new FakeSchemaRegistry(SchemaMap),
            Tracker        = null!,
            DataDir        = string.Empty,
        });
        // StartAsync → EnsureTablesAsync creates the table from SchemaMap (CREATE TABLE IF NOT EXISTS).
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

    private async Task ExecuteNonQueryAsync(string sql)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<bool> ColumnExistsAsync(string column)
    {
        var count = await QueryScalarAsync<int>(
            $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS " +
            $"WHERE TABLE_NAME = '{Schema.Table}' AND COLUMN_NAME = '{column}'");
        return count > 0;
    }

    private async Task<bool> TableExistsAsync(string table)
    {
        var count = await QueryScalarAsync<int>(
            $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES " +
            $"WHERE TABLE_NAME = '{table}'");
        return count > 0;
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

    // ── MigrateChangedColumnsAsync ────────────────────────────────────────────

    [Fact]
    public async Task MigrateChangedColumns_DoesNotThrow_AndColumnStillExists()
    {
        var driver = await StartDriverAsync();

        var modifiedProps = new Dictionary<string, SchemaProperty>(Schema.Properties)
        {
            ["qty"] = new() { Type = "number", Nullable = true }
        };
        var newSchema = DriverTestHelpers.MakeSchema(Schema.Table, ["id"], modifiedProps);

        await driver.MigrateChangedColumnsAsync(NullLogger.Instance, newSchema, ["qty"],
            CancellationToken.None);

        Assert.True(await ColumnExistsAsync("qty"));
    }

    // ── MigrateRemovedColumnsAsync ────────────────────────────────────────────

    [Fact]
    public async Task MigrateRemovedColumns_DropsColumn()
    {
        var driver = await StartDriverAsync();

        Assert.True(await ColumnExistsAsync("name"));

        var reducedProps = Schema.Properties
            .Where(kv => kv.Key != "name")
            .ToDictionary(kv => kv.Key, kv => kv.Value);
        var reducedSchema = DriverTestHelpers.MakeSchema(Schema.Table, ["id"], reducedProps);

        await driver.MigrateRemovedColumnsAsync(NullLogger.Instance, reducedSchema, ["name"],
            CancellationToken.None);

        Assert.False(await ColumnExistsAsync("name"));
    }

    [Fact]
    public async Task MigrateRemovedColumns_InsertWithoutDroppedColumn_Succeeds()
    {
        var driver = await StartDriverAsync();

        var reducedProps = Schema.Properties
            .Where(kv => kv.Key != "name")
            .ToDictionary(kv => kv.Key, kv => kv.Value);
        var reducedSchema = DriverTestHelpers.MakeSchema(Schema.Table, ["id"], reducedProps);

        await driver.MigrateRemovedColumnsAsync(NullLogger.Instance, reducedSchema, ["name"],
            CancellationToken.None);

        var id  = Guid.NewGuid().ToString();
        var evt = DriverTestHelpers.MakeInsert(Schema.Table, id, new { id, amount = 42.0 });
        var reducedSchemaMap = DriverTestHelpers.ToSchemaMap(reducedSchema);

        var driver2 = new SqlServerDriver();
        await driver2.StartAsync(new DriverConfig
        {
            Url            = _driverUrl,
            Logger         = NullLogger.Instance,
            SchemaRegistry = new FakeSchemaRegistry(reducedSchemaMap),
            Tracker        = null!,
            DataDir        = string.Empty,
        });
        await driver2.ProcessAsync(NullLogger.Instance, evt);
        await driver2.FlushAsync(NullLogger.Instance);
        await driver2.StopAsync();

        var count = await QueryScalarAsync<int>(
            $"SELECT COUNT(*) FROM [{Schema.Table}] WHERE id = '{id}'");
        Assert.Equal(1, count);
    }

    // ── DropOrphanTablesAsync ─────────────────────────────────────────────────

    [Fact]
    public async Task DropOrphanTables_DropsTablesNotInKnownSet()
    {
        var driver = await StartDriverAsync();

        await ExecuteNonQueryAsync(
            "IF OBJECT_ID(N'[eds_ss_orphan_test]', N'U') IS NULL " +
            "CREATE TABLE [eds_ss_orphan_test] (id NVARCHAR(64) PRIMARY KEY)");

        // Re-reflect DbSchema so the driver knows about the orphan table.
        await driver.InitForImportAsync(
            NullLogger.Instance, new FakeSchemaRegistry(SchemaMap), _driverUrl,
            ct: CancellationToken.None);

        Assert.True(await TableExistsAsync("eds_ss_orphan_test"));

        await driver.DropOrphanTablesAsync(
            NullLogger.Instance,
            new HashSet<string> { Schema.Table },
            CancellationToken.None);

        Assert.False(await TableExistsAsync("eds_ss_orphan_test"));
        Assert.True(await TableExistsAsync(Schema.Table));
    }

    [Fact]
    public async Task DropOrphanTables_WhenAllTablesKnown_DoesNotDropAnything()
    {
        var driver = await StartDriverAsync();

        Assert.True(await TableExistsAsync(Schema.Table));

        await driver.DropOrphanTablesAsync(
            NullLogger.Instance,
            new HashSet<string> { Schema.Table },
            CancellationToken.None);

        Assert.True(await TableExistsAsync(Schema.Table));
    }
}
