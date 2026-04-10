using EDS.Core.Abstractions;
using EDS.Core.Drivers;
using EDS.Core.Models;
using Microsoft.Extensions.Logging;
using System.Data.Common;
using System.Reflection;

namespace EDS.Core.Tests;

// ── Minimal concrete driver for migration-SQL tests (PostgreSQL-style double-quote quoting) ──

internal sealed class MigrationTestableSqlDriver : SqlDriverBase
{
    protected override Task<DbConnection> OpenConnectionAsync(CancellationToken ct) =>
        throw new NotImplementedException();
    protected override string GetDatabaseNameSql()    => "SELECT 1";
    protected override string GetSchemaInfoSql()      => "";
    protected override void AddDbNameParameter(DbCommand cmd, string dbName) { }
    protected override string BuildSql(DbChangeEvent evt, Schema schema) => "";
    protected override string BuildCreateTableIfNotExistsSql(Schema schema) => "";
    protected override string BuildDropAndCreateTableSql(Schema schema)    => "";
    protected override string BuildAlterAddColumnSql(string table, string col, string sqlType) => "";
    protected override string PropToSqlType(SchemaProperty prop, bool isPrimaryKey = false) => "TEXT";
    protected override string QuoteId(string name) => $"\"{name}\"";
    public override Task TestAsync(ILogger logger, string url, CancellationToken ct = default) => Task.CompletedTask;
    public override IReadOnlyList<DriverField> Configuration() => [];
    public override (string url, IReadOnlyList<FieldError> errors) Validate(Dictionary<string, object?> values)
        => ("test://localhost", []);

    // Expose the virtual migration methods so tests can call them directly.
    public string ExposedBuildDropColumnSql(string table, string col)    => BuildDropColumnSql(table, col);
    public string ExposedBuildDropTableSql(string table)                  => BuildDropTableSql(table);
    public string ExposedBuildAlterColumnTypeSql(string table, string col, string sqlType)
        => BuildAlterColumnTypeSql(table, col, sqlType);
}

public class SqlDriverBaseMigrationSqlTests
{
    private static readonly MigrationTestableSqlDriver Driver = new();

    // ── BuildDropColumnSql — base/default implementation ─────────────────────

    [Fact]
    public void BuildDropColumnSql_Standard_AlterTableDropColumn()
    {
        var sql = Driver.ExposedBuildDropColumnSql("orders", "amount");
        Assert.Contains("ALTER TABLE", sql);
        Assert.Contains("DROP COLUMN", sql);
        Assert.Contains("\"orders\"", sql);
        Assert.Contains("\"amount\"", sql);
    }

    [Fact]
    public void BuildDropColumnSql_Standard_EndsWithSemicolon()
    {
        var sql = Driver.ExposedBuildDropColumnSql("orders", "amount");
        Assert.EndsWith(";", sql.Trim());
    }

    [Fact]
    public void BuildDropColumnSql_Standard_TableAndColumnAreQuoted()
    {
        var sql = Driver.ExposedBuildDropColumnSql("my_table", "my_col");
        // Both identifiers must appear in quoted form
        Assert.Contains("\"my_table\"", sql);
        Assert.Contains("\"my_col\"", sql);
    }

    // ── BuildDropTableSql — base/default implementation ──────────────────────

    [Fact]
    public void BuildDropTableSql_Standard_DropTableIfExists()
    {
        var sql = Driver.ExposedBuildDropTableSql("orders");
        Assert.Contains("DROP TABLE", sql);
        Assert.Contains("IF EXISTS", sql);
        Assert.Contains("\"orders\"", sql);
    }

    [Fact]
    public void BuildDropTableSql_Standard_EndsWithSemicolon()
    {
        var sql = Driver.ExposedBuildDropTableSql("orders");
        Assert.EndsWith(";", sql.Trim());
    }

    [Fact]
    public void BuildDropTableSql_Standard_TableNameIsQuoted()
    {
        var sql = Driver.ExposedBuildDropTableSql("line_items");
        Assert.Contains("\"line_items\"", sql);
    }

    // ── BuildAlterColumnTypeSql — base/default implementation ────────────────

    [Fact]
    public void BuildAlterColumnTypeSql_Base_ReturnsEmptyString()
    {
        var sql = Driver.ExposedBuildAlterColumnTypeSql("orders", "amount", "DECIMAL(10,2)");
        Assert.Equal(string.Empty, sql);
    }

    [Theory]
    [InlineData("orders", "amount",   "BIGINT")]
    [InlineData("users",  "email",    "VARCHAR(255)")]
    [InlineData("items",  "metadata", "JSONB")]
    public void BuildAlterColumnTypeSql_Base_AlwaysReturnsEmptyString(
        string table, string col, string sqlType)
    {
        var sql = Driver.ExposedBuildAlterColumnTypeSql(table, col, sqlType);
        Assert.Equal(string.Empty, sql);
    }

    // ── Identifier quoting survives special characters ────────────────────────

    [Theory]
    [InlineData("orders-2024")]
    [InlineData("my table")]
    [InlineData("table.with.dots")]
    public void BuildDropColumnSql_SpecialCharsInTableName_AreWrappedInQuotes(string tableName)
    {
        var sql = Driver.ExposedBuildDropColumnSql(tableName, "col");
        // The table name must be enclosed in double-quotes regardless of its content
        Assert.Contains($"\"{tableName}\"", sql);
    }

    [Theory]
    [InlineData("orders-2024")]
    [InlineData("my table")]
    [InlineData("table.with.dots")]
    public void BuildDropTableSql_SpecialCharsInTableName_AreWrappedInQuotes(string tableName)
    {
        var sql = Driver.ExposedBuildDropTableSql(tableName);
        Assert.Contains($"\"{tableName}\"", sql);
    }
}
