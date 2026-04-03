using EDS.Core.Helpers;
using EDS.Core.Models;

namespace EDS.Core.Tests;

public class SqlHelpersTests
{
    // ── IsValidNumericLiteral ─────────────────────────────────────────────────

    [Theory]
    [InlineData("0")]
    [InlineData("42")]
    [InlineData("-7")]
    [InlineData("3.14")]
    [InlineData("-0.001")]
    [InlineData(".5")]
    [InlineData("1e10")]
    [InlineData("2.5E-3")]
    [InlineData("1.23e+45")]
    public void IsValidNumericLiteral_ValidInputs_ReturnsTrue(string input) =>
        Assert.True(SqlHelpers.IsValidNumericLiteral(input));

    [Theory]
    [InlineData("")]
    [InlineData("abc")]
    [InlineData("1 2")]
    [InlineData("1.2.3")]
    [InlineData("0x1F")]
    [InlineData("'; DROP TABLE orders; --")]
    [InlineData(" 42")]
    [InlineData("42 ")]
    public void IsValidNumericLiteral_InvalidInputs_ReturnsFalse(string input) =>
        Assert.False(SqlHelpers.IsValidNumericLiteral(input));

    // ── BuildUpsertSql ────────────────────────────────────────────────────────

    private static Schema SimpleSchema(string table, List<string> primaryKeys, params string[] extraCols)
    {
        var props = new Dictionary<string, SchemaProperty>();
        foreach (var pk in primaryKeys)
            props[pk] = new SchemaProperty { Type = "string" };
        foreach (var col in extraCols)
            props[col] = new SchemaProperty { Type = "string" };
        return new Schema { Table = table, Properties = props, PrimaryKeys = primaryKeys };
    }

    [Fact]
    public void BuildUpsertSql_DefaultParamName_ProducesInsert()
    {
        var schema = SimpleSchema("orders", ["id"], "name", "amount");
        var evt = new DbChangeEvent { Operation = "insert", Id = "1", Table = "orders", Key = ["1"] };

        var sql = SqlHelpers.BuildUpsertSql(schema, evt, "\"");

        Assert.StartsWith("INSERT INTO \"orders\"", sql);
        Assert.Contains("\"id\"", sql);
        Assert.Contains("\"amount\"", sql);
        Assert.Contains("\"name\"", sql);
        Assert.Contains("@id", sql);
        Assert.Contains("@amount", sql);
        Assert.Contains("@name", sql);
    }

    [Fact]
    public void BuildUpsertSql_PrimaryKeysFirst_InColumnList()
    {
        var schema = SimpleSchema("orders", ["id"], "zzz", "aaa");
        var evt = new DbChangeEvent { Operation = "insert", Id = "1", Table = "orders", Key = ["1"] };

        var sql = SqlHelpers.BuildUpsertSql(schema, evt, "\"");

        // id should appear before aaa and zzz
        var idIdx = sql.IndexOf("\"id\"", StringComparison.Ordinal);
        var aaaIdx = sql.IndexOf("\"aaa\"", StringComparison.Ordinal);
        var zzzIdx = sql.IndexOf("\"zzz\"", StringComparison.Ordinal);
        Assert.True(idIdx < aaaIdx);
        Assert.True(aaaIdx < zzzIdx);
    }

    [Fact]
    public void BuildUpsertSql_CustomParamName_UsesCustomFormat()
    {
        var schema = SimpleSchema("t", ["id"]);
        var evt = new DbChangeEvent { Operation = "insert", Id = "1", Table = "t", Key = ["1"] };

        var sql = SqlHelpers.BuildUpsertSql(schema, evt, "`", col => $":{col}");

        Assert.Contains(":id", sql);
        Assert.DoesNotContain("@id", sql);
    }

    [Fact]
    public void BuildUpsertSql_AlternativeQuoteChar_UsesBacktick()
    {
        var schema = SimpleSchema("orders", ["id"]);
        var evt = new DbChangeEvent { Operation = "insert", Id = "1", Table = "orders", Key = ["1"] };

        var sql = SqlHelpers.BuildUpsertSql(schema, evt, "`");

        Assert.Contains("`orders`", sql);
        Assert.Contains("`id`", sql);
    }

    // ── BuildDeleteSql ────────────────────────────────────────────────────────

    [Fact]
    public void BuildDeleteSql_SinglePrimaryKey_ProducesWhereClause()
    {
        var schema = SimpleSchema("orders", ["id"]);

        var sql = SqlHelpers.BuildDeleteSql(schema, "\"");

        Assert.Equal("DELETE FROM \"orders\" WHERE \"id\" = @id", sql);
    }

    [Fact]
    public void BuildDeleteSql_MultipleKeys_JoinsWithAnd()
    {
        var schema = SimpleSchema("order_items", ["orderId", "itemId"]);

        var sql = SqlHelpers.BuildDeleteSql(schema, "\"");

        Assert.Contains("\"orderId\" = @orderId", sql);
        Assert.Contains("\"itemId\" = @itemId", sql);
        Assert.Contains(" AND ", sql);
    }

    [Fact]
    public void BuildDeleteSql_CustomParamName_UsesCustomFormat()
    {
        var schema = SimpleSchema("t", ["id"]);

        var sql = SqlHelpers.BuildDeleteSql(schema, "\"", col => $"${col}");

        Assert.Contains("$id", sql);
    }

    // ── BuildCreateTableSql ───────────────────────────────────────────────────

    [Fact]
    public void BuildCreateTableSql_BasicTable_ContainsCreateStatement()
    {
        var props = new Dictionary<string, SchemaProperty>
        {
            ["id"]   = new() { Type = "string", Nullable = false },
            ["name"] = new() { Type = "string", Nullable = true },
        };
        var schema = new Schema { Table = "customers", Properties = props, PrimaryKeys = ["id"] };

        var sql = SqlHelpers.BuildCreateTableSql(schema, "\"", _ => "TEXT");

        Assert.Contains("CREATE TABLE IF NOT EXISTS \"customers\"", sql);
        Assert.Contains("\"id\" TEXT NOT NULL", sql);
        Assert.Contains("\"name\" TEXT NULL", sql);
        Assert.Contains("PRIMARY KEY (\"id\")", sql);
    }

    [Fact]
    public void BuildCreateTableSql_ArrayColumn_IsNotNull()
    {
        var props = new Dictionary<string, SchemaProperty>
        {
            ["id"]   = new() { Type = "string", Nullable = false },
            ["tags"] = new() { Type = "array",  Nullable = true },   // array → IsNotNull even if Nullable=true
        };
        var schema = new Schema { Table = "t", Properties = props, PrimaryKeys = ["id"] };

        var sql = SqlHelpers.BuildCreateTableSql(schema, "\"", _ => "TEXT");

        Assert.Contains("\"tags\" TEXT NOT NULL", sql);
    }

    [Fact]
    public void BuildCreateTableSql_NoPrimaryKeys_OmitsPrimaryKeyClause()
    {
        var props = new Dictionary<string, SchemaProperty>
        {
            ["col1"] = new() { Type = "string", Nullable = true },
        };
        var schema = new Schema { Table = "t", Properties = props, PrimaryKeys = [] };

        var sql = SqlHelpers.BuildCreateTableSql(schema, "\"", _ => "TEXT");

        Assert.DoesNotContain("PRIMARY KEY", sql);
    }

    [Fact]
    public void BuildCreateTableSql_MultipleKeys_AllInPrimaryKeyClause()
    {
        var props = new Dictionary<string, SchemaProperty>
        {
            ["a"] = new() { Type = "string" },
            ["b"] = new() { Type = "string" },
        };
        var schema = new Schema { Table = "t", Properties = props, PrimaryKeys = ["a", "b"] };

        var sql = SqlHelpers.BuildCreateTableSql(schema, "\"", _ => "INT");

        Assert.Contains("\"a\", \"b\"", sql);
    }

    // ── BuildAddColumnSql ─────────────────────────────────────────────────────

    [Fact]
    public void BuildAddColumnSql_ProducesAlterTableStatement()
    {
        var sql = SqlHelpers.BuildAddColumnSql("orders", "status", "TEXT", "\"");
        Assert.Equal("ALTER TABLE \"orders\" ADD COLUMN \"status\" TEXT", sql);
    }

    [Fact]
    public void BuildAddColumnSql_BacktickQuote_UsesBacktick()
    {
        var sql = SqlHelpers.BuildAddColumnSql("orders", "status", "VARCHAR(50)", "`");
        Assert.Equal("ALTER TABLE `orders` ADD COLUMN `status` VARCHAR(50)", sql);
    }
}
