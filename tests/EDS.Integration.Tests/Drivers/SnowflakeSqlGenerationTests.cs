using EDS.Core.Models;
using EDS.Drivers.Snowflake;
using EDS.Integration.Tests.Helpers;

namespace EDS.Integration.Tests.Drivers;

/// <summary>
/// Pure unit tests for SnowflakeDriver.BuildEventSql.
/// No database required — these test the generated SQL string only.
/// </summary>
public class SnowflakeSqlGenerationTests
{
    private static readonly Schema Orders = DriverTestHelpers.OrdersSchema();

    // ── INSERT / MERGE tests ───────────────────────────────────────────────────

    [Fact]
    public void Snowflake_Insert_ProducesMergeStatement()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", name = "Test", amount = 9.99 });

        var sql = SnowflakeDriver.BuildEventSql(Orders, evt);

        Assert.Contains("MERGE INTO",              sql);
        Assert.Contains("WHEN MATCHED",            sql);
        Assert.Contains("WHEN NOT MATCHED",        sql);
        Assert.Contains("\"eds_test_orders\"",     sql);
    }

    [Fact]
    public void Snowflake_Insert_QuotesIdentifiersWithDoubleQuotes()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", name = "Test" });

        var sql = SnowflakeDriver.BuildEventSql(Orders, evt);

        Assert.Contains("\"eds_test_orders\"", sql);
        Assert.Contains("\"id\"",              sql);
        Assert.Contains("\"name\"",            sql);
    }

    [Fact]
    public void Snowflake_Insert_NullValue_UsesNullLiteral()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", name = (string?)null });

        var sql = SnowflakeDriver.BuildEventSql(Orders, evt);

        Assert.Contains("NULL", sql);
    }

    [Fact]
    public void Snowflake_Insert_Boolean_UsesTrueFalse()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", active = true });

        var sql = SnowflakeDriver.BuildEventSql(Orders, evt);

        Assert.Contains("TRUE", sql);
    }

    [Fact]
    public void Snowflake_Insert_Number_IsRawLiteral()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", amount = 123.45 });

        var sql = SnowflakeDriver.BuildEventSql(Orders, evt);

        Assert.Contains("123.45",    sql);
        Assert.DoesNotContain("'123.45'", sql);
    }

    [Fact]
    public void Snowflake_Insert_StringValue_IsQuoted()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", name = "hello" });

        var sql = SnowflakeDriver.BuildEventSql(Orders, evt);

        Assert.Contains("'hello'", sql);
    }

    [Fact]
    public void Snowflake_Insert_StringWithSingleQuote_IsEscaped()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", name = "O'Brien" });

        var sql = SnowflakeDriver.BuildEventSql(Orders, evt);

        Assert.Contains("'O''Brien'", sql);
        // After removing all escaped pairs, no lone quote should remain
        var stripped = sql.Replace("''", "");
        Assert.Equal(0, stripped.Count(c => c == '\'') % 2);
    }

    // ── DELETE tests ───────────────────────────────────────────────────────────

    [Fact]
    public void Snowflake_Delete_ProducesDeleteStatement()
    {
        var evt = DriverTestHelpers.MakeDelete("eds_test_orders", "id1");

        var sql = SnowflakeDriver.BuildEventSql(Orders, evt);

        Assert.StartsWith("DELETE FROM", sql.TrimStart());
        Assert.Contains("\"eds_test_orders\"", sql);
    }

    [Fact]
    public void Snowflake_Delete_UsesKeyForWhereClause()
    {
        var evt = DriverTestHelpers.MakeDelete("eds_test_orders", "some-id");

        var sql = SnowflakeDriver.BuildEventSql(Orders, evt);

        Assert.Contains("WHERE",       sql);
        Assert.Contains("\"id\"",      sql);
        Assert.Contains("'some-id'",   sql);
    }

    // ── Security / escaping tests ──────────────────────────────────────────────

    [Fact]
    public void Snowflake_Insert_SqlInjection_IsEscaped()
    {
        var injection = "'; DROP TABLE eds_test_orders; --";
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", name = injection });

        var sql = SnowflakeDriver.BuildEventSql(Orders, evt);

        // After removing all escaped pairs, remaining quotes must be balanced
        var stripped = sql.Replace("''", "");
        Assert.Equal(0, stripped.Count(c => c == '\'') % 2);
    }

    [Fact]
    public void Snowflake_QuoteId_WithDoubleQuote_IsEscaped()
    {
        var schema = DriverTestHelpers.MakeSchema("t", ["id"], new()
        {
            ["id"]         = new() { Type = "string" },
            ["col\"name"]  = new() { Type = "string" },   // double-quote in column name
        });
        var evt = DriverTestHelpers.MakeInsert("t", "1", new { id = "1" });

        var sql = SnowflakeDriver.BuildEventSql(schema, evt);

        Assert.Contains("\"col\"\"name\"", sql);   // double-quote escaped as two double-quotes
    }

    // ── Composite primary key tests ────────────────────────────────────────────

    [Fact]
    public void Snowflake_Insert_MatchesOnAllPrimaryKeys()
    {
        var schema = DriverTestHelpers.MakeSchema("order_lines", ["order_id", "line_num"], new()
        {
            ["order_id"]  = new() { Type = "string",  Nullable = false },
            ["line_num"]  = new() { Type = "integer", Nullable = false },
            ["product"]   = new() { Type = "string",  Nullable = true  },
            ["qty"]       = new() { Type = "integer", Nullable = true  },
        });
        var evt = DriverTestHelpers.MakeInsert("order_lines", "ord-1",
            new { order_id = "ord-1", line_num = 3, product = "Widget", qty = 5 });

        var sql = SnowflakeDriver.BuildEventSql(schema, evt);

        // ON clause must reference both primary key columns
        var onClause = sql[sql.IndexOf("ON ", StringComparison.OrdinalIgnoreCase)..];
        Assert.Contains("\"order_id\"",  onClause);
        Assert.Contains("\"line_num\"",  onClause);
    }
}
