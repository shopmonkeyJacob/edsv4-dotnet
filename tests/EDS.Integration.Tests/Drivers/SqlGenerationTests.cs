using EDS.Core.Drivers;
using EDS.Core.Models;
using EDS.Drivers.MySQL;
using EDS.Drivers.PostgreSQL;
using EDS.Drivers.SqlServer;
using EDS.Integration.Tests.Helpers;
using System.Reflection;

namespace EDS.Integration.Tests.Drivers;

/// <summary>
/// Pure unit tests for BuildSql on each concrete SQL driver.
/// No database required — these test the generated SQL string only.
/// BuildSql is protected, so accessed via reflection.
/// </summary>
public class SqlGenerationTests
{
    // ── Reflection helper ─────────────────────────────────────────────────────

    private static string BuildSql(SqlDriverBase driver, DbChangeEvent evt, Schema schema)
    {
        var method = driver.GetType()
            .GetMethod("BuildSql", BindingFlags.NonPublic | BindingFlags.Instance)!;
        return (string)method.Invoke(driver, [evt, schema])!;
    }

    // Convenience wrappers for each driver
    private static string PgSql(DbChangeEvent evt, Schema schema)  => BuildSql(new PostgreSqlDriver(), evt, schema);
    private static string MySql(DbChangeEvent evt, Schema schema)  => BuildSql(new MySqlDriver(),      evt, schema);
    private static string MsSql(DbChangeEvent evt, Schema schema)  => BuildSql(new SqlServerDriver(),  evt, schema);

    private static readonly Schema Orders = DriverTestHelpers.OrdersSchema();

    // ═══════════════════════════════════════════════════════════════════════════
    // PostgreSQL
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void PG_Insert_ProducesInsertOnConflict()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1", new { id = "id1", name = "Test", amount = 9.99 });
        var sql = PgSql(evt, Orders);

        Assert.Contains("INSERT INTO", sql);
        Assert.Contains("ON CONFLICT", sql);
        Assert.Contains("DO UPDATE SET", sql);
        Assert.Contains("\"eds_test_orders\"", sql);
    }

    [Fact]
    public void PG_Insert_MissingColumnsInPayload_UsesNull()
    {
        // Payload has only id — all other schema columns should become NULL
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1", new { id = "id1" });
        var sql = PgSql(evt, Orders);

        // The value list should contain NULLs for missing columns
        var valuesSection = sql[sql.IndexOf("VALUES", StringComparison.OrdinalIgnoreCase)..];
        Assert.Contains("NULL", valuesSection);
    }

    [Fact]
    public void PG_Insert_NullValueInPayload_UsesNullLiteral()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", name = (string?)null });
        var sql = PgSql(evt, Orders);

        Assert.Contains("NULL", sql);
    }

    [Fact]
    public void PG_Insert_StringWithSingleQuote_IsEscaped()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", name = "O'Brien" });
        var sql = PgSql(evt, Orders);

        Assert.Contains("'O''Brien'", sql);
        // Ensure there is no unmatched single quote that would break the SQL
        var singleQuoteCount = sql.Replace("''", "").Count(c => c == '\'');
        Assert.Equal(0, singleQuoteCount % 2);
    }

    [Fact]
    public void PG_Insert_SqlInjection_IsEscaped()
    {
        var injection = "'; DROP TABLE eds_test_orders; --";
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", name = injection });
        var sql = PgSql(evt, Orders);

        // After removing all quoted pairs, no lone quote should remain
        var stripped = sql.Replace("''", "");
        var quoteCount = stripped.Count(c => c == '\'');
        Assert.Equal(0, quoteCount % 2);
        // The dangerous string is present but safely embedded
        Assert.Contains("DROP TABLE", sql);        // content is there
        Assert.DoesNotContain(";\nDROP", sql);     // but never executed as a statement
    }

    [Fact]
    public void PG_Insert_Unicode_IsPreserved()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", name = "こんにちは 🎉" });
        var sql = PgSql(evt, Orders);

        Assert.Contains("こんにちは 🎉", sql);
    }

    [Fact]
    public void PG_Insert_Newline_IsPreservedInsideQuotes()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", name = "line1\nline2" });
        var sql = PgSql(evt, Orders);

        Assert.Contains("line1", sql);
        Assert.Contains("line2", sql);
    }

    [Fact]
    public void PG_Insert_Boolean_UsesTrueFalse()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", active = true });
        var sql = PgSql(evt, Orders);

        Assert.Contains("TRUE", sql);
    }

    [Fact]
    public void PG_Insert_Number_IsRawLiteral()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", amount = 123.45 });
        var sql = PgSql(evt, Orders);

        Assert.Contains("123.45", sql);
        // Numbers must NOT be quoted
        Assert.DoesNotContain("'123.45'", sql);
    }

    [Fact]
    public void PG_Insert_JsonObject_IsQuotedString()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", meta = new { key = "value" } });
        var sql = PgSql(evt, Orders);

        // Object values are serialized as JSON and quoted as strings
        Assert.Contains("\"key\"", sql);
    }

    [Fact]
    public void PG_Update_WithDiff_OnlyUpdatesNamedColumns()
    {
        var evt = DriverTestHelpers.MakeUpdate("eds_test_orders", "id1",
            new { id = "id1", name = "New Name", amount = 5.0 },
            diff: ["name"]);  // only name changed
        var sql = PgSql(evt, Orders);

        var updateSection = sql[sql.IndexOf("DO UPDATE SET", StringComparison.OrdinalIgnoreCase)..];
        Assert.Contains("\"name\"", updateSection);
        Assert.DoesNotContain("\"amount\"", updateSection);
    }

    [Fact]
    public void PG_Update_WithoutDiff_UpdatesAllNonPkColumns()
    {
        var evt = DriverTestHelpers.MakeUpdate("eds_test_orders", "id1",
            new { id = "id1", name = "New", amount = 5.0 });
        var sql = PgSql(evt, Orders);

        var updateSection = sql[sql.IndexOf("DO UPDATE SET", StringComparison.OrdinalIgnoreCase)..];
        Assert.Contains("\"name\"", updateSection);
        Assert.Contains("\"amount\"", updateSection);
    }

    [Fact]
    public void PG_Delete_ProducesDeleteStatement()
    {
        var evt = DriverTestHelpers.MakeDelete("eds_test_orders", "id1");
        var sql = PgSql(evt, Orders);

        Assert.StartsWith("DELETE FROM", sql.TrimStart());
        Assert.Contains("\"eds_test_orders\"", sql);
        Assert.Contains("\"id\"", sql);
    }

    [Fact]
    public void PG_QuoteId_WithDoubleQuote_IsEscaped()
    {
        // A schema with a column name containing a double-quote (unusual but must be safe)
        var schema = DriverTestHelpers.MakeSchema("t", ["id"], new()
        {
            ["id"]         = new() { Type = "string" },
            ["col\"name"]  = new() { Type = "string" },   // double-quote in name
        });
        var evt = DriverTestHelpers.MakeInsert("t", "1", new { id = "1" });
        var sql = PgSql(evt, schema);

        Assert.Contains("\"col\"\"name\"", sql);   // double-quote escaped as two double-quotes
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // MySQL
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void MySQL_Insert_ProducesOnDuplicateKeyUpdate()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", name = "Test" });
        var sql = MySql(evt, Orders);

        Assert.Contains("INSERT INTO", sql);
        Assert.Contains("ON DUPLICATE KEY UPDATE", sql);
        Assert.Contains("`eds_test_orders`", sql);
    }

    [Fact]
    public void MySQL_Insert_StringWithQuote_IsEscaped()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", name = "O'Brien" });
        var sql = MySql(evt, Orders);

        Assert.Contains("'O''Brien'", sql);
        var stripped = sql.Replace("''", "");
        Assert.Equal(0, stripped.Count(c => c == '\'') % 2);
    }

    [Fact]
    public void MySQL_Insert_SqlInjection_IsEscaped()
    {
        var injection = "'; DROP TABLE eds_test_orders; --";
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", name = injection });
        var sql = MySql(evt, Orders);

        var stripped = sql.Replace("''", "");
        Assert.Equal(0, stripped.Count(c => c == '\'') % 2);
    }

    [Fact]
    public void MySQL_Insert_DateTime_IsReformattedToMySqlFormat()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", createdAt = "2024-03-15T14:30:00Z" });
        var sql = MySql(evt, Orders);

        // MySQL TIMESTAMP format: 'YYYY-MM-DD HH:mm:ss.ffffff'
        Assert.Contains("2024-03-15 14:30:00", sql);
        Assert.DoesNotContain("2024-03-15T14:30:00Z", sql);   // no ISO T/Z format
    }

    [Fact]
    public void MySQL_Delete_ProducesDeleteStatement()
    {
        var evt = DriverTestHelpers.MakeDelete("eds_test_orders", "id1");
        var sql = MySql(evt, Orders);

        Assert.StartsWith("DELETE FROM", sql.TrimStart());
        Assert.Contains("`eds_test_orders`", sql);
    }

    [Fact]
    public void MySQL_Boolean_UsesBinaryLiteral()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", active = false });
        var sql = MySql(evt, Orders);

        Assert.Contains("0", sql);
    }

    [Fact]
    public void MySQL_QuoteId_WithBacktick_IsEscaped()
    {
        var schema = DriverTestHelpers.MakeSchema("t", ["id"], new()
        {
            ["id"]         = new() { Type = "string" },
            ["col`name"]   = new() { Type = "string" },   // backtick in name
        });
        var evt = DriverTestHelpers.MakeInsert("t", "1", new { id = "1" });
        var sql = MySql(evt, schema);

        Assert.Contains("`col``name`", sql);   // backtick escaped as double-backtick
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SQL Server
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void SqlServer_Insert_ProducesMergeStatement()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", name = "Test" });
        var sql = MsSql(evt, Orders);

        Assert.Contains("MERGE", sql);
        Assert.Contains("WHEN MATCHED THEN UPDATE", sql);
        Assert.Contains("WHEN NOT MATCHED THEN INSERT", sql);
        Assert.Contains("[eds_test_orders]", sql);
    }

    [Fact]
    public void SqlServer_Insert_StringHasNPrefix()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", name = "hello" });
        var sql = MsSql(evt, Orders);

        // SQL Server QuoteString prepends N for Unicode support
        Assert.Contains("N'hello'", sql);
    }

    [Fact]
    public void SqlServer_Insert_StringWithQuote_IsEscaped()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", name = "O'Brien" });
        var sql = MsSql(evt, Orders);

        Assert.Contains("N'O''Brien'", sql);
        var stripped = sql.Replace("''", "");
        Assert.Equal(0, stripped.Count(c => c == '\'') % 2);
    }

    [Fact]
    public void SqlServer_Insert_SqlInjection_IsEscaped()
    {
        var injection = "'; DROP TABLE eds_test_orders; --";
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", name = injection });
        var sql = MsSql(evt, Orders);

        var stripped = sql.Replace("''", "");
        Assert.Equal(0, stripped.Count(c => c == '\'') % 2);
    }

    [Fact]
    public void SqlServer_Insert_Unicode_IsPreserved()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "id1",
            new { id = "id1", name = "こんにちは" });
        var sql = MsSql(evt, Orders);

        Assert.Contains("こんにちは", sql);
        Assert.Contains("N'こんにちは'", sql);
    }

    [Fact]
    public void SqlServer_Delete_ProducesDeleteStatement()
    {
        var evt = DriverTestHelpers.MakeDelete("eds_test_orders", "id1");
        var sql = MsSql(evt, Orders);

        Assert.StartsWith("DELETE FROM", sql.TrimStart());
        Assert.Contains("[eds_test_orders]", sql);
    }

    [Fact]
    public void SqlServer_QuoteId_WithClosingBracket_IsEscaped()
    {
        var schema = DriverTestHelpers.MakeSchema("t", ["id"], new()
        {
            ["id"]       = new() { Type = "string" },
            ["col]name"] = new() { Type = "string" },   // ] in name
        });
        var evt = DriverTestHelpers.MakeInsert("t", "1", new { id = "1" });
        var sql = MsSql(evt, schema);

        Assert.Contains("[col]]name]", sql);   // ] escaped as ]]
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Cross-driver: same payload, dialect differences
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void AllDrivers_Insert_NoBareUnmatchedQuotes()
    {
        var evt = DriverTestHelpers.MakeInsert("eds_test_orders", "test-id",
            new { id = "test-id", name = "it's a \"test\" value", amount = 42.5, active = true });

        foreach (var sql in new[] { PgSql(evt, Orders), MySql(evt, Orders), MsSql(evt, Orders) })
        {
            // After removing all escaped pairs ('' and N''), remaining quotes must be balanced
            var stripped = sql.Replace("''", "").Replace("N'", "'");
            var quoteCount = stripped.Count(c => c == '\'');
            Assert.True(quoteCount % 2 == 0, $"Unbalanced quotes in SQL:\n{sql}");
        }
    }

    [Fact]
    public void AllDrivers_Delete_ContainsTableAndWhereClause()
    {
        var evt = DriverTestHelpers.MakeDelete("eds_test_orders", "some-id");

        foreach (var sql in new[] { PgSql(evt, Orders), MySql(evt, Orders), MsSql(evt, Orders) })
        {
            Assert.Contains("DELETE FROM", sql.ToUpperInvariant());
            Assert.Contains("WHERE", sql.ToUpperInvariant());
            Assert.Contains("some-id", sql);
        }
    }
}
