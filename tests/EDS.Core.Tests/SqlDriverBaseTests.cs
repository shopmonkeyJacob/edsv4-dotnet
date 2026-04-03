using EDS.Core.Abstractions;
using EDS.Core.Drivers;
using EDS.Core.Models;
using Microsoft.Extensions.Logging;
using System.Data.Common;
using System.Text.Json;

namespace EDS.Core.Tests;

/// <summary>
/// Minimal concrete SqlDriverBase implementation used only in tests to expose
/// the protected QuoteJsonElement and QuoteString helpers.
/// </summary>
internal sealed class TestableSqlDriver : SqlDriverBase
{
    // ── Expose protected helpers ──────────────────────────────────────────────
    public string ExposedQuoteJsonElement(JsonElement el) => QuoteJsonElement(el);
    public string ExposedQuoteString(string value)        => QuoteString(value);
    public string ExposedQuoteId(string name)             => QuoteId(name);

    // ── Minimal abstract implementations (not used in these tests) ───────────
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
}

public class SqlDriverBaseTests
{
    private static readonly TestableSqlDriver Driver = new();

    private static JsonElement Json(string rawJson) =>
        JsonDocument.Parse(rawJson).RootElement.Clone();

    // ── QuoteJsonElement: null / undefined ────────────────────────────────────

    [Fact]
    public void QuoteJsonElement_Null_ReturnsNullLiteral() =>
        Assert.Equal("NULL", Driver.ExposedQuoteJsonElement(Json("null")));

    // ── QuoteJsonElement: booleans ────────────────────────────────────────────

    [Fact]
    public void QuoteJsonElement_True_ReturnsDialectTrue() =>
        Assert.Equal("1", Driver.ExposedQuoteJsonElement(Json("true")));   // base driver uses 1/0

    [Fact]
    public void QuoteJsonElement_False_ReturnsDialectFalse() =>
        Assert.Equal("0", Driver.ExposedQuoteJsonElement(Json("false")));

    // ── QuoteJsonElement: numbers ─────────────────────────────────────────────

    [Theory]
    [InlineData("0")]
    [InlineData("42")]
    [InlineData("-7")]
    [InlineData("3.14")]
    [InlineData("1.23e10")]
    [InlineData("-0.001")]
    public void QuoteJsonElement_ValidNumber_ReturnsRawText(string number) =>
        Assert.Equal(number, Driver.ExposedQuoteJsonElement(Json(number)));

    // ── QuoteJsonElement: strings ─────────────────────────────────────────────

    [Fact]
    public void QuoteJsonElement_SimpleString_IsSingleQuoted() =>
        Assert.Equal("'hello'", Driver.ExposedQuoteJsonElement(Json("\"hello\"")));

    [Fact]
    public void QuoteJsonElement_EmptyString_IsEmptyQuoted() =>
        Assert.Equal("''", Driver.ExposedQuoteJsonElement(Json("\"\"")));

    [Fact]
    public void QuoteJsonElement_StringWithSingleQuote_IsDoubled() =>
        Assert.Equal("'O''Brien'", Driver.ExposedQuoteJsonElement(Json("\"O'Brien\"")));

    [Fact]
    public void QuoteJsonElement_SqlInjectionAttempt_IsEscaped()
    {
        var el = Json("\"'; DROP TABLE orders; --\"");
        var result = Driver.ExposedQuoteJsonElement(el);
        // The content is present but safely embedded as a quoted string literal.
        // Safety invariant: after removing all escaped pairs (''), no lone quote remains.
        Assert.StartsWith("'", result);
        Assert.EndsWith("'", result);
        var stripped = result.Replace("''", "");
        Assert.Equal(0, stripped.Count(c => c == '\'') % 2);
    }

    [Fact]
    public void QuoteJsonElement_StringWithUnicode_IsPreserved()
    {
        var el = Json("\"こんにちは\"");
        var result = Driver.ExposedQuoteJsonElement(el);
        Assert.Equal("'こんにちは'", result);
    }

    [Fact]
    public void QuoteJsonElement_StringWithEmoji_IsPreserved()
    {
        var el = Json("\"hello 🎉\"");
        var result = Driver.ExposedQuoteJsonElement(el);
        Assert.Equal("'hello 🎉'", result);
    }

    [Fact]
    public void QuoteJsonElement_StringWithNewlines_IsPreserved()
    {
        var el = Json("\"line1\\nline2\"");
        var result = Driver.ExposedQuoteJsonElement(el);
        Assert.StartsWith("'", result);
        Assert.Contains("line1", result);
        Assert.Contains("line2", result);
    }

    [Fact]
    public void QuoteJsonElement_StringWithMultipleQuotes_AllDoubled()
    {
        var el = Json("\"it's the user's data\"");
        var result = Driver.ExposedQuoteJsonElement(el);
        Assert.Equal("'it''s the user''s data'", result);
    }

    // ── QuoteJsonElement: objects and arrays ──────────────────────────────────

    [Fact]
    public void QuoteJsonElement_Object_IsQuotedJsonString()
    {
        var el = Json("{\"key\":\"value\"}");
        var result = Driver.ExposedQuoteJsonElement(el);
        Assert.StartsWith("'", result);
        Assert.EndsWith("'", result);
        Assert.Contains("key", result);
    }

    [Fact]
    public void QuoteJsonElement_Array_IsQuotedJsonString()
    {
        var el = Json("[1,2,3]");
        var result = Driver.ExposedQuoteJsonElement(el);
        Assert.StartsWith("'", result);
        Assert.EndsWith("'", result);
        Assert.Contains("1", result);
    }

    [Fact]
    public void QuoteJsonElement_NestedObjectWithQuotes_IsSafelyEscaped()
    {
        var el = Json("{\"name\":\"O'Brien\"}");
        var result = Driver.ExposedQuoteJsonElement(el);
        // The raw JSON contains a single quote — it must be doubled
        Assert.StartsWith("'", result);
        Assert.EndsWith("'", result);
        Assert.Contains("''", result);
    }

    // ── QuoteString ───────────────────────────────────────────────────────────

    [Fact]
    public void QuoteString_Normal_IsSingleQuoted() =>
        Assert.Equal("'hello'", Driver.ExposedQuoteString("hello"));

    [Fact]
    public void QuoteString_Empty_IsEmptyQuoted() =>
        Assert.Equal("''", Driver.ExposedQuoteString(""));

    [Fact]
    public void QuoteString_WithSingleQuote_IsDoubled() =>
        Assert.Equal("'O''Brien'", Driver.ExposedQuoteString("O'Brien"));

    [Fact]
    public void QuoteString_OnlyQuotes_AllDoubled() =>
        Assert.Equal("''''", Driver.ExposedQuoteString("'"));

    [Fact]
    public void QuoteString_ClassicInjection_IsEscaped() =>
        Assert.Equal("'''; DROP TABLE t; --'", Driver.ExposedQuoteString("'; DROP TABLE t; --"));

    [Fact]
    public void QuoteString_TwoConsecutiveQuotes_AllDoubled() =>
        Assert.Equal("'he said ''hi'' there'", Driver.ExposedQuoteString("he said 'hi' there"));
}
