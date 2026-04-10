using EDS.Importer;

namespace EDS.Integration.Tests.Importers;

/// <summary>
/// Pure unit tests for CrdbFileParser.
/// No database required — these test filename parsing and timestamp conversion only.
/// </summary>
public class CrdbFileParserTests
{
    // ── Valid filenames used across multiple tests ─────────────────────────────

    private const string ValidGz    = "202403151430221234567890000000000-n1-0-orders-0.ndjson.gz";
    private const string ValidPlain = "202403151430221234567890000000000-n1-0-work_orders-0.ndjson";
    private const string ValidAlt   = "202312251200001234567890000000000-n5-3-invoice_items-2.ndjson.gz";

    // ── TryParse tests ─────────────────────────────────────────────────────────

    [Fact]
    public void TryParse_ValidGzippedFilename_ReturnsTableAndTimestamp()
    {
        var result = CrdbFileParser.TryParse(ValidGz, out var table, out var timestamp);

        Assert.True(result);
        Assert.Equal("orders", table);
        Assert.Equal(2024, timestamp.Year);
    }

    [Fact]
    public void TryParse_ValidPlainNdjson_Succeeds()
    {
        var result = CrdbFileParser.TryParse(ValidPlain, out var table, out var timestamp);

        Assert.True(result);
        Assert.Equal("work_orders", table);
        Assert.NotEqual(default, timestamp);
    }

    [Fact]
    public void TryParse_InvalidFilename_ReturnsFalse()
    {
        var result = CrdbFileParser.TryParse("README.txt", out var table, out _);

        Assert.False(result);
        Assert.Equal(string.Empty, table);
    }

    [Fact]
    public void TryParse_EmptyString_ReturnsFalse()
    {
        var result = CrdbFileParser.TryParse(string.Empty, out var table, out _);

        Assert.False(result);
        Assert.Equal(string.Empty, table);
    }

    [Fact]
    public void TryParse_ExtractsCorrectTableName()
    {
        CrdbFileParser.TryParse(ValidAlt, out var table, out _);

        Assert.Equal("invoice_items", table);
    }

    [Fact]
    public void TryParse_ExtractsCorrectTimestamp()
    {
        CrdbFileParser.TryParse(ValidGz, out _, out var timestamp);

        Assert.Equal(2024,  timestamp.Year);
        Assert.Equal(3,     timestamp.Month);
        Assert.Equal(15,    timestamp.Day);
        Assert.Equal(14,    timestamp.Hour);
        Assert.Equal(30,    timestamp.Minute);
        Assert.Equal(22,    timestamp.Second);
    }

    [Fact]
    public void TryParse_FullPathInput_WorksCorrectly()
    {
        var fullPath = $"/data/crdb/exports/{ValidGz}";

        var result = CrdbFileParser.TryParse(fullPath, out var table, out var timestamp);

        Assert.True(result);
        Assert.Equal("orders", table);
        Assert.Equal(2024, timestamp.Year);
    }

    [Theory]
    [InlineData("202403151430221234567890000000000-n1-0-orders-0.ndjson.gz",        "orders")]
    [InlineData("202403151430221234567890000000000-n1-0-work_orders-0.ndjson",      "work_orders")]
    [InlineData("202312251200001234567890000000000-n5-3-invoice_items-2.ndjson.gz", "invoice_items")]
    public void TryParse_MultipleTableNames_ExtractsCorrectTable(string filename, string expectedTable)
    {
        var result = CrdbFileParser.TryParse(filename, out var table, out _);

        Assert.True(result);
        Assert.Equal(expectedTable, table);
    }

    // ── ParseTimestamp tests ───────────────────────────────────────────────────

    [Fact]
    public void ParseTimestamp_KnownValue_ReturnsCorrectDate()
    {
        // "20240315143000" = 2024-03-15 14:30:00 UTC, followed by 9 zero-nanoseconds + 10 logical digits
        var ts = "20240315143000" + "000000000" + "0000000000";

        var result = CrdbFileParser.ParseTimestamp(ts);

        Assert.Equal(2024, result.Year);
        Assert.Equal(3,    result.Month);
        Assert.Equal(15,   result.Day);
        Assert.Equal(14,   result.Hour);
        Assert.Equal(30,   result.Minute);
        Assert.Equal(0,    result.Second);
        Assert.Equal(TimeSpan.Zero, result.Offset);
    }

    [Fact]
    public void ParseTimestamp_WithNanoseconds_IncludesSubseconds()
    {
        // 123456789 nanoseconds = 1234567 ticks (100 ns each), so Ticks%TimeSpan.TicksPerSecond != 0
        var ts = "20240315143000" + "123456789" + "0000000000";

        var result = CrdbFileParser.ParseTimestamp(ts);

        Assert.Equal(2024, result.Year);
        // Nanoseconds / 100 = 1234567 ticks added; result should have sub-second component
        Assert.NotEqual(0, result.Millisecond + result.Microsecond);
    }

    [Fact]
    public void ParseTimestamp_TooShort_ReturnsNow()
    {
        var before = DateTimeOffset.UtcNow.AddSeconds(-1);
        var result = CrdbFileParser.ParseTimestamp("20240315");  // only 8 chars, < 23
        var after  = DateTimeOffset.UtcNow.AddSeconds(1);

        Assert.InRange(result, before, after);
    }

    [Fact]
    public void ParseTimestamp_InvalidFormat_ReturnsNow()
    {
        // Length >= 23 but not a valid date
        var before = DateTimeOffset.UtcNow.AddSeconds(-1);
        var result = CrdbFileParser.ParseTimestamp("XXXXXXXXXXXXXXXXXXXXXXX");
        var after  = DateTimeOffset.UtcNow.AddSeconds(1);

        Assert.InRange(result, before, after);
    }
}
