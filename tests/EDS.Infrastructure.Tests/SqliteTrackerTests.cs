using EDS.Core.Abstractions;
using EDS.Core.Models;
using EDS.Infrastructure.Tracking;
using Microsoft.Data.Sqlite;
using System.Text.Json;

namespace EDS.Infrastructure.Tests;

public class SqliteTrackerTests : IAsyncLifetime
{
    private string _dbPath = string.Empty;
    private SqliteTracker _tracker = null!;

    public Task InitializeAsync()
    {
        _dbPath  = Path.GetTempFileName() + ".db";
        _tracker = new SqliteTracker(_dbPath);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _tracker.DisposeAsync();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    // ── GetKeyAsync ───────────────────────────────────────────────────────────

    [Fact]
    public async Task GetKey_NonExistentKey_ReturnsNull()
    {
        var result = await _tracker.GetKeyAsync("does-not-exist");

        Assert.Null(result);
    }

    // ── SetKeyAsync / GetKeyAsync ─────────────────────────────────────────────

    [Fact]
    public async Task SetKey_ThenGetKey_ReturnsValue()
    {
        await _tracker.SetKeyAsync("greeting", "hello");

        var result = await _tracker.GetKeyAsync("greeting");

        Assert.Equal("hello", result);
    }

    [Fact]
    public async Task SetKey_OverwritesExistingValue()
    {
        await _tracker.SetKeyAsync("key", "first");
        await _tracker.SetKeyAsync("key", "second");

        var result = await _tracker.GetKeyAsync("key");

        Assert.Equal("second", result);
    }

    // ── DeleteKeyAsync ────────────────────────────────────────────────────────

    [Fact]
    public async Task DeleteKey_ExistingKey_RemovesIt()
    {
        await _tracker.SetKeyAsync("to-delete", "value");

        await _tracker.DeleteKeyAsync("to-delete");

        var result = await _tracker.GetKeyAsync("to-delete");
        Assert.Null(result);
    }

    [Fact]
    public async Task DeleteKey_NonExistentKey_DoesNotThrow()
    {
        var ex = await Record.ExceptionAsync(() => _tracker.DeleteKeyAsync("ghost-key"));

        Assert.Null(ex);
    }

    // ── SetKeysAsync ──────────────────────────────────────────────────────────

    [Fact]
    public async Task SetKeys_MultiplePairs_AllPersisted()
    {
        var pairs = new Dictionary<string, string>
        {
            ["alpha"]   = "1",
            ["beta"]    = "2",
            ["gamma"]   = "3",
            ["delta"]   = "4"
        };

        await _tracker.SetKeysAsync(pairs);

        Assert.Equal("1", await _tracker.GetKeyAsync("alpha"));
        Assert.Equal("2", await _tracker.GetKeyAsync("beta"));
        Assert.Equal("3", await _tracker.GetKeyAsync("gamma"));
        Assert.Equal("4", await _tracker.GetKeyAsync("delta"));
    }

    [Fact]
    public async Task SetKeys_IsAtomic_AllOrNothing()
    {
        // Verify that after a successful SetKeysAsync all keys are visible —
        // i.e., none are missing even when multiple are written in one call.
        var pairs = new Dictionary<string, string>
        {
            ["batch:a"] = "A",
            ["batch:b"] = "B",
            ["batch:c"] = "C"
        };

        await _tracker.SetKeysAsync(pairs);

        foreach (var (key, expected) in pairs)
        {
            var actual = await _tracker.GetKeyAsync(key);
            Assert.Equal(expected, actual);
        }
    }

    // ── DeleteKeysWithPrefixAsync ─────────────────────────────────────────────

    [Fact]
    public async Task DeleteKeysWithPrefix_RemovesMatchingKeys()
    {
        await _tracker.SetKeysAsync(new Dictionary<string, string>
        {
            ["ns:one"]   = "1",
            ["ns:two"]   = "2",
            ["ns:three"] = "3"
        });

        await _tracker.DeleteKeysWithPrefixAsync("ns:");

        Assert.Null(await _tracker.GetKeyAsync("ns:one"));
        Assert.Null(await _tracker.GetKeyAsync("ns:two"));
        Assert.Null(await _tracker.GetKeyAsync("ns:three"));
    }

    [Fact]
    public async Task DeleteKeysWithPrefix_LeavesNonMatchingKeys()
    {
        await _tracker.SetKeyAsync("ns:target", "del");
        await _tracker.SetKeyAsync("other:keep", "keep");

        await _tracker.DeleteKeysWithPrefixAsync("ns:");

        Assert.Equal("keep", await _tracker.GetKeyAsync("other:keep"));
    }

    [Fact]
    public async Task DeleteKeysWithPrefix_ReturnsCountOfDeleted()
    {
        await _tracker.SetKeysAsync(new Dictionary<string, string>
        {
            ["pfx:x"] = "1",
            ["pfx:y"] = "2",
            ["pfx:z"] = "3",
            ["other"] = "4"
        });

        var deleted = await _tracker.DeleteKeysWithPrefixAsync("pfx:");

        Assert.Equal(3, deleted);
    }

    // ── Persistence across reopen ─────────────────────────────────────────────

    [Fact]
    public async Task GetKey_AfterReopenDatabase_ValuePersists()
    {
        await _tracker.SetKeyAsync("persistent", "survives");
        await _tracker.DisposeAsync();

        // Reopen with the same file path.
        _tracker = new SqliteTracker(_dbPath);

        var result = await _tracker.GetKeyAsync("persistent");

        Assert.Equal("survives", result);
    }

    // ── Concurrency ───────────────────────────────────────────────────────────

    [Fact]
    public async Task ConcurrentWrites_DoNotCorruptData()
    {
        const int count = 20;

        var tasks = Enumerable.Range(0, count)
            .Select(i => _tracker.SetKeyAsync($"concurrent:{i}", $"value-{i}"));

        await Task.WhenAll(tasks);

        for (var i = 0; i < count; i++)
        {
            var result = await _tracker.GetKeyAsync($"concurrent:{i}");
            Assert.Equal($"value-{i}", result);
        }
    }

    // ── PushAsync (IDlqWriter) ────────────────────────────────────────────────

    [Fact]
    public async Task PushAsync_SingleEvent_RowPersistedWithCorrectFields()
    {
        var evt = MakeEvent("orders", "insert", companyId: "c1", locationId: "l1",
            after: """{"id":"evt-1","name":"Widget"}""");

        await _tracker.PushAsync([evt], "connection refused", retryCount: 5);

        var row = await ReadFirstDlqRowAsync();
        Assert.Equal(evt.Id,         row.EventId);
        Assert.Equal("orders",       row.TableName);
        Assert.Equal("insert",       row.Operation);
        Assert.Equal("c1",           row.CompanyId);
        Assert.Equal("l1",           row.LocationId);
        Assert.Equal(5L,             row.RetryCount);
        Assert.Equal("connection refused", row.Error);
        Assert.Contains("Widget",    row.Payload);   // raw After JSON
    }

    [Fact]
    public async Task PushAsync_MultipleEvents_AllRowsWrittenAtomically()
    {
        var evts = Enumerable.Range(0, 3)
            .Select(i => MakeEvent("invoices", "update", after: $$"""{"id":"{{i}}"}"""))
            .ToList();

        await _tracker.PushAsync(evts, "timeout", retryCount: 3);

        Assert.Equal(3, await CountDlqRowsAsync());
    }

    [Fact]
    public async Task PushAsync_NullOptionalFields_StoredAsNullInDatabase()
    {
        var evt = MakeEvent("services", "delete", companyId: null, locationId: null);

        await _tracker.PushAsync([evt], "error", retryCount: 1);

        var row = await ReadFirstDlqRowAsync();
        Assert.Null(row.CompanyId);
        Assert.Null(row.LocationId);
    }

    [Fact]
    public async Task PushAsync_EventWithNoPayload_PayloadIsNull()
    {
        var evt = MakeEvent("parts", "delete");  // no after/before

        await _tracker.PushAsync([evt], "timeout", retryCount: 0);

        var row = await ReadFirstDlqRowAsync();
        Assert.Null(row.Payload);
    }

    [Fact]
    public async Task PushAsync_DeleteEvent_StoresBefore()
    {
        var evt = MakeEvent("parts", "delete",
            before: """{"id":"b1","qty":5}""");

        await _tracker.PushAsync([evt], "error", retryCount: 2);

        var row = await ReadFirstDlqRowAsync();
        Assert.Contains("qty", row.Payload);    // falls back to Before
    }

    [Fact]
    public async Task PushAsync_LongError_TruncatedTo2000Chars()
    {
        var longError = new string('x', 3000);
        var evt = MakeEvent("orders", "insert");

        await _tracker.PushAsync([evt], longError, retryCount: 1);

        var row = await ReadFirstDlqRowAsync();
        Assert.Equal(2000, row.Error.Length);
    }

    [Fact]
    public async Task PushAsync_EmptyList_WritesNoRows()
    {
        await _tracker.PushAsync([], "some error", retryCount: 1);

        Assert.Equal(0, await CountDlqRowsAsync());
    }

    [Fact]
    public async Task PushAsync_PersistsAcrossReopen()
    {
        var evt = MakeEvent("vehicles", "insert", after: """{"id":"v1"}""");
        await _tracker.PushAsync([evt], "disk full", retryCount: 5);
        await _tracker.DisposeAsync();

        _tracker = new SqliteTracker(_dbPath);

        Assert.Equal(1, await CountDlqRowsAsync());
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static DbChangeEvent MakeEvent(
        string table,
        string operation,
        string? companyId  = null,
        string? locationId = null,
        string? after      = null,
        string? before     = null) =>
        new()
        {
            Id         = Guid.NewGuid().ToString(),
            Table      = table,
            Operation  = operation,
            Key        = ["pk-1"],
            CompanyId  = companyId,
            LocationId = locationId,
            After      = after  is not null ? JsonDocument.Parse(after).RootElement  : null,
            Before     = before is not null ? JsonDocument.Parse(before).RootElement : null,
        };

    private async Task<int> CountDlqRowsAsync()
    {
        await using var conn = new SqliteConnection($"Data Source={_dbPath}");
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM dlq";
        return (int)(long)(await cmd.ExecuteScalarAsync())!;
    }

    private async Task<(string EventId, string TableName, string Operation,
        string? CompanyId, string? LocationId, long RetryCount, string Error, string? Payload)>
        ReadFirstDlqRowAsync()
    {
        await using var conn = new SqliteConnection($"Data Source={_dbPath}");
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText =
            "SELECT event_id, table_name, operation, company_id, location_id, " +
            "retry_count, error, payload FROM dlq ORDER BY id LIMIT 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();
        return (
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetString(4),
            reader.GetInt64(5),
            reader.GetString(6),
            reader.IsDBNull(7) ? null : reader.GetString(7)
        );
    }
}
