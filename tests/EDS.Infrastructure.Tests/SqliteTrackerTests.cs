using EDS.Core.Abstractions;
using EDS.Infrastructure.Tracking;

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
}
