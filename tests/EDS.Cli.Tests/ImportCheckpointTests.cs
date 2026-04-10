using EDS.Cli;
using EDS.Infrastructure.Tracking;

namespace EDS.Cli.Tests;

/// <summary>
/// Resilience tests for ImportService checkpoint and table-export-info persistence.
/// These cover the regression where TryLoadCheckpointAsync crashed with a JsonException
/// when the tracker key held a bare JSON array written by an older build.
/// </summary>
public sealed class ImportCheckpointTests : IAsyncLifetime
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"eds-cli-tests-{Guid.NewGuid():N}.db");
    private SqliteTracker _tracker = null!;

    // ── IAsyncLifetime ────────────────────────────────────────────────────────

    public Task InitializeAsync()
    {
        _tracker = new SqliteTracker(_dbPath);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _tracker.DisposeAsync();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static ImportCheckpoint MakeCheckpoint(string jobId = "job-abc", List<string>? files = null) =>
        new()
        {
            JobId          = jobId,
            DownloadDir    = "/tmp/eds-downloads",
            CompletedFiles = files ?? [],
            StartedAt      = new DateTimeOffset(2024, 6, 1, 12, 0, 0, TimeSpan.Zero),
        };

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task SaveThenLoad_RoundTripsCheckpoint()
    {
        var ct = CancellationToken.None;
        var original = MakeCheckpoint("job-roundtrip", ["file1.ndjson.gz", "file2.ndjson.gz"]);

        await ImportService.SaveCheckpointAsync(_tracker, original, ct);
        var loaded = await ImportService.TryLoadCheckpointAsync(_tracker, ct);

        Assert.NotNull(loaded);
        Assert.Equal(original.JobId,          loaded.JobId);
        Assert.Equal(original.DownloadDir,    loaded.DownloadDir);
        Assert.Equal(original.StartedAt,      loaded.StartedAt);
        Assert.Equal(original.CompletedFiles, loaded.CompletedFiles);
    }

    [Fact]
    public async Task Load_NonExistentKey_ReturnsNull()
    {
        var result = await ImportService.TryLoadCheckpointAsync(_tracker, CancellationToken.None);

        Assert.Null(result);
    }

    [Fact]
    public async Task Load_CorruptJson_ReturnsNullAndDeletesKey()
    {
        // Simulate the old behavior: NdjsonGzImporter wrote a bare JSON array to the
        // checkpoint key instead of a serialised ImportCheckpoint object.
        // This is the core regression test — prior code threw JsonException here.
        var ct = CancellationToken.None;
        await _tracker.SetKeyAsync(ImportService.CheckpointTrackerKey,
            """["file1.ndjson.gz","file2.ndjson.gz"]""", ct);

        var result = await ImportService.TryLoadCheckpointAsync(_tracker, ct);

        Assert.Null(result);

        // The stale key must be removed so subsequent runs start fresh.
        var remaining = await _tracker.GetKeyAsync(ImportService.CheckpointTrackerKey, ct);
        Assert.Null(remaining);
    }

    [Fact]
    public async Task Load_ValidCheckpointWithCompletedFiles_RoundTrips()
    {
        var ct    = CancellationToken.None;
        var files = new List<string> { "alpha.ndjson.gz", "beta.ndjson.gz", "gamma.ndjson.gz" };
        var cp    = MakeCheckpoint("job-with-files", files);

        await ImportService.SaveCheckpointAsync(_tracker, cp, ct);
        var loaded = await ImportService.TryLoadCheckpointAsync(_tracker, ct);

        Assert.NotNull(loaded);
        Assert.Equal(files, loaded.CompletedFiles);
    }

    [Fact]
    public async Task Load_AfterDelete_ReturnsNull()
    {
        var ct = CancellationToken.None;
        await ImportService.SaveCheckpointAsync(_tracker, MakeCheckpoint(), ct);

        // Manually delete the key to simulate a user clearing state.
        await _tracker.DeleteKeyAsync(ImportService.CheckpointTrackerKey, ct);

        var result = await ImportService.TryLoadCheckpointAsync(_tracker, ct);
        Assert.Null(result);
    }

    [Fact]
    public async Task SaveTableExportInfo_ThenLoad_RoundTrips()
    {
        var ct = CancellationToken.None;
        var info = new List<TableExportInfo>
        {
            new("customers",    new DateTimeOffset(2024, 5, 1, 0, 0, 0, TimeSpan.Zero)),
            new("appointments", new DateTimeOffset(2024, 5, 2, 8, 30, 0, TimeSpan.Zero)),
        };

        await ImportService.SaveTableExportInfoAsync(_tracker, info, ct);
        var loaded = await ImportService.TryLoadTableExportInfoAsync(_tracker, ct);

        Assert.NotNull(loaded);
        Assert.Equal(2, loaded.Count);
        Assert.Equal("customers",    loaded[0].Table);
        Assert.Equal("appointments", loaded[1].Table);
        Assert.Equal(info[0].Timestamp, loaded[0].Timestamp);
        Assert.Equal(info[1].Timestamp, loaded[1].Timestamp);
    }

    [Fact]
    public async Task Load_TableExportInfo_NonExistent_ReturnsNull()
    {
        var result = await ImportService.TryLoadTableExportInfoAsync(_tracker, CancellationToken.None);

        Assert.Null(result);
    }

    [Fact]
    public async Task Checkpoint_CompletedFiles_PreservesAllEntries()
    {
        var ct    = CancellationToken.None;
        var files = Enumerable.Range(1, 100)
            .Select(i => $"part-{i:D4}.ndjson.gz")
            .ToList();
        var cp = MakeCheckpoint("job-100-files", files);

        await ImportService.SaveCheckpointAsync(_tracker, cp, ct);
        var loaded = await ImportService.TryLoadCheckpointAsync(_tracker, ct);

        Assert.NotNull(loaded);
        Assert.Equal(100, loaded.CompletedFiles.Count);
        Assert.Equal(files, loaded.CompletedFiles);
    }
}
