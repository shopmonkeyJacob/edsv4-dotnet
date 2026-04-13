using EDS.Cli;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace EDS.Cli.Tests;

/// <summary>
/// Resilience tests for the export-job retry / back-off logic in ImportService.
/// These tests exercise PollDownloadWithRetryAsync via injectable delegates — no
/// real HTTP calls are made.
///
/// NOTE: The retry feature is not yet in production.  These tests are marked E2E
/// so they are excluded from the release CI pipeline (--filter "Category!=E2E").
/// </summary>
[Trait("Category", "E2E")]
public sealed class ImportRetryTests
{
    private static readonly ILogger _log = NullLogger.Instance;
    private const string ApiUrl = "https://api.example.com";
    private const string ApiKey = "test-key";

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static ExportJobResponse MakeJob(bool completed, params (string table, string[] urls)[] tables)
    {
        var dict = new Dictionary<string, ExportJobTableData>(StringComparer.Ordinal);
        foreach (var (name, urls) in tables)
            dict[name] = new ExportJobTableData { Status = "Completed", Urls = [.. urls] };
        return new ExportJobResponse { Completed = completed, Tables = dict };
    }

    private static ExportJobResponse MakeFailedJob(params string[] failedTables)
    {
        var dict = new Dictionary<string, ExportJobTableData>(StringComparer.Ordinal);
        foreach (var name in failedTables)
            dict[name] = new ExportJobTableData { Status = "Failed", Urls = [] };
        return new ExportJobResponse { Completed = false, Tables = dict };
    }

    private static ExportJobResponse MakePartialJob(
        (string table, string[] urls)[] completed,
        string[] pending)
    {
        var dict = new Dictionary<string, ExportJobTableData>(StringComparer.Ordinal);
        foreach (var (name, urls) in completed)
            dict[name] = new ExportJobTableData { Status = "Completed", Urls = [.. urls] };
        foreach (var name in pending)
            dict[name] = new ExportJobTableData { Status = "InProgress", Urls = [] };
        return new ExportJobResponse { Completed = false, Tables = dict };
    }

    // No-op delay so tests run instantly.
    private static Task NoDelay(TimeSpan _, CancellationToken __) => Task.CompletedTask;

    // ── Retry delay values ─────────────────────────────────────────────────────

    [Fact]
    public void RetryDelays_AreCorrect()
    {
        int[] expected = [30, 60, 120, 240, 480];
        Assert.Equal(expected, ImportService._ImportRetryDelays);
    }

    // ── First-attempt success ─────────────────────────────────────────────────

    [Fact]
    public async Task FirstAttempt_Success_ReturnsTableInfos()
    {
        var job = MakeJob(completed: true,
            ("customers", ["https://cdn.example.com/2024-01-01T00:00:00Z.ndjson.gz"]),
            ("appointments", ["https://cdn.example.com/2024-01-01T01:00:00Z.ndjson.gz"]));

        int pollCalls = 0;
        int downloadCalls = 0;
        int createCalls = 0;

        var result = await ImportService.PollDownloadWithRetryAsync(
            ApiUrl, ApiKey, "job-001",
            destDir: Path.GetTempPath(),
            companyIds: null,
            locationIds: null,
            logger: _log,
            maxRetries: 5,
            pollFn: (_, _, _, _, _) => { pollCalls++; return Task.FromResult(job); },
            downloadFn: (_, _, _, _) =>
            {
                downloadCalls++;
                return Task.FromResult<IReadOnlyList<TableExportInfo>>([
                    new("customers", DateTimeOffset.UtcNow),
                    new("appointments", DateTimeOffset.UtcNow),
                ]);
            },
            createJobFn: (_, _, _, _) => { createCalls++; return Task.FromResult("job-new"); },
            delayFn: NoDelay);

        Assert.Equal(2, result.Count);
        Assert.Equal(1, pollCalls);
        Assert.Equal(1, downloadCalls);
        Assert.Equal(0, createCalls); // no retry needed
    }

    // ── One timeout, then success ─────────────────────────────────────────────

    [Fact]
    public async Task OneTimeout_ThenSuccess_ReturnsAllTableInfos()
    {
        // First poll: timeout with customers complete, appointments pending.
        // Second poll: success with appointments.
        int attempt = 0;
        int createCalls = 0;
        var delays = new List<TimeSpan>();

        var firstJob = MakePartialJob(
            completed: [("customers", ["https://cdn.example.com/2024-01-01T00:00:00Z.ndjson.gz"])],
            pending: ["appointments"]);

        var secondJob = MakeJob(completed: true,
            ("appointments", ["https://cdn.example.com/2024-01-01T01:00:00Z.ndjson.gz"]));

        Task<ExportJobResponse> PollFn(string _, string __, string ___, ILogger ____, CancellationToken _____)
        {
            attempt++;
            if (attempt == 1)
                throw new ExportTimeoutError("timed out", partialJob: firstJob);
            return Task.FromResult(secondJob);
        }

        var result = await ImportService.PollDownloadWithRetryAsync(
            ApiUrl, ApiKey, "job-001",
            destDir: Path.GetTempPath(),
            companyIds: null,
            locationIds: null,
            logger: _log,
            maxRetries: 5,
            pollFn: PollFn,
            downloadFn: (job, _, _, _) =>
            {
                var infos = job.Tables.Keys
                    .Select(t => new TableExportInfo(t, DateTimeOffset.UtcNow))
                    .ToList();
                return Task.FromResult<IReadOnlyList<TableExportInfo>>(infos);
            },
            createJobFn: (_, _, req, _) =>
            {
                createCalls++;
                Assert.Equal(["appointments"], req.Tables);
                return Task.FromResult("job-retry-1");
            },
            delayFn: (ts, _) => { delays.Add(ts); return Task.CompletedTask; });

        Assert.Equal(2, result.Count);
        Assert.Equal(1, createCalls);
        Assert.Single(delays);
        Assert.Equal(TimeSpan.FromSeconds(30), delays[0]); // first back-off
    }

    // ── All retries exhausted ─────────────────────────────────────────────────

    [Fact]
    public async Task AllRetriesExhausted_ThrowsExportTimeoutError()
    {
        // Every poll throws with an empty partial (no URLs), so no tables ever complete.
        var emptyJob = new ExportJobResponse
        {
            Completed = false,
            Tables    = new Dictionary<string, ExportJobTableData>
            {
                ["appointments"] = new ExportJobTableData { Status = "InProgress", Urls = [] },
            }
        };

        int pollCalls = 0;
        int createCalls = 0;

        var ex = await Assert.ThrowsAsync<ExportTimeoutError>(() =>
            ImportService.PollDownloadWithRetryAsync(
                ApiUrl, ApiKey, "job-001",
                destDir: Path.GetTempPath(),
                companyIds: null,
                locationIds: null,
                logger: _log,
                maxRetries: 3,
                pollFn: (_, _, _, _, _) =>
                {
                    pollCalls++;
                    throw new ExportTimeoutError("timed out", partialJob: emptyJob);
                },
                downloadFn: (_, _, _, _) => Task.FromResult<IReadOnlyList<TableExportInfo>>([]),
                createJobFn: (_, _, _, _) => { createCalls++; return Task.FromResult("job-retry"); },
                delayFn: NoDelay));

        Assert.NotNull(ex);
        Assert.Equal(4, pollCalls);    // initial + 3 retries
        Assert.Equal(3, createCalls);  // 3 retry jobs created
    }

    // ── Backoff delay sequence ────────────────────────────────────────────────

    [Fact]
    public async Task RetryDelays_FollowBackoffSequence()
    {
        var delays = new List<TimeSpan>();
        int pollCalls = 0;

        // 5 retries, all timing out — captures all 5 delays in the sequence.
        await Assert.ThrowsAsync<ExportTimeoutError>(() =>
            ImportService.PollDownloadWithRetryAsync(
                ApiUrl, ApiKey, "job-001",
                destDir: Path.GetTempPath(),
                companyIds: null,
                locationIds: null,
                logger: _log,
                maxRetries: 5,
                pollFn: (_, _, _, _, _) =>
                {
                    pollCalls++;
                    throw new ExportTimeoutError("timed out", partialJob: new ExportJobResponse
                    {
                        Completed = false,
                        Tables    = new Dictionary<string, ExportJobTableData>
                        {
                            ["t1"] = new ExportJobTableData { Status = "InProgress", Urls = [] }
                        }
                    });
                },
                downloadFn: (_, _, _, _) => Task.FromResult<IReadOnlyList<TableExportInfo>>([]),
                createJobFn: (_, _, _, _) => Task.FromResult("job-retry"),
                delayFn: (ts, _) => { delays.Add(ts); return Task.CompletedTask; }));

        Assert.Equal(5, delays.Count);
        Assert.Equal(TimeSpan.FromSeconds(30),  delays[0]);
        Assert.Equal(TimeSpan.FromSeconds(60),  delays[1]);
        Assert.Equal(TimeSpan.FromSeconds(120), delays[2]);
        Assert.Equal(TimeSpan.FromSeconds(240), delays[3]);
        Assert.Equal(TimeSpan.FromSeconds(480), delays[4]);
    }

    // ── Partial success on timeout ────────────────────────────────────────────

    [Fact]
    public async Task Timeout_WithSomeTables_DownloadsCompletedTablesBeforeRetry()
    {
        var downloaded = new List<string>();

        // First poll: 2 tables have URLs, 1 is still pending.
        var partialJob = MakePartialJob(
            completed: [
                ("customers",     ["https://cdn.example.com/2024-01-01T00:00:00Z.ndjson.gz"]),
                ("appointments",  ["https://cdn.example.com/2024-01-01T01:00:00Z.ndjson.gz"]),
            ],
            pending: ["invoices"]);

        var finalJob = MakeJob(completed: true,
            ("invoices", ["https://cdn.example.com/2024-01-01T02:00:00Z.ndjson.gz"]));

        int pollAttempt = 0;
        int createCalls = 0;

        var result = await ImportService.PollDownloadWithRetryAsync(
            ApiUrl, ApiKey, "job-001",
            destDir: Path.GetTempPath(),
            companyIds: null,
            locationIds: null,
            logger: _log,
            maxRetries: 5,
            pollFn: (_, _, _, _, _) =>
            {
                pollAttempt++;
                if (pollAttempt == 1)
                    throw new ExportTimeoutError("timed out", partialJob: partialJob);
                return Task.FromResult(finalJob);
            },
            downloadFn: (job, _, _, _) =>
            {
                var infos = job.Tables.Keys
                    .Select(t => { downloaded.Add(t); return new TableExportInfo(t, DateTimeOffset.UtcNow); })
                    .ToList();
                return Task.FromResult<IReadOnlyList<TableExportInfo>>(infos);
            },
            createJobFn: (_, _, req, _) =>
            {
                createCalls++;
                Assert.Equal(["invoices"], req.Tables);
                return Task.FromResult("job-retry-1");
            },
            delayFn: NoDelay);

        // All 3 tables should be in the result.
        Assert.Equal(3, result.Count);

        // Partial download happened first (customers + appointments), then invoices on retry.
        Assert.Contains("customers",    downloaded);
        Assert.Contains("appointments", downloaded);
        Assert.Contains("invoices",     downloaded);

        Assert.Equal(1, createCalls);
    }

    // ── No pending tables after timeout ──────────────────────────────────────

    [Fact]
    public async Task Timeout_AllTablesHaveUrls_ReturnsWithoutRetry()
    {
        // The "partial" job has URLs for every table, so there's nothing left to retry.
        var allDonePartialJob = MakeJob(completed: false,
            ("customers",    ["https://cdn.example.com/2024-01-01T00:00:00Z.ndjson.gz"]),
            ("appointments", ["https://cdn.example.com/2024-01-01T01:00:00Z.ndjson.gz"]));

        int createCalls = 0;

        var result = await ImportService.PollDownloadWithRetryAsync(
            ApiUrl, ApiKey, "job-001",
            destDir: Path.GetTempPath(),
            companyIds: null,
            locationIds: null,
            logger: _log,
            maxRetries: 5,
            pollFn: (_, _, _, _, _) =>
                throw new ExportTimeoutError("timed out", partialJob: allDonePartialJob),
            downloadFn: (job, _, _, _) =>
            {
                var infos = job.Tables.Keys
                    .Select(t => new TableExportInfo(t, DateTimeOffset.UtcNow))
                    .ToList();
                return Task.FromResult<IReadOnlyList<TableExportInfo>>(infos);
            },
            createJobFn: (_, _, _, _) => { createCalls++; return Task.FromResult("job-retry"); },
            delayFn: NoDelay);

        Assert.Equal(2, result.Count);
        Assert.Equal(0, createCalls); // no retry needed — everything already had URLs
    }

    // ── CompanyIds / LocationIds forwarded on retry ───────────────────────────

    [Fact]
    public async Task RetryJob_ForwardsCompanyIdsAndLocationIds()
    {
        var companyIds  = new List<string> { "co-1", "co-2" };
        var locationIds = new List<string> { "loc-1" };

        ExportJobCreateRequest? capturedRequest = null;

        var pendingJob = new ExportJobResponse
        {
            Completed = false,
            Tables    = new Dictionary<string, ExportJobTableData>
            {
                ["t1"] = new ExportJobTableData { Status = "InProgress", Urls = [] }
            }
        };

        var successJob = MakeJob(completed: true, ("t1", []));

        int pollAttempt = 0;

        await ImportService.PollDownloadWithRetryAsync(
            ApiUrl, ApiKey, "job-001",
            destDir: Path.GetTempPath(),
            companyIds: companyIds,
            locationIds: locationIds,
            logger: _log,
            maxRetries: 5,
            pollFn: (_, _, _, _, _) =>
            {
                pollAttempt++;
                if (pollAttempt == 1)
                    throw new ExportTimeoutError("timed out", partialJob: pendingJob);
                return Task.FromResult(successJob);
            },
            downloadFn: (_, _, _, _) => Task.FromResult<IReadOnlyList<TableExportInfo>>([]),
            createJobFn: (_, _, req, _) =>
            {
                capturedRequest = req;
                return Task.FromResult("job-retry-1");
            },
            delayFn: NoDelay);

        Assert.NotNull(capturedRequest);
        Assert.Equal(companyIds,  capturedRequest.CompanyIds);
        Assert.Equal(locationIds, capturedRequest.LocationIds);
    }

    // ── PollUntilCompleteAsync deadline override ──────────────────────────────

    [Fact]
    public async Task PollUntilComplete_DeadlineOverride_TimesOutImmediately()
    {
        // Set deadline to the past so the very first iteration exits.
        var pastDeadline = DateTimeOffset.UtcNow.AddSeconds(-1);

        await Assert.ThrowsAsync<ExportTimeoutError>(() =>
            ImportService.PollUntilCompleteAsync(
                ApiUrl, ApiKey, "job-001", _log,
                deadlineOverride: pastDeadline,
                checkJobFn: (_, _, _, _) =>
                    Task.FromResult(MakeJob(completed: false, ("t1", [])))));
    }

    // ── PollUntilCompleteAsync checkJobFn injection ───────────────────────────

    [Fact]
    public async Task PollUntilComplete_CheckJobFn_IsInvokedInsteadOfDefault()
    {
        int checkCalls = 0;
        var completeJob = MakeJob(completed: true, ("t1", []));

        var result = await ImportService.PollUntilCompleteAsync(
            ApiUrl, ApiKey, "job-001", _log,
            checkJobFn: (_, _, _, _) =>
            {
                checkCalls++;
                return Task.FromResult(completeJob);
            });

        Assert.Equal(completeJob, result);
        Assert.Equal(1, checkCalls);
    }
}
