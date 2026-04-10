using EDS.Core.Abstractions;
using EDS.Core.Utilities;
using EDS.Importer;
using Microsoft.Extensions.Logging;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace EDS.Cli;

// ── API request / response types ─────────────────────────────────────────────

internal sealed class ExportJobCreateRequest
{
    [JsonPropertyName("tables")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IReadOnlyList<string>? Tables { get; init; }

    [JsonPropertyName("companyIds")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IReadOnlyList<string>? CompanyIds { get; init; }

    [JsonPropertyName("locationIds")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IReadOnlyList<string>? LocationIds { get; init; }

    [JsonPropertyName("timeOffset")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public long? TimeOffset { get; init; }
}

internal sealed class ExportJobTableData
{
    [JsonPropertyName("error")]  public string              Error  { get; init; } = "";
    [JsonPropertyName("status")] public string              Status { get; init; } = "";
    [JsonPropertyName("urls")]   public List<string>        Urls   { get; init; } = [];
    [JsonPropertyName("cursor")] public string              Cursor { get; init; } = "";
}

internal sealed class ExportJobResponse
{
    [JsonPropertyName("completed")] public bool                                   Completed { get; init; }
    [JsonPropertyName("tables")]    public Dictionary<string, ExportJobTableData> Tables    { get; init; } = [];

    public string ProgressString()
    {
        int done = 0;
        foreach (var d in Tables.Values) if (d.Status == "Completed") done++;
        var pct = Tables.Count > 0 ? 100.0 * done / Tables.Count : 0;
        return $"{done}/{Tables.Count} ({pct:F1}%)";
    }
}

internal sealed record TableExportInfo(string Table, DateTimeOffset Timestamp);

// ── ImportCheckpoint ──────────────────────────────────────────────────────────

/// <summary>
/// Persisted snapshot of an in-progress (or interrupted) import run.
/// Stored in the tracker so <c>eds import --resume</c> can continue without
/// re-downloading or re-processing already-finished files.
/// </summary>
internal sealed class ImportCheckpoint
{
    [JsonPropertyName("jobId")]          public string       JobId          { get; init; } = "";
    [JsonPropertyName("downloadDir")]    public string       DownloadDir    { get; init; } = "";
    [JsonPropertyName("completedFiles")] public List<string> CompletedFiles { get; init; } = [];
    [JsonPropertyName("startedAt")]      public DateTimeOffset StartedAt    { get; init; } = DateTimeOffset.UtcNow;
}

// ── ImportService ─────────────────────────────────────────────────────────────

internal static class ImportService
{
    private const string TableExportTrackerKey  = "import:table-export";
    internal const string CheckpointTrackerKey  = "import:checkpoint";

    /// <summary>
    /// Separate key used by <see cref="EDS.Importer.NdjsonGzImporter"/> to persist the
    /// set of already-completed filenames as a JSON array. Kept distinct from
    /// <see cref="CheckpointTrackerKey"/> so that per-file progress updates do not
    /// overwrite the full <see cref="ImportCheckpoint"/> object.
    /// </summary>
    internal const string CompletedFilesTrackerKey = "import:completed-files";

    // ── Export job ────────────────────────────────────────────────────────────

    public static async Task<string> CreateExportJobAsync(
        string apiUrl, string apiKey,
        ExportJobCreateRequest request,
        CancellationToken ct = default)
    {
        using var http = MakeClient(apiKey);
        var body = new StringContent(
            JsonSerializer.Serialize(request), Encoding.UTF8, "application/json");

        var resp = await RetryHelper.ExecuteAsync(
            innerCt => http.PostAsync($"{apiUrl}/v3/export/bulk", body, innerCt),
            operationName: "create export job", ct: ct);
        await ThrowIfErrorAsync(resp, "creating export job", ct);

        using var doc  = JsonDocument.Parse(await resp.Content.ReadAsStringAsync(ct));
        var root = doc.RootElement;
        EnsureSuccess(root);
        return root.GetProperty("data").GetProperty("jobId").GetString()
               ?? throw new InvalidOperationException("No jobId in response.");
    }

    public static async Task<ExportJobResponse> PollUntilCompleteAsync(
        string apiUrl, string apiKey, string jobId,
        ILogger logger, CancellationToken ct = default)
    {
        var lastLogged = DateTimeOffset.MinValue;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            if (DateTimeOffset.UtcNow - lastLogged > TimeSpan.FromMinutes(1))
            {
                logger.LogInformation("[import] Checking export status ({JobId})", jobId);
                lastLogged = DateTimeOffset.UtcNow;
            }

            ExportJobResponse job;
            try
            {
                job = await CheckJobAsync(apiUrl, apiKey, jobId, ct);
            }
            catch (Exception ex) when (RetryHelper.IsTransient(ex) && !ct.IsCancellationRequested)
            {
                logger.LogWarning("[import] Transient error polling export status — will retry in 15s: {Error}", ex.Message);
                await Task.Delay(TimeSpan.FromSeconds(15), ct);
                lastLogged = DateTimeOffset.MinValue; // force log on next iteration
                continue;
            }

            foreach (var (table, d) in job.Tables)
                if (d.Status == "Failed")
                    throw new InvalidOperationException($"Export of table '{table}' failed: {d.Error}");

            logger.LogDebug("[import] Export status: {Status}", job.ProgressString());

            if (job.Completed)
            {
                logger.LogInformation("[import] Export complete: {Status}", job.ProgressString());
                return job;
            }

            await Task.Delay(TimeSpan.FromSeconds(5), ct);
        }
    }

    // ── Download ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Downloads all files from the completed export job into <paramref name="destDir"/>
    /// using up to 10 concurrent HTTP downloads. Returns one <see cref="TableExportInfo"/>
    /// per table describing the latest file timestamp (used for change-tracking).
    /// </summary>
    public static async Task<IReadOnlyList<TableExportInfo>> BulkDownloadAsync(
        ExportJobResponse job,
        string destDir,
        ILogger logger,
        CancellationToken ct = default)
    {
        var tableTs   = new Dictionary<string, DateTimeOffset>(StringComparer.Ordinal);
        var downloads = new List<Uri>();

        foreach (var (tableName, data) in job.Tables)
        {
            if (data.Urls.Count == 0)
            {
                // Empty table — derive a timestamp from the cursor (nanoseconds)
                var ts = long.TryParse(data.Cursor, out var ns)
                    ? DateTimeOffset.FromUnixTimeMilliseconds(ns / 1_000_000)
                    : DateTimeOffset.UtcNow;
                tableTs[tableName] = ts;
                continue;
            }

            var latestTs = DateTimeOffset.MinValue;
            foreach (var urlStr in data.Urls)
            {
                var uri = new Uri(urlStr);
                if (CrdbFileParser.TryParse(Path.GetFileName(uri.LocalPath), out _, out var ts)
                    && ts > latestTs)
                    latestTs = ts;
                downloads.Add(uri);
            }
            tableTs[tableName] = latestTs > DateTimeOffset.MinValue ? latestTs : DateTimeOffset.UtcNow;
        }

        if (downloads.Count > 0)
        {
            var started   = DateTimeOffset.UtcNow;
            long bytes    = 0;
            int  done     = 0;
            var  sem      = new SemaphoreSlim(10);

            await Parallel.ForEachAsync(downloads, ct, async (uri, innerCt) =>
            {
                await sem.WaitAsync(innerCt);
                try
                {
                    var dest = Path.GetFullPath(Path.Combine(destDir, Path.GetFileName(uri.LocalPath)));
                    if (!dest.StartsWith(Path.GetFullPath(destDir) + Path.DirectorySeparatorChar, StringComparison.Ordinal))
                        throw new InvalidOperationException($"Download destination escaped target directory: {uri}");
                    using var http = new HttpClient();
                    using var r = await RetryHelper.ExecuteAsync(
                        ct2 => http.GetAsync(uri, ct2),
                        logger, operationName: $"download {Path.GetFileName(uri.LocalPath)}", ct: innerCt);
                    r.EnsureSuccessStatusCode();
                    await using var f = File.Create(dest);
                    await r.Content.CopyToAsync(f, innerCt);
                    Interlocked.Add(ref bytes, new FileInfo(dest).Length);
                    var n = Interlocked.Increment(ref done);
                    logger.LogDebug("[import] Downloaded {N}/{Total}", n, downloads.Count);
                }
                finally { sem.Release(); }
            });

            logger.LogInformation("[import] Downloaded {Count} file(s) ({Bytes:N0} bytes) in {Elapsed}.",
                downloads.Count, Interlocked.Read(ref bytes), DateTimeOffset.UtcNow - started);
        }

        return tableTs.Select(kv => new TableExportInfo(kv.Key, kv.Value)).ToList();
    }

    // ── Table-export tracker ──────────────────────────────────────────────────

    public static async Task SaveTableExportInfoAsync(
        ITracker tracker, IReadOnlyList<TableExportInfo> info, CancellationToken ct) =>
        await tracker.SetKeyAsync(TableExportTrackerKey, JsonSerializer.Serialize(info), ct);

    public static async Task<IReadOnlyList<TableExportInfo>?> TryLoadTableExportInfoAsync(
        ITracker tracker, CancellationToken ct)
    {
        var json = await tracker.GetKeyAsync(TableExportTrackerKey, ct);
        return json is null ? null : JsonSerializer.Deserialize<List<TableExportInfo>>(json);
    }

    // ── Import checkpoint ─────────────────────────────────────────────────────

    public static async Task SaveCheckpointAsync(
        ITracker tracker, ImportCheckpoint checkpoint, CancellationToken ct) =>
        await tracker.SetKeyAsync(CheckpointTrackerKey,
            JsonSerializer.Serialize(checkpoint), ct);

    public static async Task<ImportCheckpoint?> TryLoadCheckpointAsync(
        ITracker tracker, CancellationToken ct)
    {
        var json = await tracker.GetKeyAsync(CheckpointTrackerKey, ct);
        if (json is null) return null;
        try
        {
            return JsonSerializer.Deserialize<ImportCheckpoint>(json);
        }
        catch (JsonException)
        {
            // Stale or incompatible value in the tracker (e.g. a bare JSON array written
            // by an older build). Treat as no checkpoint so the run starts fresh.
            await tracker.DeleteKeyAsync(CheckpointTrackerKey, ct);
            return null;
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static async Task<ExportJobResponse> CheckJobAsync(
        string apiUrl, string apiKey, string jobId, CancellationToken ct)
    {
        using var http = MakeClient(apiKey);
        var resp = await RetryHelper.ExecuteAsync(
            innerCt => http.GetAsync($"{apiUrl}/v3/export/bulk/{Uri.EscapeDataString(jobId)}", innerCt),
            operationName: "check export status", ct: ct);
        await ThrowIfErrorAsync(resp, "checking export status", ct);

        using var doc  = JsonDocument.Parse(await resp.Content.ReadAsStringAsync(ct));
        var root = doc.RootElement;
        EnsureSuccess(root);

        var data = root.GetProperty("data");
        return new ExportJobResponse
        {
            Completed = data.GetProperty("completed").GetBoolean(),
            Tables    = data.GetProperty("tables")
                .Deserialize<Dictionary<string, ExportJobTableData>>() ?? []
        };
    }

    private static void EnsureSuccess(JsonElement root)
    {
        if (!root.GetProperty("success").GetBoolean())
        {
            var msg = root.TryGetProperty("message", out var m) ? m.GetString() : null;
            throw new InvalidOperationException($"API error: {msg ?? "unknown"}");
        }
    }

    private static async Task ThrowIfErrorAsync(
        HttpResponseMessage resp, string context, CancellationToken ct)
    {
        if (resp.IsSuccessStatusCode) return;
        var body      = await resp.Content.ReadAsStringAsync(ct);
        var requestId = resp.Headers.TryGetValues("X-Request-ID", out var vals)
            ? vals.FirstOrDefault() : null;
        var tag = requestId is not null ? $" (requestId={requestId})" : "";
        throw new InvalidOperationException(
            $"Error {context}: HTTP {(int)resp.StatusCode}{tag} — {body}");
    }

    private static HttpClient MakeClient(string apiKey)
    {
        var http = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
        http.DefaultRequestHeaders.Authorization =
            new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", apiKey);
        http.DefaultRequestHeaders.UserAgent.ParseAdd(
            $"Shopmonkey EDS Server/{EdsVersion.Current}");
        return http;
    }
}
