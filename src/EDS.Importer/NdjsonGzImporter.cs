using EDS.Core.Abstractions;
using EDS.Core.Models;
using Microsoft.Extensions.Logging;
using System.IO.Compression;
using System.Text;
using System.Text.Json;

namespace EDS.Importer;

public sealed class ImporterConfig
{
    public required string DataDir { get; init; }
    public required ISchemaRegistry SchemaRegistry { get; init; }
    public ISchemaValidator? SchemaValidator { get; init; }
    public IReadOnlyList<string> Tables { get; init; } = [];
    public bool DryRun { get; init; }
    public bool Single { get; init; }
    public bool SchemaOnly { get; init; }
    public bool NoDelete { get; init; }
    public string? JobId { get; init; }
    public int MaxParallel { get; init; } = 4;
    public ILogger? Logger { get; init; }

    /// <summary>
    /// Tracker used to persist file-level progress. When set, each completed
    /// file is checkpointed so an interrupted import can be resumed.
    /// </summary>
    public ITracker? Tracker { get; init; }

    /// <summary>
    /// Key under which the checkpoint JSON is stored in <see cref="Tracker"/>.
    /// Must be provided whenever <see cref="Tracker"/> is non-null.
    /// </summary>
    public string? CheckpointKey { get; init; }

    /// <summary>
    /// Set of filenames (basename only) already processed in a previous run.
    /// These files are skipped and counted as done without touching the database.
    /// When non-empty, the table-recreation step is also skipped (resume mode).
    /// </summary>
    public IReadOnlySet<string> CompletedFiles { get; init; } = new HashSet<string>();
}

/// <summary>
/// Bulk importer for CRDB NDJSON.gz export files.
/// Mirrors the Go internal/importer/importer.go Run() logic.
/// </summary>
public sealed class NdjsonGzImporter
{
    private readonly ImporterConfig _config;
    private readonly ILogger _logger;

    public NdjsonGzImporter(ImporterConfig config)
    {
        _config = config;
        _logger = config.Logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
    }

    /// <summary>
    /// Runs the full import. Mirrors Go's importer.Run():
    ///   1. If not NoDelete and not resuming → drop+recreate ALL tables from schema
    ///   2. If SchemaOnly  → return after schema creation
    ///   3. Read NDJSON files, call ImportEventAsync per row, ImportCompletedAsync per table.
    ///      Already-completed files (from a prior interrupted run) are skipped.
    /// </summary>
    public async Task RunAsync(IImportHandler handler, CancellationToken ct = default)
    {
        var started   = DateTimeOffset.UtcNow;
        var schemaMap = await _config.SchemaRegistry.GetLatestSchemaAsync(ct);

        // ── Resumability: build the mutable completed-files set ───────────────
        // Pre-seeded from a prior checkpoint so already-finished files are skipped.
        var completedFiles = new HashSet<string>(_config.CompletedFiles, StringComparer.OrdinalIgnoreCase);
        bool isResume      = completedFiles.Count > 0;

        // ── Drop+recreate ALL tables before any file processing ───────────────
        // Skipped when resuming — tables already exist and are partially populated.
        if (!_config.NoDelete && !isResume)
        {
            // Drop tables that are in the DB but no longer in the HQ schema (orphans)
            // before recreating all current schema tables, so the DB ends up fully in sync.
            if (handler is IDriverImport importDriver)
            {
                var knownTables = new HashSet<string>(schemaMap.Keys, StringComparer.OrdinalIgnoreCase);
                await importDriver.DropOrphanTablesAsync(_logger, knownTables, ct);
            }

            _logger.LogInformation("[import] Dropping and recreating {Count} table(s)...", schemaMap.Count);
            foreach (var (_, schema) in schemaMap)
                await handler.CreateDatasourceAsync(schema, ct);
        }
        else if (isResume)
        {
            _logger.LogInformation("[import] Resuming — skipping table recreation ({N} file(s) already done).",
                completedFiles.Count);
            // Reconcile schema: the HQ schema may have gained columns since the tables
            // were originally created. Add any missing columns before importing rows so
            // that INSERT statements built from the latest schema don't hit "Unknown column".
            if (handler is IDriverMigration migration)
            {
                _logger.LogDebug("[import] Reconciling schema for {N} table(s)...", schemaMap.Count);
                foreach (var (_, schema) in schemaMap)
                    await migration.MigrateNewColumnsAsync(_logger, schema, schema.Columns().ToList(), ct);
            }
        }

        if (_config.SchemaOnly)
            return;

        // ── Discover files ────────────────────────────────────────────────────
        var files = DiscoverFiles(_config.DataDir, _config.Tables);
        _logger.LogInformation("[import] Found {Count} file(s) to process.", files.Count);

        long totalRows  = 0;
        int  totalFiles = 0;
        int  skipped    = 0;
        var  byTable    = files.GroupBy(f => f.Table).ToList();

        // The handler (IImportHandler / SqlDriverBase) holds mutable state (_pending buffer)
        // that is not thread-safe. Serialize handler access so concurrent table groups don't
        // corrupt the shared buffer, while still allowing MaxParallel table groups to overlap
        // their file I/O and decompression work.
        var handlerLock = new SemaphoreSlim(1, 1);
        var maxParallel = _config.MaxParallel > 0
            ? _config.MaxParallel
            : Environment.ProcessorCount;

        await Parallel.ForEachAsync(
            byTable,
            new ParallelOptions { MaxDegreeOfParallelism = maxParallel, CancellationToken = ct },
            async (tableGroup, innerCt) =>
        {
            var tableName = tableGroup.Key;
            if (!schemaMap.TryGetValue(tableName, out var schema))
            {
                _logger.LogWarning("[import] No schema for table '{Table}' — skipping.", tableName);
                return;
            }

            long rowCount     = 0;
            var  tableStarted = DateTimeOffset.UtcNow;

            foreach (var file in tableGroup.OrderBy(f => f.Timestamp))
            {
                var fileName = Path.GetFileName(file.Path);

                // Skip files finished in a previous (interrupted) run.
                if (completedFiles.Contains(fileName))
                {
                    _logger.LogDebug("[import] Skipping already-completed file: {File}", fileName);
                    Interlocked.Increment(ref skipped);
                    continue;
                }

                await handlerLock.WaitAsync(innerCt);
                try
                {
                    rowCount += await ProcessFileAsync(file.Path, file.Timestamp, schema, handler, innerCt);
                }
                finally
                {
                    handlerLock.Release();
                }
                Interlocked.Increment(ref totalFiles);

                // ── Checkpoint: persist this file as done ─────────────────────
                if (_config.Tracker is not null && _config.CheckpointKey is not null)
                {
                    lock (completedFiles)
                    {
                        completedFiles.Add(fileName);
                    }
                    await _config.Tracker.SetKeyAsync(
                        _config.CheckpointKey,
                        System.Text.Json.JsonSerializer.Serialize(completedFiles),
                        innerCt);
                }
            }

            await handlerLock.WaitAsync(innerCt);
            try
            {
                await handler.ImportCompletedAsync(tableName, rowCount, innerCt);
            }
            finally
            {
                handlerLock.Release();
            }
            Interlocked.Add(ref totalRows, rowCount);

            _logger.LogInformation("[import] {Table}: {Rows} row(s) in {Elapsed}.",
                tableName, rowCount, DateTimeOffset.UtcNow - tableStarted);
        });

        if (skipped > 0)
            _logger.LogInformation("[import] Skipped {Skipped} already-completed file(s).", skipped);

        _logger.LogInformation("[import] Done: {Rows} row(s) from {Files} file(s) in {Elapsed}.",
            totalRows, totalFiles, DateTimeOffset.UtcNow - started);
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private async Task<long> ProcessFileAsync(
        string filePath,
        DateTimeOffset fileTimestamp,
        Schema schema,
        IImportHandler handler,
        CancellationToken ct)
    {
        _logger.LogDebug("[import] Processing file: {File}", filePath);

        await using var fs = File.OpenRead(filePath);
        Stream stream = filePath.EndsWith(".gz", StringComparison.OrdinalIgnoreCase)
            ? new GZipStream(fs, CompressionMode.Decompress)
            : fs;

        long count = 0;
        using var reader = new StreamReader(stream, Encoding.UTF8, bufferSize: 65536);

        string? line;
        while ((line = await reader.ReadLineAsync(ct)) is not null)
        {
            if (string.IsNullOrWhiteSpace(line)) continue;

            DbChangeEvent evt;
            try
            {
                evt = ParseLine(line, schema, fileTimestamp);
            }
            catch (Exception ex)
            {
                _logger.LogWarning("[import] Failed to parse line in {File}: {Error}", filePath, ex.Message);
                continue;
            }

            if (_config.SchemaValidator is not null)
            {
                var result = _config.SchemaValidator.Validate(evt);
                if (result.SchemaFound && !result.IsValid)
                {
                    _logger.LogDebug("[import] Validation failed for {Table}/{Id} — skipping.", evt.Table, evt.Id);
                    continue;
                }
            }

            if (!_config.DryRun)
                await handler.ImportEventAsync(evt, schema, ct);

            count++;
        }

        return count;
    }

    /// <summary>
    /// Converts one NDJSON line into a synthetic INSERT DbChangeEvent.
    /// Mirrors Go: event.Operation = "INSERT", extract locationId/companyId/userId,
    /// set Timestamp from file timestamp (not wall clock).
    /// </summary>
    private static DbChangeEvent ParseLine(string line, Schema schema, DateTimeOffset fileTimestamp)
    {
        using var doc = JsonDocument.Parse(line);
        var root = doc.RootElement;

        var id = root.TryGetProperty("id", out var idEl)
            ? idEl.GetString() ?? Guid.NewGuid().ToString()
            : Guid.NewGuid().ToString();

        // Mirror Go: extract optional context fields from the row payload
        string? locationId = root.TryGetProperty("locationId", out var locEl) ? locEl.GetString() : null;
        string? companyId  = root.TryGetProperty("companyId",  out var compEl) ? compEl.GetString() : null;
        string? userId     = root.TryGetProperty("userId",     out var userEl) ? userEl.GetString() : null;

        // Go sets LocationId from locationId, then overwrites it with companyId if present
        // (mirrors the Go source behaviour, which appears intentional for routing)
        var effectiveLocationId = companyId ?? locationId;

        // Convert file timestamp to milliseconds (Timestamp) and nanoseconds string (MvccTimestamp)
        const long UnixEpochTicks = 621355968000000000L;
        var timestampMs   = fileTimestamp.ToUnixTimeMilliseconds();
        var timestampNs   = (fileTimestamp.UtcTicks - UnixEpochTicks) * 100L;

        return new DbChangeEvent
        {
            Operation     = "INSERT",
            Id            = id,
            Table         = schema.Table,
            Key           = [id],
            ModelVersion  = schema.ModelVersion,
            LocationId    = effectiveLocationId,
            CompanyId     = companyId,
            UserId        = userId,
            After         = JsonSerializer.Deserialize<JsonElement>(line),
            Timestamp     = timestampMs,
            MvccTimestamp = timestampNs.ToString(),
            Imported      = true
        };
    }

    /// <summary>
    /// Discovers all NDJSON[.gz] files under <paramref name="dataDir"/> whose table name
    /// is in <paramref name="tables"/> (or all tables when the list is empty).
    /// </summary>
    private static List<(string Path, string Table, DateTimeOffset Timestamp)> DiscoverFiles(
        string dataDir,
        IReadOnlyList<string> tables)
    {
        if (!Directory.Exists(dataDir)) return [];

        var result = new List<(string, string, DateTimeOffset)>();
        foreach (var file in Directory.EnumerateFiles(dataDir, "*.ndjson*", SearchOption.AllDirectories))
        {
            if (!CrdbFileParser.TryParse(Path.GetFileName(file), out var table, out var ts))
                continue;

            if (tables.Count > 0 && !tables.Contains(table, StringComparer.OrdinalIgnoreCase))
                continue;

            result.Add((file, table, ts));
        }
        return result;
    }
}
