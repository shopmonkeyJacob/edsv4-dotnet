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
    ///   1. If not NoDelete → drop+recreate ALL tables from schema (not just those with files)
    ///   2. If SchemaOnly  → return after schema creation
    ///   3. Read NDJSON files, call ImportEventAsync per row, ImportCompletedAsync per table.
    /// </summary>
    public async Task RunAsync(IImportHandler handler, CancellationToken ct = default)
    {
        var started   = DateTimeOffset.UtcNow;
        var schemaMap = await _config.SchemaRegistry.GetLatestSchemaAsync(ct);

        // ── Drop+recreate ALL tables before any file processing ───────────────
        // Mirrors Go: handler.CreateDatasource(schema) is called once with the
        // full schema map before any rows are processed.
        if (!_config.NoDelete)
        {
            _logger.LogInformation("[import] Dropping and recreating {Count} table(s)...", schemaMap.Count);
            foreach (var (_, schema) in schemaMap)
                await handler.CreateDatasourceAsync(schema, ct);
        }

        if (_config.SchemaOnly)
            return;

        // ── Discover files ────────────────────────────────────────────────────
        var files = DiscoverFiles(_config.DataDir, _config.Tables);
        _logger.LogInformation("[import] Found {Count} file(s) to process.", files.Count);

        long totalRows  = 0;
        int  totalFiles = 0;
        var  byTable    = files.GroupBy(f => f.Table).ToList();

        foreach (var tableGroup in byTable)
        {
            var tableName = tableGroup.Key;
            if (!schemaMap.TryGetValue(tableName, out var schema))
            {
                _logger.LogWarning("[import] No schema for table '{Table}' — skipping.", tableName);
                continue;
            }

            long rowCount     = 0;
            var  tableStarted = DateTimeOffset.UtcNow;

            foreach (var file in tableGroup.OrderBy(f => f.Timestamp))
            {
                rowCount    += await ProcessFileAsync(file.Path, file.Timestamp, schema, handler, ct);
                totalFiles++;
            }

            await handler.ImportCompletedAsync(tableName, rowCount, ct);
            totalRows += rowCount;

            _logger.LogInformation("[import] {Table}: {Rows} row(s) in {Elapsed}.",
                tableName, rowCount, DateTimeOffset.UtcNow - tableStarted);
        }

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
