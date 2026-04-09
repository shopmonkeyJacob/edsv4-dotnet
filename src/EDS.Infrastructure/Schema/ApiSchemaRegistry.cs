using EDS.Core.Abstractions;
using EDS.Core.Models;
using EDS.Core.Utilities;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Text.Json;

using CoreSchema = EDS.Core.Models.Schema;

namespace EDS.Infrastructure.Schema;

/// <summary>
/// Schema registry backed by the Shopmonkey API.
/// On construction it fetches all schemas from <c>GET {apiUrl}/v3/schema</c> (mirroring Go's NewAPIRegistry).
/// Individual schema version fetches fall back to <c>GET {apiUrl}/v3/schema/{table}/{version}</c>.
/// Version tracking is persisted through the SQLite tracker.
/// </summary>
public sealed class ApiSchemaRegistry : ISchemaRegistry
{
    private const string TableVersionPrefix   = "registry:";
    private const string VersionCacheSuffix   = ":version";
    private const string AllSchemasCacheKey   = "registry:all_schemas";
    private const string AllSchemasTsKey      = "registry:all_schemas:ts";
    private static readonly TimeSpan CacheTtl = TimeSpan.FromHours(24);

    private readonly ITracker  _tracker;
    private readonly ILogger<ApiSchemaRegistry> _logger;
    private readonly string    _apiUrl;
    private readonly string    _userAgent;
    private readonly HttpClient _http;

    // In-memory caches (mirrors Go's util.Cache)
    private readonly ConcurrentDictionary<string, (CoreSchema schema, DateTimeOffset expires)> _schemaCache = new();
    private readonly ConcurrentDictionary<string, (string version, DateTimeOffset expires)> _versionCache = new();

    // Latest schema map fetched at startup
    private SchemaMap _latestSchemas = new();

    private ApiSchemaRegistry(ITracker tracker, ILogger<ApiSchemaRegistry> logger, string apiUrl, string edsVersion)
    {
        _tracker   = tracker;
        _logger    = logger;
        _apiUrl    = apiUrl.TrimEnd('/');
        _userAgent = $"Shopmonkey EDS Server/{edsVersion}";
        _http      = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
        _http.DefaultRequestHeaders.UserAgent.ParseAdd(_userAgent);
    }

    /// <summary>
    /// Creates and initialises a new <see cref="ApiSchemaRegistry"/> by fetching the full
    /// schema catalog from the API, mirroring Go's <c>registry.NewAPIRegistry()</c>.
    /// </summary>
    public static async Task<ApiSchemaRegistry> CreateAsync(
        ITracker tracker,
        ILogger<ApiSchemaRegistry> logger,
        string apiUrl,
        string edsVersion,
        CancellationToken ct = default)
    {
        var r = new ApiSchemaRegistry(tracker, logger, apiUrl, edsVersion);
        await r.LoadAllSchemasAsync(ct);
        return r;
    }

    // ── ISchemaRegistry ───────────────────────────────────────────────────────

    public Task<SchemaMap> GetLatestSchemaAsync(CancellationToken ct = default) =>
        Task.FromResult(_latestSchemas);

    /// <summary>
    /// Bypasses the 24-hour TTL and immediately re-fetches the full schema catalog from the
    /// Shopmonkey API. Call this at the start of an import to guarantee the local cache
    /// reflects the latest HQ schema before any table DDL is executed.
    /// </summary>
    public async Task ForceRefreshAsync(CancellationToken ct = default)
    {
        _logger.LogInformation("[schema] Force-refreshing schema catalog from HQ (bypassing cache)...");
        // Expire the timestamp so LoadAllSchemasAsync treats the next call as a cold start.
        await _tracker.SetKeyAsync(AllSchemasTsKey, DateTimeOffset.MinValue.ToString("O"), ct);
        _latestSchemas = new SchemaMap();
        _schemaCache.Clear();
        _versionCache.Clear();
        await LoadAllSchemasAsync(ct);
    }

    public async Task<CoreSchema?> GetSchemaAsync(string table, string version, CancellationToken ct = default)
    {
        var cacheKey = GetSchemaCacheKey(table, version);

        // 1. in-memory cache
        if (_schemaCache.TryGetValue(cacheKey, out var cached) && cached.expires > DateTimeOffset.UtcNow)
            return cached.schema;

        // 2. SQLite tracker
        var persisted = await _tracker.GetKeyAsync(cacheKey, ct);
        if (persisted is not null)
        {
            try
            {
                var fromTracker = JsonSerializer.Deserialize<CoreSchema>(persisted);
                if (fromTracker is not null)
                {
                    _schemaCache[cacheKey] = (fromTracker, DateTimeOffset.UtcNow.Add(CacheTtl));
                    return fromTracker;
                }
            }
            catch { /* fall through to API */ }
        }

        // 3. API fallback: GET {apiUrl}/v3/schema/{table}/{version}
        return await FetchSchemaFromApiAsync(table, version, cacheKey, ct);
    }

    public async Task<(bool found, string version)> GetTableVersionAsync(string table, CancellationToken ct = default)
    {
        var key = GetVersionCacheKey(table);

        if (_versionCache.TryGetValue(key, out var cached) && cached.expires > DateTimeOffset.UtcNow)
            return (true, cached.version);

        var persisted = await _tracker.GetKeyAsync(key, ct);
        if (persisted is not null)
        {
            _versionCache[key] = (persisted, DateTimeOffset.UtcNow.Add(CacheTtl));
            return (true, persisted);
        }
        return (false, string.Empty);
    }

    public async Task SetTableVersionAsync(string table, string version, CancellationToken ct = default)
    {
        var key = GetVersionCacheKey(table);
        _versionCache[key] = (version, DateTimeOffset.UtcNow.Add(CacheTtl));
        await _tracker.SetKeyAsync(key, version, ct);
        _logger.LogTrace("[schema] set table={Table} version={Version}", table, version);
    }

    public ValueTask DisposeAsync()
    {
        _http.Dispose();
        return ValueTask.CompletedTask;
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /// <summary>
    /// Loads the full schema catalog. On warm restarts (cache younger than 24 h) the schemas
    /// are read from the local SQLite tracker in a single read — no HTTP call is made.
    /// On cold starts the catalog is fetched from the API and all schemas are written to
    /// SQLite in a single batched transaction.
    /// </summary>
    private async Task LoadAllSchemasAsync(CancellationToken ct)
    {
        // ── Warm-start: load from SQLite when the cached catalog is still fresh ──
        var tsRaw = await _tracker.GetKeyAsync(AllSchemasTsKey, ct);
        if (tsRaw is not null
            && DateTimeOffset.TryParse(tsRaw, null, System.Globalization.DateTimeStyles.RoundtripKind, out var savedAt)
            && DateTimeOffset.UtcNow - savedAt < CacheTtl)
        {
            var blob = await _tracker.GetKeyAsync(AllSchemasCacheKey, ct);
            if (blob is not null)
            {
                var map = JsonSerializer.Deserialize<Dictionary<string, CoreSchema>>(blob);
                if (map is not null && map.Count > 0)
                {
                    _latestSchemas = new SchemaMap();
                    foreach (var (_, schema) in map)
                    {
                        _latestSchemas[schema.Table] = schema;
                        var ck = GetSchemaCacheKey(schema.Table, schema.ModelVersion);
                        _schemaCache[ck] = (schema, DateTimeOffset.UtcNow.Add(CacheTtl));
                    }
                    _logger.LogInformation(
                        "[schema] Loaded {Count} table schemas from local cache (age={Age}).",
                        _latestSchemas.Count,
                        (DateTimeOffset.UtcNow - savedAt).ToString(@"hh\:mm\:ss"));
                    return;
                }
            }
        }

        // ── Cold-start: fetch from API ────────────────────────────────────────
        _logger.LogInformation("[schema] Fetching full schema catalog from {ApiUrl}/v3/schema", _apiUrl);

        var resp = await RetryHelper.ExecuteAsync(
            innerCt => _http.GetAsync($"{_apiUrl}/v3/schema", innerCt),
            _logger, operationName: "fetch schema catalog", ct: ct);
        if (!resp.IsSuccessStatusCode)
        {
            var body = await resp.Content.ReadAsStringAsync(ct);
            throw new InvalidOperationException(
                $"Error fetching schema catalog: HTTP {(int)resp.StatusCode} — {body}");
        }

        var json = await resp.Content.ReadAsStringAsync(ct);
        var apiMap = JsonSerializer.Deserialize<Dictionary<string, CoreSchema>>(json)
            ?? throw new InvalidOperationException("Schema catalog response was null.");

        _latestSchemas = new SchemaMap();

        // Batch all writes (catalog blob + per-schema entries) into one SQLite transaction
        var toWrite = new Dictionary<string, string>
        {
            [AllSchemasCacheKey] = json,
            [AllSchemasTsKey]    = DateTimeOffset.UtcNow.ToString("O")
        };

        foreach (var (tableName, schema) in apiMap)
        {
            _latestSchemas[tableName] = schema;
            var cacheKey = GetSchemaCacheKey(schema.Table, schema.ModelVersion);
            _schemaCache[cacheKey] = (schema, DateTimeOffset.UtcNow.Add(CacheTtl));
            toWrite[cacheKey] = JsonSerializer.Serialize(schema);
        }

        try { await _tracker.SetKeysAsync(toWrite, ct); }
        catch (Exception ex) { _logger.LogWarning(ex, "[schema] Could not persist schema cache to SQLite."); }

        _logger.LogInformation("[schema] Loaded {Count} table schemas from API.", _latestSchemas.Count);
    }

    private async Task<CoreSchema?> FetchSchemaFromApiAsync(
        string table, string version, string cacheKey, CancellationToken ct)
    {
        _logger.LogDebug("[schema] Fetching schema for {Table} v{Version} from API", table, version);
        try
        {
            var resp = await RetryHelper.ExecuteAsync(
                innerCt => _http.GetAsync(
                    $"{_apiUrl}/v3/schema/{Uri.EscapeDataString(table)}/{Uri.EscapeDataString(version)}", innerCt),
                _logger, operationName: $"fetch schema {table} v{version}", ct: ct);
            if (!resp.IsSuccessStatusCode)
            {
                _logger.LogWarning("[schema] API returned {Status} for {Table} v{Version}",
                    (int)resp.StatusCode, table, version);
                return null;
            }

            var json   = await resp.Content.ReadAsStringAsync(ct);
            var schema = JsonSerializer.Deserialize<CoreSchema>(json);
            if (schema is null) return null;

            _schemaCache[cacheKey] = (schema, DateTimeOffset.UtcNow.Add(CacheTtl));
            try { await _tracker.SetKeyAsync(cacheKey, json, ct); } catch { }
            return schema;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[schema] Failed to fetch schema for {Table} v{Version}", table, version);
            return null;
        }
    }

    private static string GetSchemaCacheKey(string table, string version) =>
        $"{TableVersionPrefix}{table}-{version}";

    private static string GetVersionCacheKey(string table) =>
        $"{TableVersionPrefix}{table}{VersionCacheSuffix}";
}
