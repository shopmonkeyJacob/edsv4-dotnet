using EDS.Infrastructure.Schema;
using EDS.Infrastructure.Tracking;
using Microsoft.Extensions.Logging.Abstractions;
using System.Text.Json;
using CoreSchema     = EDS.Core.Models.Schema;
using SchemaProperty = EDS.Core.Models.SchemaProperty;

namespace EDS.Infrastructure.Tests;

/// <summary>
/// Tests for <see cref="ApiSchemaRegistry"/> that exercise only observable
/// behaviour reachable without a live HTTP server.  All tests use a fresh
/// temporary SQLite database and seed the tracker cache directly so that
/// <c>CreateAsync</c> takes the warm-start path and never contacts the network.
/// </summary>
public class ApiSchemaRegistryTests : IAsyncLifetime
{
    private const string FakeApiUrl = "http://invalid.local";
    private const string EdsVersion = "0.0.0-test";

    // Cache keys that mirror the private constants in ApiSchemaRegistry.
    private const string AllSchemasCacheKey = "registry:all_schemas";
    private const string AllSchemasTsKey    = "registry:all_schemas:ts";

    private string        _dbPath  = string.Empty;
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

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>Builds a minimal Schema suitable for cache seeding.</summary>
    private static CoreSchema MakeSchema(string table, string modelVersion = "1") =>
        new()
        {
            Table        = table,
            ModelVersion = modelVersion,
            Properties   = new Dictionary<string, SchemaProperty>
            {
                ["id"] = new SchemaProperty { Type = "string" }
            },
            Required    = ["id"],
            PrimaryKeys = ["id"]
        };

    /// <summary>
    /// A non-empty sentinel schema used to satisfy the warm-start requirement
    /// that <c>map.Count &gt; 0</c>.  Tests that don't care about the schema
    /// content can call <see cref="SeedMinimalSchemasCache"/> instead.
    /// </summary>
    private static readonly Dictionary<string, CoreSchema> MinimalSchemaSeed = new()
    {
        ["_sentinel"] = MakeSchema("_sentinel", "0")
    };

    /// <summary>
    /// Seeds the tracker with a fresh all-schemas blob that contains at least
    /// one entry, so <c>LoadAllSchemasAsync</c> always takes the warm-start
    /// path and never makes an HTTP call.
    /// </summary>
    private Task SeedMinimalSchemasCache() =>
        SeedAllSchemasCache(MinimalSchemaSeed);

    /// <summary>
    /// Seeds the tracker with a fresh all-schemas blob whose content matches
    /// <paramref name="schemas"/>.  The caller must ensure at least one entry
    /// is present if a warm-start (no HTTP) is required.
    /// </summary>
    private async Task SeedAllSchemasCache(IReadOnlyDictionary<string, CoreSchema> schemas)
    {
        var blob = JsonSerializer.Serialize(schemas);
        await _tracker.SetKeyAsync(AllSchemasTsKey,    DateTimeOffset.UtcNow.ToString("O"));
        await _tracker.SetKeyAsync(AllSchemasCacheKey, blob);
    }

    private static string GetSchemaCacheKey(string table, string version) =>
        $"registry:{table}-{version}";

    private Task<ApiSchemaRegistry> CreateRegistryAsync(CancellationToken ct = default) =>
        ApiSchemaRegistry.CreateAsync(
            _tracker,
            NullLogger<ApiSchemaRegistry>.Instance,
            FakeApiUrl,
            EdsVersion,
            ct);

    // ── Warm-start / GetLatestSchemaAsync ─────────────────────────────────────

    [Fact]
    public async Task GetLatestSchema_WarmCache_ReturnsSchemaWithoutHttpCall()
    {
        var schemas = new Dictionary<string, CoreSchema>
        {
            ["orders"]   = MakeSchema("orders",   "2"),
            ["invoices"] = MakeSchema("invoices", "3")
        };
        await SeedAllSchemasCache(schemas);

        await using var registry = await CreateRegistryAsync();

        var latest = await registry.GetLatestSchemaAsync();

        Assert.Equal(2, latest.Count);
        Assert.True(latest.ContainsKey("orders"));
        Assert.True(latest.ContainsKey("invoices"));
    }

    // ── GetTableVersionAsync / SetTableVersionAsync ───────────────────────────

    [Fact]
    public async Task GetTableVersion_SetThenGet_RoundTrips()
    {
        await SeedMinimalSchemasCache();
        await using var registry = await CreateRegistryAsync();

        await registry.SetTableVersionAsync("orders", "v42");
        var (found, version) = await registry.GetTableVersionAsync("orders");

        Assert.True(found);
        Assert.Equal("v42", version);
    }

    [Fact]
    public async Task GetTableVersion_BeforeSet_ReturnsNotFound()
    {
        await SeedMinimalSchemasCache();
        await using var registry = await CreateRegistryAsync();

        var (found, version) = await registry.GetTableVersionAsync("unknown-table");

        Assert.False(found);
        Assert.Equal(string.Empty, version);
    }

    // ── Version persistence across registry reopen ────────────────────────────

    [Fact]
    public async Task SetTableVersion_PersistsAcrossReopen()
    {
        await SeedMinimalSchemasCache();
        await using (var registry = await CreateRegistryAsync())
        {
            await registry.SetTableVersionAsync("appointments", "v7");
        }

        // Re-seed the timestamp so the second open also takes the warm-start path.
        await SeedMinimalSchemasCache();
        await using var registry2 = await CreateRegistryAsync();

        var (found, version) = await registry2.GetTableVersionAsync("appointments");

        Assert.True(found);
        Assert.Equal("v7", version);
    }

    // ── GetSchemaAsync from persisted per-schema cache ────────────────────────

    [Fact]
    public async Task GetSchemaAsync_FromPersistedCache_ReturnsSchema()
    {
        var schema   = MakeSchema("workOrders", "5");
        var cacheKey = GetSchemaCacheKey("workOrders", "5");

        // Store the individual schema entry so GetSchemaAsync can find it.
        await _tracker.SetKeyAsync(cacheKey, JsonSerializer.Serialize(schema));

        // Warm-start requires at least one entry in the main blob; include the
        // schema there too so the registry is consistent with production state.
        await SeedAllSchemasCache(new Dictionary<string, CoreSchema>
        {
            ["workOrders"] = schema
        });

        await using var registry = await CreateRegistryAsync();

        // GetSchemaAsync first checks the in-memory cache (populated during warm-
        // start), then the tracker, then the API.  Either the in-memory or the
        // tracker path will return the schema without an HTTP call.
        var result = await registry.GetSchemaAsync("workOrders", "5");

        Assert.NotNull(result);
        Assert.Equal("workOrders", result!.Table);
        Assert.Equal("5",          result.ModelVersion);
    }

    // ── Stale cache ───────────────────────────────────────────────────────────

    [Fact]
    public async Task GetLatestSchema_StaleCache_ReturnsEmpty()
    {
        // Write a stale timestamp so the warm-start condition fails.
        var schemas = new Dictionary<string, CoreSchema>
        {
            ["orders"] = MakeSchema("orders", "1")
        };
        await _tracker.SetKeyAsync(AllSchemasTsKey,    DateTimeOffset.MinValue.ToString("O"));
        await _tracker.SetKeyAsync(AllSchemasCacheKey, JsonSerializer.Serialize(schemas));

        // With a stale cache the registry falls through to cold-start, which
        // attempts an HTTP fetch.  Cancel quickly so the test is not slow.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));

        // The fetch must fail (DNS / cancellation) — this confirms the stale
        // cache was not used, matching production behaviour.
        await Assert.ThrowsAnyAsync<Exception>(() => CreateRegistryAsync(cts.Token));
    }
}
