using EDS.Core.Abstractions;
using EDS.Core.Models;
using EDS.Importer;
using EDS.Infrastructure.Tracking;
using EDS.Integration.Tests.Helpers;
using System.IO.Compression;
using System.Text.Json;

namespace EDS.Integration.Tests.Importers;

public sealed class NdjsonGzImporterTests : IAsyncLifetime
{
    private string _tempDir = string.Empty;

    public Task InitializeAsync()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"eds-importer-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
        return Task.CompletedTask;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static Schema OrdersSchema() => new()
    {
        Table        = "orders",
        ModelVersion = "1",
        PrimaryKeys  = ["id"],
        Required     = ["id"],
        Properties   = new()
        {
            ["id"]   = new() { Type = "string", Nullable = false },
            ["name"] = new() { Type = "string", Nullable = true },
        }
    };

    private static string WriteTempNdjson(string dir, string table, IEnumerable<object> rows)
    {
        var filename = $"202403151430221234567890000000000-n1-0-{table}-0.ndjson";
        var path = Path.Combine(dir, filename);
        File.WriteAllLines(path, rows.Select(r => JsonSerializer.Serialize(r)));
        return path;
    }

    private NdjsonGzImporter MakeImporter(
        string? dataDir = null,
        Schema? schema = null,
        bool dryRun = false,
        bool schemaOnly = false,
        bool noDelete = false,
        IReadOnlyList<string>? tables = null,
        IReadOnlySet<string>? completedFiles = null,
        ITracker? tracker = null,
        string? checkpointKey = null)
    {
        var s = schema ?? OrdersSchema();
        var schemaMap = DriverTestHelpers.ToSchemaMap(s);
        var registry  = new FakeSchemaRegistry(schemaMap);

        return new NdjsonGzImporter(new ImporterConfig
        {
            DataDir        = dataDir ?? _tempDir,
            SchemaRegistry = registry,
            DryRun         = dryRun,
            SchemaOnly     = schemaOnly,
            NoDelete       = noDelete,
            Tables         = tables ?? [],
            CompletedFiles = completedFiles ?? new HashSet<string>(),
            Tracker        = tracker,
            CheckpointKey  = checkpointKey,
        });
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task RunAsync_SingleFile_ImportsAllRows()
    {
        var rows = new object[]
        {
            new { id = "1", name = "Alice" },
            new { id = "2", name = "Bob" },
            new { id = "3", name = "Carol" },
        };
        WriteTempNdjson(_tempDir, "orders", rows);

        var handler  = new FakeImportHandler();
        var importer = MakeImporter();

        await importer.RunAsync(handler);

        Assert.Equal(3, handler.Imported.Count);
    }

    [Fact]
    public async Task RunAsync_CallsImportCompleted_WithRowCount()
    {
        var rows = new object[]
        {
            new { id = "1", name = "Alpha" },
            new { id = "2", name = "Beta" },
        };
        WriteTempNdjson(_tempDir, "orders", rows);

        var handler  = new FakeImportHandler();
        var importer = MakeImporter();

        await importer.RunAsync(handler);

        Assert.Single(handler.Completed);
        Assert.Equal("orders", handler.Completed[0].table);
        Assert.Equal(2L,       handler.Completed[0].rows);
    }

    [Fact]
    public async Task RunAsync_CreatesTablesBeforeImporting_WhenNotNoDelete()
    {
        WriteTempNdjson(_tempDir, "orders", [new { id = "1", name = "X" }]);

        var handler  = new FakeImportHandler();
        var importer = MakeImporter(noDelete: false);

        await importer.RunAsync(handler);

        Assert.Contains("orders", handler.CreatedTables);
    }

    [Fact]
    public async Task RunAsync_SkipsTableCreation_WhenNoDeleteIsTrue()
    {
        WriteTempNdjson(_tempDir, "orders", [new { id = "1", name = "X" }]);

        var handler  = new FakeImportHandler();
        var importer = MakeImporter(noDelete: true);

        await importer.RunAsync(handler);

        Assert.Empty(handler.CreatedTables);
    }

    [Fact]
    public async Task RunAsync_SchemaOnly_NoRowsImported()
    {
        WriteTempNdjson(_tempDir, "orders", [new { id = "1", name = "X" }]);

        var handler  = new FakeImportHandler();
        var importer = MakeImporter(schemaOnly: true, noDelete: false);

        await importer.RunAsync(handler);

        Assert.Empty(handler.Imported);
        Assert.Contains("orders", handler.CreatedTables);
    }

    [Fact]
    public async Task RunAsync_DryRun_NoImportEventCalls()
    {
        WriteTempNdjson(_tempDir, "orders",
        [
            new { id = "1", name = "Dry1" },
            new { id = "2", name = "Dry2" },
        ]);

        var handler  = new FakeImportHandler();
        var importer = MakeImporter(dryRun: true, noDelete: false);

        await importer.RunAsync(handler);

        Assert.Empty(handler.Imported);
        Assert.Contains("orders", handler.CreatedTables);
    }

    [Fact]
    public async Task RunAsync_SkipsFilesInCompletedFiles_Set()
    {
        WriteTempNdjson(_tempDir, "orders",
        [
            new { id = "1", name = "Already" },
            new { id = "2", name = "Done" },
        ]);

        var alreadyDone = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "202403151430221234567890000000000-n1-0-orders-0.ndjson"
        };

        var handler  = new FakeImportHandler();
        var importer = MakeImporter(completedFiles: alreadyDone);

        await importer.RunAsync(handler);

        Assert.Empty(handler.Imported);
    }

    [Fact]
    public async Task RunAsync_MultipleFilesForSameTable_AllImported()
    {
        // Write two files for the same table with distinct timestamps
        var file1 = Path.Combine(_tempDir, "202403151430221234567890000000000-n1-0-orders-0.ndjson");
        var file2 = Path.Combine(_tempDir, "202403151430231234567890000000000-n1-0-orders-1.ndjson");

        File.WriteAllLines(file1, [
            JsonSerializer.Serialize(new { id = "1", name = "Row1" }),
            JsonSerializer.Serialize(new { id = "2", name = "Row2" }),
        ]);
        File.WriteAllLines(file2, [
            JsonSerializer.Serialize(new { id = "3", name = "Row3" }),
        ]);

        var handler  = new FakeImportHandler();
        var importer = MakeImporter();

        await importer.RunAsync(handler);

        Assert.Equal(3, handler.Imported.Count);
    }

    [Fact]
    public async Task RunAsync_SkipsFilesForUnknownTables()
    {
        // Write a file whose table name is not in the schema map
        var filename = "202403151430221234567890000000000-n1-0-unknown_table-0.ndjson";
        var path     = Path.Combine(_tempDir, filename);
        File.WriteAllLines(path, [JsonSerializer.Serialize(new { id = "1" })]);

        var handler  = new FakeImportHandler();
        var importer = MakeImporter(); // schema only has "orders"

        // Should not throw
        await importer.RunAsync(handler);

        Assert.Empty(handler.Imported);
    }

    [Fact]
    public async Task RunAsync_TablesFilter_OnlyImportsRequestedTables()
    {
        // Write files for two tables
        var ordersFile   = "202403151430221234567890000000000-n1-0-orders-0.ndjson";
        var productsFile = "202403151430221234567890000000000-n1-0-products-0.ndjson";

        File.WriteAllLines(Path.Combine(_tempDir, ordersFile), [
            JsonSerializer.Serialize(new { id = "1", name = "Order" }),
        ]);
        File.WriteAllLines(Path.Combine(_tempDir, productsFile), [
            JsonSerializer.Serialize(new { id = "p1", name = "Product" }),
        ]);

        var handler  = new FakeImportHandler();
        var importer = MakeImporter(tables: ["orders"]);

        await importer.RunAsync(handler);

        Assert.Single(handler.Imported);
        Assert.All(handler.Imported, item => Assert.Equal("orders", item.evt.Table));
    }

    [Fact]
    public async Task RunAsync_EmptyFile_ImportCompletedCalledWithZeroRows()
    {
        // Write an empty file (no lines)
        var filename = "202403151430221234567890000000000-n1-0-orders-0.ndjson";
        File.WriteAllText(Path.Combine(_tempDir, filename), string.Empty);

        var handler  = new FakeImportHandler();
        var importer = MakeImporter();

        await importer.RunAsync(handler);

        Assert.Single(handler.Completed);
        Assert.Equal(0L, handler.Completed[0].rows);
        Assert.Empty(handler.Imported);
    }

    [Fact]
    public async Task RunAsync_CheckpointUpdated_AfterEachFile()
    {
        var trackerPath = Path.Combine(_tempDir, "tracker.db");
        await using var tracker = new SqliteTracker(trackerPath);
        const string checkpointKey = "import/orders/checkpoint";

        WriteTempNdjson(_tempDir, "orders",
        [
            new { id = "1", name = "CP1" },
            new { id = "2", name = "CP2" },
        ]);

        var handler  = new FakeImportHandler();
        var importer = MakeImporter(tracker: tracker, checkpointKey: checkpointKey);

        await importer.RunAsync(handler);

        var json = await tracker.GetKeyAsync(checkpointKey);
        Assert.NotNull(json);

        var completed = JsonSerializer.Deserialize<HashSet<string>>(json!);
        Assert.NotNull(completed);
        Assert.Contains("202403151430221234567890000000000-n1-0-orders-0.ndjson", completed!);
    }

    [Fact]
    public async Task RunAsync_GzippedFile_IsDecompressedAndImported()
    {
        var filename = "202403151430221234567890000000000-n1-0-orders-0.ndjson.gz";
        var path     = Path.Combine(_tempDir, filename);

        await using (var fs = File.Create(path))
        await using (var gz = new GZipStream(fs, CompressionMode.Compress))
        await using (var sw = new StreamWriter(gz))
        {
            await sw.WriteLineAsync(JsonSerializer.Serialize(new { id = "1", name = "GzTest" }));
            await sw.WriteLineAsync(JsonSerializer.Serialize(new { id = "2", name = "GzTest2" }));
        }

        var handler  = new FakeImportHandler();
        var importer = MakeImporter();

        await importer.RunAsync(handler);

        Assert.Equal(2, handler.Imported.Count);
        Assert.All(handler.Imported, item => Assert.Equal("orders", item.evt.Table));
    }
}

// ── Fake handler ─────────────────────────────────────────────────────────────

file sealed class FakeImportHandler : IImportHandler
{
    public List<(DbChangeEvent evt, Schema schema)> Imported = new();
    public List<string> CreatedTables = new();
    public List<(string table, long rows)> Completed = new();

    public Task CreateDatasourceAsync(Schema schema, CancellationToken ct = default)
    {
        CreatedTables.Add(schema.Table);
        return Task.CompletedTask;
    }

    public Task ImportEventAsync(DbChangeEvent evt, Schema schema, CancellationToken ct = default)
    {
        Imported.Add((evt, schema));
        return Task.CompletedTask;
    }

    public Task ImportCompletedAsync(string table, long rowCount, CancellationToken ct = default)
    {
        Completed.Add((table, rowCount));
        return Task.CompletedTask;
    }
}
