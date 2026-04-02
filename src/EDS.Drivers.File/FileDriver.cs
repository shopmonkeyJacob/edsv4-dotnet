using EDS.Core.Abstractions;
using EDS.Core.Helpers;
using EDS.Core.Models;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace EDS.Drivers.File;

/// <summary>
/// Writes CDC events as JSON files to a local directory.
/// Path pattern: {dir}/{table}/{timestamp}-{id}.json
/// Useful for bulk export and local testing.
/// </summary>
public sealed class FileDriver : IDriver, IDriverLifecycle, IDriverHelp
{
    private string? _directory;
    private readonly List<(DbChangeEvent evt, string path)> _pending = new();

    public string Name => "File";
    public string Description => "Streams EDS messages as JSON files to a local directory.";
    public string ExampleUrl => "file:///var/data/eds-output";
    public string Help => "Each event is written as a JSON file under {dir}/{table}/{timestamp}-{id}.json.";

    public int MaxBatchSize => -1;

    public Task StartAsync(DriverConfig config, CancellationToken ct = default)
    {
        var uri = new Uri(config.Url);
        _directory = uri.LocalPath;
        Directory.CreateDirectory(_directory);
        return Task.CompletedTask;
    }

    public Task<bool> ProcessAsync(ILogger logger, DbChangeEvent evt, CancellationToken ct = default)
    {
        var tableDir = Path.Combine(_directory!, evt.Table);
        Directory.CreateDirectory(tableDir);

        var filename = $"{evt.Timestamp}-{evt.Id}.json";
        var path = Path.Combine(tableDir, filename);
        _pending.Add((evt, path));
        return Task.FromResult(false);
    }

    public async Task FlushAsync(ILogger logger, CancellationToken ct = default)
    {
        foreach (var (evt, path) in _pending)
        {
            var json = JsonSerializer.SerializeToUtf8Bytes(evt, new JsonSerializerOptions { WriteIndented = false });
            await System.IO.File.WriteAllBytesAsync(path, json, ct);
        }
        logger.LogDebug("Flushed {Count} files.", _pending.Count);
        _pending.Clear();
    }

    public async Task TestAsync(ILogger logger, string url, CancellationToken ct = default)
    {
        var uri = new Uri(url);
        var dir = uri.LocalPath;
        Directory.CreateDirectory(dir);
        // Write and delete a test file to verify permissions
        var testFile = Path.Combine(dir, $".test-{Guid.NewGuid():N}");
        await System.IO.File.WriteAllTextAsync(testFile, "test", ct);
        System.IO.File.Delete(testFile);
    }

    public Task StopAsync(CancellationToken ct = default)
    {
        _pending.Clear();
        return Task.CompletedTask;
    }

    public IReadOnlyList<DriverField> Configuration() =>
    [
        DriverFieldHelpers.RequiredString("Directory", "The local directory path to write files to")
    ];

    public (string url, IReadOnlyList<FieldError> errors) Validate(Dictionary<string, object?> values)
    {
        try
        {
            var dir = DriverFieldHelpers.GetRequiredString("Directory", values);
            return ($"file://{dir}", []);
        }
        catch (Exception ex)
        {
            return (string.Empty, [new FieldError { Field = "Directory", Message = ex.Message }]);
        }
    }
}
