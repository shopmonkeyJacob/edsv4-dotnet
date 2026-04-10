using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using EDS.Core.Abstractions;
using EDS.Core.Helpers;
using EDS.Core.Models;
using Microsoft.Extensions.Logging;
using System.IO.Compression;
using System.Text;
using System.Text.Json;

namespace EDS.Drivers.EventHub;

/// <summary>
/// Publishes CDC events to Azure Event Hubs.
/// Partition key: {table}.{companyId}.{locationId}.{primaryKey}
/// Events with the same partition key are batched together.
/// </summary>
public sealed class EventHubDriver : IDriver, IDriverLifecycle, IDriverHelp, IDriverDirectImport
{
    private EventHubProducerClient? _client;
    private readonly List<DbChangeEvent> _pending = new();

    public string Name => "Azure EventHub";
    public string Description => "Streams EDS messages to Azure Event Hubs.";
    public string ExampleUrl => "eventhub://namespace.servicebus.windows.net/hub?shared-access-key-name=X&shared-access-key=Y";
    public string Help => "Events are batched by partition key (table.company.location.pk) to preserve ordering per entity.";

    public int MaxBatchSize => 500;

    public Task StartAsync(DriverConfig config, CancellationToken ct = default)
    {
        var uri = new Uri(config.Url);
        var qs = System.Web.HttpUtility.ParseQueryString(uri.Query);
        var eventHubName = uri.AbsolutePath.TrimStart('/');
        var ns = $"sb://{uri.Host}/";
        var keyName = qs["shared-access-key-name"] ?? throw new InvalidOperationException("Missing shared-access-key-name");
        var key = qs["shared-access-key"] ?? throw new InvalidOperationException("Missing shared-access-key");
        var connStr = $"Endpoint={ns};SharedAccessKeyName={keyName};SharedAccessKey={key}";
        _client = new EventHubProducerClient(connStr, eventHubName);
        return Task.CompletedTask;
    }

    public Task<bool> ProcessAsync(ILogger logger, DbChangeEvent evt, CancellationToken ct = default)
    {
        _pending.Add(evt);
        return Task.FromResult(_pending.Count >= MaxBatchSize);
    }

    public async Task FlushAsync(ILogger logger, CancellationToken ct = default)
    {
        if (_pending.Count == 0) return;

        // Group by partition key to preserve ordering
        var groups = _pending.GroupBy(e => e.GetPartitionKey());

        foreach (var group in groups)
        {
            var options = new CreateBatchOptions { PartitionKey = group.Key };
            using var batch = await _client!.CreateBatchAsync(options, ct);

            foreach (var evt in group)
            {
                var json = JsonSerializer.Serialize(evt);
                var data = new EventData(Encoding.UTF8.GetBytes(json))
                {
                    ContentType = "application/json",
                    MessageId = evt.Id
                };
                data.Properties["table"] = evt.Table;
                data.Properties["operation"] = evt.Operation;
                data.Properties["companyId"] = evt.CompanyId ?? string.Empty;

                if (!batch.TryAdd(data))
                {
                    // Batch full — send current batch and start a new one
                    await _client.SendAsync(batch, ct);
                    using var overflow = await _client.CreateBatchAsync(options, ct);
                    if (!overflow.TryAdd(data))
                        logger.LogError("[EventHub] Event for {Table}/{Id} exceeds max batch size and will be dropped.",
                            evt.Table, evt.Id);
                    else
                        await _client.SendAsync(overflow, ct);
                }
            }

            if (batch.Count > 0)
                await _client!.SendAsync(batch, ct);
        }

        logger.LogDebug("Flushed {Count} events to EventHub.", _pending.Count);
        _pending.Clear();
    }

    public async Task TestAsync(ILogger logger, string url, CancellationToken ct = default)
    {
        await StartAsync(new DriverConfig
        {
            Url = url, Logger = logger,
            SchemaRegistry = null!, Tracker = null!, DataDir = string.Empty
        }, ct);
        await _client!.GetEventHubPropertiesAsync(ct);
    }

    public async Task StopAsync(CancellationToken ct = default)
    {
        if (_client is not null)
        {
            await _client.DisposeAsync();
            _client = null;
        }
    }

    // ── IDriverDirectImport ───────────────────────────────────────────────────

    public Task InitForDirectImportAsync(ILogger logger, string url, CancellationToken ct = default)
    {
        return StartAsync(new DriverConfig
        {
            Url = url, Logger = logger,
            SchemaRegistry = null!, Tracker = null!, DataDir = string.Empty
        }, ct);
    }

    public async Task ImportFilesAsync(
        ILogger logger,
        IReadOnlyList<(string Table, string FilePath)> files,
        CancellationToken ct = default)
    {
        long totalRecords = 0;

        foreach (var (table, filePath) in files)
        {
            logger.LogInformation("[import] Publishing {File}", Path.GetFileName(filePath));
            long count = 0;

            await using var fs = File.OpenRead(filePath);
            Stream stream = filePath.EndsWith(".gz", StringComparison.OrdinalIgnoreCase)
                ? new GZipStream(fs, CompressionMode.Decompress)
                : fs;

            using var reader = new StreamReader(stream, Encoding.UTF8, bufferSize: 65536);
            string? line;
            while ((line = await reader.ReadLineAsync(ct)) is not null)
            {
                if (string.IsNullOrWhiteSpace(line)) continue;

                DbChangeEvent evt;
                try { evt = BuildImportEvent(line, table); }
                catch (Exception ex)
                {
                    logger.LogWarning("[import] Skipping invalid line in {File}: {Error}",
                        Path.GetFileName(filePath), ex.Message);
                    continue;
                }

                var shouldFlush = await ProcessAsync(logger, evt, ct);
                if (shouldFlush) await FlushAsync(logger, ct);
                count++;
            }

            if (_pending.Count > 0) await FlushAsync(logger, ct);
            logger.LogInformation("[import] {File}: {Count} record(s) published.", Path.GetFileName(filePath), count);
            totalRecords += count;
        }

        logger.LogInformation("[import] Published {Total} record(s) to EventHub.", totalRecords);
    }

    private static DbChangeEvent BuildImportEvent(string json, string table)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        var id = root.TryGetProperty("id", out var idEl) && idEl.ValueKind == JsonValueKind.String
            ? idEl.GetString() ?? Guid.NewGuid().ToString()
            : Guid.NewGuid().ToString();
        var companyId  = root.TryGetProperty("companyId",  out var compEl)  ? compEl.GetString()  : null;
        var locationId = root.TryGetProperty("locationId", out var locEl)   ? locEl.GetString()   : null;
        // Mirrors Go: LocationId uses locationId but falls back to companyId when locationId is absent.
        // This matches the partition-key routing behaviour in the Go source.
        var effectiveLocationId = companyId ?? locationId;
        return new DbChangeEvent
        {
            Operation  = "INSERT",
            Id         = id,
            Table      = table,
            Key        = [id],
            CompanyId  = companyId,
            LocationId = effectiveLocationId,
            After      = JsonSerializer.Deserialize<JsonElement>(json),
            Timestamp  = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Imported   = true,
        };
    }

    public IReadOnlyList<DriverField> Configuration() =>
    [
        DriverFieldHelpers.RequiredString("Namespace", "Azure EventHub namespace (e.g. mynamespace.servicebus.windows.net)"),
        DriverFieldHelpers.RequiredString("EventHubName", "Event Hub name"),
        DriverFieldHelpers.RequiredString("SharedAccessKeyName", "SAS key name"),
        DriverFieldHelpers.OptionalPassword("SharedAccessKey", "SAS key value")
    ];

    public (string url, IReadOnlyList<FieldError> errors) Validate(Dictionary<string, object?> values)
    {
        try
        {
            var ns = DriverFieldHelpers.GetRequiredString("Namespace", values);
            var hub = DriverFieldHelpers.GetRequiredString("EventHubName", values);
            var keyName = DriverFieldHelpers.GetRequiredString("SharedAccessKeyName", values);
            var key = DriverFieldHelpers.GetOptionalString("SharedAccessKey", string.Empty, values);
            return ($"eventhub://{ns}/{hub}?shared-access-key-name={Uri.EscapeDataString(keyName)}&shared-access-key={Uri.EscapeDataString(key)}", []);
        }
        catch (Exception ex)
        {
            return (string.Empty, [new FieldError { Field = "Namespace", Message = ex.Message }]);
        }
    }
}
