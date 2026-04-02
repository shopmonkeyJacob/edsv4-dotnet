using Confluent.Kafka;
using EDS.Core.Abstractions;
using EDS.Core.Helpers;
using EDS.Core.Models;
using Microsoft.Extensions.Logging;
using System.IO.Hashing;
using System.Text;
using System.Text.Json;

namespace EDS.Drivers.Kafka;

/// <summary>
/// Publishes CDC events to a Kafka topic.
/// Partition key: {table}.{companyId}.{locationId}.{primaryKey}
/// Message key: dbchange.{table}.{operation}.{companyId}.{locationId}.{id}
/// </summary>
public sealed class KafkaDriver : IDriver, IDriverLifecycle, IDriverHelp, IDriverAlias
{
    private IProducer<string, string>? _producer;
    private string _topic = "eds";
    private readonly List<(DbChangeEvent evt, Message<string, string> msg)> _pending = new();

    public IReadOnlyList<string> Aliases => ["kafka"];

    public string Name => "Kafka";
    public string Description => "Streams EDS messages to a Kafka topic.";
    public string ExampleUrl => "kafka://broker:9092/topic";
    public string Help => "Events are published with partition keys based on table+company+location to preserve ordering per entity.";

    public int MaxBatchSize => 1000;

    public Task StartAsync(DriverConfig config, CancellationToken ct = default)
    {
        var uri = new Uri(config.Url);
        var broker = $"{uri.Host}:{(uri.Port > 0 ? uri.Port : 9092)}";
        _topic = uri.AbsolutePath.TrimStart('/');
        if (string.IsNullOrEmpty(_topic)) _topic = "eds";

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = broker,
            Acks = Acks.All,
            EnableIdempotence = true,
            LingerMs = 5
        };

        // Parse optional auth params from query string
        var query = System.Web.HttpUtility.ParseQueryString(uri.Query);
        if (!string.IsNullOrEmpty(query["sasl.username"]))
        {
            producerConfig.SaslMechanism = SaslMechanism.Plain;
            producerConfig.SecurityProtocol = SecurityProtocol.SaslSsl;
            producerConfig.SaslUsername = query["sasl.username"];
            producerConfig.SaslPassword = query["sasl.password"];
        }

        _producer = new ProducerBuilder<string, string>(producerConfig).Build();
        return Task.CompletedTask;
    }

    public Task<bool> ProcessAsync(ILogger logger, DbChangeEvent evt, CancellationToken ct = default)
    {
        var partitionKey = evt.GetPartitionKey();
        var messageKey = $"dbchange.{evt.Table}.{evt.Operation}.{evt.CompanyId ?? "none"}.{evt.LocationId ?? "none"}.{evt.Id}";
        var value = JsonSerializer.Serialize(evt);

        _pending.Add((evt, new Message<string, string>
        {
            Key = messageKey,
            Value = value,
            Headers = new Headers
            {
                { "partition-key", Encoding.UTF8.GetBytes(partitionKey) }
            }
        }));

        return Task.FromResult(_pending.Count >= MaxBatchSize);
    }

    public async Task FlushAsync(ILogger logger, CancellationToken ct = default)
    {
        // Group by partition key for ordered delivery
        var groups = _pending
            .GroupBy(p => p.evt.GetPartitionKey())
            .ToList();

        foreach (var group in groups)
        {
            var partition = ComputePartition(group.Key);
            foreach (var (evt, msg) in group)
            {
                var result = await _producer!.ProduceAsync(
                    new TopicPartition(_topic, new Partition(partition)),
                    msg,
                    ct);
                if (result.Status != PersistenceStatus.Persisted)
                    logger.LogWarning("[Kafka] Delivery not confirmed for {Table}/{Id}: status={Status}",
                        evt.Table, evt.Id, result.Status);
            }
        }

        _producer!.Flush(ct);
        logger.LogDebug("Flushed {Count} messages to Kafka topic '{Topic}'.", _pending.Count, _topic);
        _pending.Clear();
    }

    public Task TestAsync(ILogger logger, string url, CancellationToken ct = default)
    {
        // AdminClient.GetMetadata is the standard Kafka "ping" — it does a live
        // broker round-trip and throws KafkaException if the broker is unreachable.
        return Task.Run(() =>
        {
            var uri    = new Uri(url);
            var broker = $"{uri.Host}:{(uri.Port > 0 ? uri.Port : 9092)}";
            var query  = System.Web.HttpUtility.ParseQueryString(uri.Query);

            var adminConfig = new AdminClientConfig { BootstrapServers = broker };
            if (!string.IsNullOrEmpty(query["sasl.username"]))
            {
                adminConfig.SaslMechanism    = SaslMechanism.Plain;
                adminConfig.SecurityProtocol = SecurityProtocol.SaslSsl;
                adminConfig.SaslUsername     = query["sasl.username"];
                adminConfig.SaslPassword     = query["sasl.password"];
            }

            using var adminClient = new AdminClientBuilder(adminConfig).Build();
            adminClient.GetMetadata(TimeSpan.FromSeconds(10));
        }, ct);
    }

    public Task StopAsync(CancellationToken ct = default)
    {
        _producer?.Flush(TimeSpan.FromSeconds(10));
        _producer?.Dispose();
        _producer = null;
        return Task.CompletedTask;
    }

    public IReadOnlyList<DriverField> Configuration() =>
    [
        DriverFieldHelpers.RequiredString("Broker", "Kafka broker address (host:port)"),
        DriverFieldHelpers.RequiredString("Topic", "Kafka topic name", "eds"),
        DriverFieldHelpers.OptionalString("SaslUsername", "SASL username (optional)"),
        DriverFieldHelpers.OptionalPassword("SaslPassword", "SASL password (optional)")
    ];

    public (string url, IReadOnlyList<FieldError> errors) Validate(Dictionary<string, object?> values)
    {
        try
        {
            var broker = DriverFieldHelpers.GetRequiredString("Broker", values);
            var topic = DriverFieldHelpers.GetOptionalString("Topic", "eds", values);
            return ($"kafka://{broker}/{topic}", []);
        }
        catch (Exception ex)
        {
            return (string.Empty, [new FieldError { Field = "Broker", Message = ex.Message }]);
        }
    }


    private static int ComputePartition(string key, int partitionCount = 64)
    {
        var bytes = Encoding.UTF8.GetBytes(key);
        var hash = XxHash32.HashToUInt32(bytes);
        return (int)(hash % (uint)partitionCount);
    }
}
