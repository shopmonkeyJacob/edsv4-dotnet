using EDS.Core.Abstractions;

namespace EDS.Infrastructure.Nats;

public sealed class ConsumerConfig
{
    /// <summary>NATS server URL.</summary>
    public required string Url { get; init; }

    /// <summary>Path to the NATS credentials (.creds) file.</summary>
    public required string CredentialsFile { get; init; }

    /// <summary>
    /// Configuration passed to <see cref="IDriverLifecycle.StartAsync"/> before the first event is processed.
    /// Required when the driver implements <see cref="IDriverLifecycle"/>.
    /// </summary>
    public DriverConfig? DriverConfig { get; init; }

    /// <summary>Company IDs to filter on. Empty = all companies.</summary>
    public IReadOnlyList<string> CompanyIds { get; init; } = [];

    /// <summary>Suffix appended to the durable consumer name.</summary>
    public string Suffix { get; init; } = string.Empty;

    /// <summary>Maximum in-flight (unacked) messages.</summary>
    public int MaxAckPending { get; init; } = 16384;

    /// <summary>Bounded channel capacity for the internal event buffer.</summary>
    public int MaxPendingBuffer { get; init; } = 16384;

    /// <summary>Minimum period to accumulate events before flushing.</summary>
    public TimeSpan MinPendingLatency { get; init; } = TimeSpan.FromSeconds(2);

    /// <summary>Maximum period before a flush is forced.</summary>
    public TimeSpan MaxPendingLatency { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Heartbeat interval sent back to Shopmonkey.</summary>
    public TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromMinutes(1);

    /// <summary>Pause when buffer is empty to avoid CPU spin.</summary>
    public TimeSpan EmptyBufferPauseTime { get; init; } = TimeSpan.FromMilliseconds(10);

    /// <summary>
    /// Per-table MVCC timestamps from a preceding import.
    /// Events with timestamps at or before these values are skipped.
    /// </summary>
    public Dictionary<string, DateTimeOffset> ExportTableTimestamps { get; init; } = new();

    /// <summary>If true, configure the consumer to start from the beginning of the stream.</summary>
    public bool DeliverAll { get; init; }

    public ISchemaValidator? SchemaValidator { get; init; }
    public ISchemaRegistry? Registry { get; init; }

    /// <summary>
    /// Dead-letter queue for events that cannot be flushed after all retry attempts.
    /// When set, permanently-failed events are persisted before the consumer stops.
    /// </summary>
    public IDlqWriter? Dlq { get; init; }
}
