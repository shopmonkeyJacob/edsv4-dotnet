using System.Text.Json.Serialization;

namespace EDS.Infrastructure.Metrics;

/// <summary>
/// Thread-safe singleton that accumulates runtime state for the /status endpoint.
/// Properties are set by Program.cs at startup; counters are updated by
/// NatsConsumerService as events flow through.
/// </summary>
public sealed class StatusProvider
{
    private readonly DateTime _startedAt = DateTime.UtcNow;
    private long _eventsProcessed;
    private volatile bool _paused;
    private volatile bool _natsConnected;
    private volatile bool _consumerRunning;
    private volatile int _pendingFlush;
    private readonly object _lastEventLock = new();
    private DateTimeOffset? _lastEventAt;
    private string _lastEventTable = "";

    /// <summary>Set once at startup from EdsVersion.Current.</summary>
    public string Version { get; set; } = "";

    /// <summary>Set once at startup from the established session ID.</summary>
    public string SessionId { get; set; } = "";

    /// <summary>
    /// Set once at startup. Credentials are stripped — only scheme://host:port/path
    /// is exposed so no secrets leak through the status endpoint.
    /// </summary>
    public string Driver { get; set; } = "";

    // ── Updates from NatsConsumerService ─────────────────────────────────────

    public void SetNatsConnected(bool connected) => _natsConnected = connected;
    public void SetConsumerRunning(bool running) => _consumerRunning = running;
    public void SetPaused(bool paused) => _paused = paused;

    public void SetPendingFlush(int count) => Interlocked.Exchange(ref _pendingFlush, count);

    public void RecordLastEvent(string table)
    {
        lock (_lastEventLock)
        {
            _lastEventAt    = DateTimeOffset.UtcNow;
            _lastEventTable = table;
        }
    }

    public void RecordFlush(long count) =>
        Interlocked.Add(ref _eventsProcessed, count);

    // ── Health probes ─────────────────────────────────────────────────────────

    public (int StatusCode, HealthSnapshot Snapshot) GetLiveness()
    {
        bool running = _consumerRunning;
        return (running ? 200 : 503, new HealthSnapshot
        {
            Status = running ? "ok" : "unavailable",
            Checks = new() { ["consumer"] = running ? "ok" : "stopped" },
        });
    }

    public (int StatusCode, HealthSnapshot Snapshot) GetReadiness()
    {
        bool connected = _natsConnected;
        bool running   = _consumerRunning;
        bool paused    = _paused;
        bool ok        = connected && running;
        return (ok ? 200 : 503, new HealthSnapshot
        {
            Status = ok ? "ok" : "unavailable",
            Checks = new()
            {
                ["nats"]     = connected ? "ok" : "disconnected",
                ["consumer"] = running ? (paused ? "paused" : "ok") : "stopped",
            },
        });
    }

    // ── Snapshot ──────────────────────────────────────────────────────────────

    public StatusSnapshot GetSnapshot()
    {
        DateTimeOffset? lastAt;
        string lastTable;
        lock (_lastEventLock)
        {
            lastAt    = _lastEventAt;
            lastTable = _lastEventTable;
        }

        return new StatusSnapshot
        {
            Version         = Version,
            SessionId       = SessionId,
            Driver          = Driver,
            UptimeSeconds   = (long)(DateTime.UtcNow - _startedAt).TotalSeconds,
            Paused          = _paused,
            LastEventAt     = lastAt,
            LastEventTable  = lastTable,
            EventsProcessed = Interlocked.Read(ref _eventsProcessed),
            PendingFlush    = _pendingFlush,
        };
    }

    /// <summary>
    /// Strips user:password from a driver URL so credentials are never exposed
    /// through the status endpoint. Returns the scheme://host:port/path form.
    /// </summary>
    public static string SanitizeUrl(string url)
    {
        if (string.IsNullOrEmpty(url)) return url;
        if (!Uri.TryCreate(url, UriKind.Absolute, out var uri)) return url;
        var b = new UriBuilder(uri) { UserName = string.Empty, Password = string.Empty };
        return b.Uri.ToString().TrimEnd('/');
    }
}

/// <summary>JSON-serialisable response returned by GET /healthz/live and GET /healthz/ready.</summary>
public sealed class HealthSnapshot
{
    [JsonPropertyName("status")]  public string                      Status { get; init; } = "";
    [JsonPropertyName("checks")]  public Dictionary<string, string>  Checks { get; init; } = new();
}

/// <summary>JSON-serialisable snapshot returned by GET /status.</summary>
public sealed class StatusSnapshot
{
    [JsonPropertyName("version")]          public string          Version         { get; init; } = "";
    [JsonPropertyName("session_id")]       public string          SessionId       { get; init; } = "";
    [JsonPropertyName("driver")]           public string          Driver          { get; init; } = "";
    [JsonPropertyName("uptime_seconds")]   public long            UptimeSeconds   { get; init; }
    [JsonPropertyName("paused")]           public bool            Paused          { get; init; }
    [JsonPropertyName("last_event_at")]    public DateTimeOffset? LastEventAt     { get; init; }
    [JsonPropertyName("last_event_table")] public string          LastEventTable  { get; init; } = "";
    [JsonPropertyName("events_processed")] public long            EventsProcessed { get; init; }
    [JsonPropertyName("pending_flush")]    public int             PendingFlush    { get; init; }
}
