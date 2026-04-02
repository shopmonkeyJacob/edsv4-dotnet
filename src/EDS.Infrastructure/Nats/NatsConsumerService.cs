using EDS.Core.Abstractions;
using EDS.Core.Models;
using EDS.Infrastructure.Metrics;
using MessagePack;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using System.IO.Compression;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Linq;
using System.Threading.Channels;

namespace EDS.Infrastructure.Nats;

// Alias to avoid name collision with NATS.Client.JetStream.Models.ConsumerConfig
using NatsConsumerCfg = NATS.Client.JetStream.Models.ConsumerConfig;

internal sealed class CredentialInfo
{
    public List<string> CompanyIds { get; init; } = [];
    public string ServerID  { get; init; } = "";
    public string SessionID { get; init; } = "";
}

[MessagePackObject(AllowPrivate = true)]
internal sealed class HeartbeatPayload
{
    [Key("sessionId")] public string         SessionId { get; set; } = "";
    [Key("offset")]    public long           Offset    { get; set; }
    [Key("uptime")]    public long           Uptime    { get; set; }
    [Key("stats")]     public HeartbeatStats Stats     { get; set; } = new();
    [Key("paused")]    public bool           Paused    { get; set; }
}

[MessagePackObject(AllowPrivate = true)]
internal sealed class HeartbeatStats
{
    [Key("metrics")] public HeartbeatMetrics Metrics { get; set; } = new();
    [Key("memory")]  public HeartbeatMemory  Memory  { get; set; } = new();
}

[MessagePackObject(AllowPrivate = true)]
internal sealed class HeartbeatMetrics
{
    [Key("flushCount")]         public double FlushCount         { get; set; }
    [Key("flushDuration")]      public double FlushDuration      { get; set; }
    [Key("processingDuration")] public double ProcessingDuration { get; set; }
    [Key("pendingEvents")]      public double PendingEvents      { get; set; }
    [Key("totalEvents")]        public double TotalEvents        { get; set; }
}

[MessagePackObject(AllowPrivate = true)]
internal sealed class HeartbeatMemory
{
    [Key("total")]       public ulong  Total       { get; set; }
    [Key("available")]   public ulong  Available   { get; set; }
    [Key("used")]        public ulong  Used        { get; set; }
    [Key("usedPercent")] public double UsedPercent { get; set; }
}

/// <summary>
/// BackgroundService that consumes events from NATS JetStream and routes them to the driver.
/// Mirrors the Go consumer goroutine loop with min/max latency buffering and heartbeats.
/// </summary>
public sealed class NatsConsumerService : BackgroundService
{
    private readonly ConsumerConfig _config;
    private readonly IDriver _driver;
    private readonly ILogger<NatsConsumerService> _logger;
    private readonly DateTime _started = DateTime.UtcNow;
    private long _offset;
    private bool _paused;
    private NatsConnection? _nats;

    public NatsConsumerService(
        ConsumerConfig config,
        IDriver driver,
        ILogger<NatsConsumerService> logger)
    {
        _config = config;
        _driver = driver;
        _logger = logger;
    }

    /// <summary>
    /// Pauses or resumes message processing. When paused, incoming messages are
    /// NAKed immediately so they are redelivered once processing resumes.
    /// The paused state is reported in heartbeats so HQ can reflect it in the UI.
    /// </summary>
    public void SetPaused(bool paused)
    {
        _paused = paused;
        _logger.LogInformation("[consumer] {Action} by HQ notification.", paused ? "Paused" : "Resumed");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // ── Step 1: Parse credential file to get serverID, sessionID, companyIDs ──
        var credInfo = ParseCredentialInfo(_config.CredentialsFile);
        _logger.LogInformation(
            "[consumer] server={ServerId}, session={SessionId}, companies=[{Companies}]",
            credInfo.ServerID, credInfo.SessionID, string.Join(", ", credInfo.CompanyIds));

        var opts = new NatsOpts
        {
            Url              = _config.Url,
            ReconnectWaitMin = TimeSpan.FromSeconds(2),
            MaxReconnectRetry = -1,
            AuthOpts = new NatsAuthOpts
            {
                AuthCredCallback = (_, _) =>
                    ValueTask.FromResult(NatsAuthCred.FromCredsFile(_config.CredentialsFile))
            }
        };

        await using var nats = new NatsConnection(opts);
        _nats = nats;
        nats.ConnectionDisconnected += (_, _) =>
        {
            _logger.LogWarning("[consumer] NATS connection disconnected.");
            return ValueTask.CompletedTask;
        };

        await nats.ConnectAsync();
        _logger.LogInformation("[consumer] Connected to NATS at {Url}", _config.Url);

        // ── Step 2: Start heartbeat background task ───────────────────────────────
        using var heartbeatCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var heartbeatTask = Task.Run(
            () => SendHeartbeatsAsync(credInfo.SessionID, heartbeatCts.Token),
            CancellationToken.None);

        // ── Step 3: Create/update JetStream consumer ──────────────────────────────
        var js = new NatsJSContext(nats);
        var consumerName    = BuildConsumerName(credInfo);
        var filterSubjects  = BuildFilterSubjects(credInfo);

        _logger.LogInformation("[consumer] consumer={Name}, subjects=[{Subjects}]",
            consumerName, string.Join(", ", filterSubjects));

        var natsCfg = new NatsConsumerCfg(consumerName)
        {
            DeliverPolicy      = _config.DeliverAll
                                     ? ConsumerConfigDeliverPolicy.All
                                     : ConsumerConfigDeliverPolicy.New,
            AckPolicy          = ConsumerConfigAckPolicy.Explicit,
            MaxAckPending      = _config.MaxAckPending,
            MaxDeliver         = 20,
            AckWait            = TimeSpan.FromMinutes(5),
            InactiveThreshold  = TimeSpan.FromDays(3),
            FilterSubjects     = filterSubjects
        };

        // ── Steps 3 + 4 run concurrently: JetStream consumer setup and driver startup
        //    have no dependency on each other, so parallelise them to reduce time-to-first-message.
        var consumerTask = js.CreateOrUpdateConsumerAsync("dbchange", natsCfg, stoppingToken).AsTask();

        Task lifecycleTask = Task.CompletedTask;
        if (_driver is IDriverLifecycle lifecycle)
        {
            _logger.LogInformation("[consumer] Starting driver lifecycle.");
            lifecycleTask = lifecycle.StartAsync(_config.DriverConfig, stoppingToken);
        }

        await Task.WhenAll(consumerTask, lifecycleTask);

        var consumer = await consumerTask;
        _logger.LogInformation("[consumer] JetStream consumer '{Name}' ready.", consumerName);

        // ── Step 5: Run processing loop ───────────────────────────────────────────
        try
        {
            _logger.LogInformation("[consumer] Entering processing loop.");
            await RunProcessingLoopAsync(consumer, stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "[consumer] Processing loop exited with error — initiating shutdown.");
            _logger.LogCritical("[consumer] Fatal error in consumer fork. Session={SessionId} Uptime={Uptime}s.",
                credInfo.SessionID,
                (long)(DateTime.UtcNow - _started).TotalSeconds);
            Environment.ExitCode = 1;
            throw;
        }
        finally
        {
            heartbeatCts.Cancel();
            try { await heartbeatTask; } catch (OperationCanceledException) { }
            _logger.LogInformation("[consumer] Consumer fork stopped. Session={SessionId}.", credInfo.SessionID);
        }
    }

    // ── Heartbeat ─────────────────────────────────────────────────────────────

    private async Task SendHeartbeatsAsync(string sessionId, CancellationToken ct)
    {
        // Send immediately on start
        await PublishHeartbeatAsync(sessionId, ct);

        using var timer = new PeriodicTimer(_config.HeartbeatInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
                await PublishHeartbeatAsync(sessionId, ct);
        }
        catch (OperationCanceledException) { }
    }

    private async Task PublishHeartbeatAsync(string sessionId, CancellationToken ct)
    {
        if (_nats is null) return;
        try
        {
            var proc      = System.Diagnostics.Process.GetCurrentProcess();
            var gcInfo    = GC.GetGCMemoryInfo();
            var total     = (ulong)Math.Max(0, gcInfo.TotalAvailableMemoryBytes);
            var used      = (ulong)Math.Max(0, proc.WorkingSet64);
            var available = total > used ? total - used : 0UL;

            var hb = new HeartbeatPayload
            {
                SessionId = sessionId,
                Offset    = Interlocked.Increment(ref _offset),
                Uptime    = (long)(DateTime.UtcNow - _started).TotalSeconds,
                Paused    = _paused,
                Stats     = new HeartbeatStats
                {
                    Metrics = new HeartbeatMetrics
                    {
                        PendingEvents = EdsMetrics.PendingEvents.Value,
                        TotalEvents   = EdsMetrics.TotalEvents.Value,
                    },
                    Memory = new HeartbeatMemory
                    {
                        Total       = total,
                        Used        = used,
                        Available   = available,
                        UsedPercent = total > 0 ? (double)used / total * 100.0 : 0
                    }
                }
            };

            var data    = MessagePackSerializer.Serialize(hb);
            var headers = new NatsHeaders { ["content-encoding"] = "msgpack" };
            var subject = $"eds.client.{sessionId}.heartbeat";
            await _nats.PublishAsync(subject, data, headers: headers, cancellationToken: ct);

            var uptimeSpan = TimeSpan.FromSeconds(hb.Uptime);
            _logger.LogInformation("[heartbeat] alive — uptime={Uptime} offset={Offset}",
                uptimeSpan.ToString(@"d\.hh\:mm\:ss"), hb.Offset);
            _logger.LogDebug("[heartbeat] pending={Pending} total={Total} mem={MemMB}MB subject={Subject}",
                hb.Stats.Metrics.PendingEvents,
                hb.Stats.Metrics.TotalEvents,
                hb.Stats.Memory.Used / 1024 / 1024,
                subject);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "[consumer] Error sending heartbeat.");
        }
    }

    // ── Processing loop ───────────────────────────────────────────────────────

    private async Task RunProcessingLoopAsync(INatsJSConsumer consumer, CancellationToken ct)
    {
        var pending   = new List<(DbChangeEvent evt, INatsJSMsg<byte[]> msg)>();
        DateTimeOffset? pendingStarted = null;
        int batchSize = _driver.MaxBatchSize;

        // Channel decouples the continuous NATS consumer from our time-based flush logic.
        // Capacity of 4096 matches Go's PullMaxMessages(4096).
        var channel = Channel.CreateBounded<INatsJSMsg<byte[]>>(
            new BoundedChannelOptions(4096)
            {
                FullMode     = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = true
            });

        // Producer: forwards ConsumeAsync messages into the channel.
        // ConsumeAsync mirrors Go's nc.Consume() — a continuous pull consumer.
        var producerTask = Task.Run(async () =>
        {
            _logger.LogDebug("[consumer] Producer task started.");
            try
            {
                var consumeOpts = new NatsJSConsumeOpts
                {
                    MaxMsgs       = 4096,
                    IdleHeartbeat = TimeSpan.FromSeconds(5),
                    Expires       = TimeSpan.FromMinutes(1),
                };
                _logger.LogDebug("[consumer] Starting ConsumeAsync loop.");
                await foreach (var msg in consumer.ConsumeAsync<byte[]>(opts: consumeOpts, cancellationToken: ct))
                {
                    _logger.LogDebug("[consumer] Received message: subject={Subject} bytes={Bytes}",
                        msg.Subject, msg.Data?.Length ?? 0);
                    await channel.Writer.WriteAsync(msg, ct);
                }
                _logger.LogDebug("[consumer] ConsumeAsync loop ended normally.");
            }
            catch (OperationCanceledException)
            {
                _logger.LogDebug("[consumer] Producer task cancelled.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[consumer] ConsumeAsync producer faulted.");
            }
            finally
            {
                channel.Writer.TryComplete();
            }
        }, CancellationToken.None);

        // Consumer: reads from the channel with a short read-timeout so we can do
        // time-based flushes even when no messages are arriving.
        try
        {
            while (!ct.IsCancellationRequested)
            {
                INatsJSMsg<byte[]>? natsMsg = null;
                try
                {
                    using var readCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    readCts.CancelAfter(_config.MaxPendingLatency);
                    natsMsg = await channel.Reader.ReadAsync(readCts.Token);
                }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                {
                    // Timeout — no new messages; fall through to time-based flush.
                }
                catch (ChannelClosedException)
                {
                    break;
                }

                if (natsMsg is not null)
                {
                    // When paused, NAK with a delay so NATS holds the message server-side
                    // before redelivery. Without the delay NATS redelivers immediately,
                    // causing a tight loop that wastes bandwidth and CPU.
                    if (_paused)
                    {
                        await natsMsg.NakAsync(new AckOpts { NakDelay = TimeSpan.FromSeconds(30) }, cancellationToken: ct);
                        continue;
                    }

                    _logger.LogDebug("[consumer] Dequeued message: subject={Subject} bytes={Bytes}",
                        natsMsg.Subject, natsMsg.Data?.Length ?? 0);

                    DbChangeEvent evt;
                    try
                    {
                        evt = await DecodeMessageAsync(natsMsg, ct);
                        _logger.LogDebug("[consumer] Decoded event: op={Op} table={Table} id={Id} modelVersion={Version}",
                            evt.Operation, evt.Table, evt.Id, evt.ModelVersion);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "[consumer] Failed to decode NATS message.");
                        await natsMsg.NakAsync(cancellationToken: ct);
                        continue;
                    }

                    // ── Schema migration check (mirrors Go handlePossibleMigration) ──
                    bool forceFlush = false;
                    var registry = _config.Registry;
                    if (registry is not null && _driver is IDriverMigration migration)
                    {
                        try
                        {
                            forceFlush = await HandlePossibleMigrationAsync(migration, registry, evt, ct);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "[consumer] Migration check failed for {Table} v{Version}.",
                                evt.Table, evt.ModelVersion);
                            await natsMsg.NakAsync(cancellationToken: ct);
                            continue;
                        }
                    }

                    EdsMetrics.PendingEvents.Inc();
                    pending.Add((evt, natsMsg));
                    pendingStarted ??= DateTimeOffset.UtcNow;

                    bool shouldFlushNow = await _driver.ProcessAsync(_logger, evt, ct);

                    if (shouldFlushNow || forceFlush || (batchSize > 0 && pending.Count >= batchSize))
                    {
                        await FlushAndAckAsync(pending, ct);
                        pendingStarted = null;
                        continue;
                    }
                }

                // Time-based flush
                if (pending.Count > 0 && pendingStarted is not null)
                {
                    var elapsed = DateTimeOffset.UtcNow - pendingStarted.Value;
                    if (elapsed >= _config.MaxPendingLatency ||
                        (elapsed >= _config.MinPendingLatency && natsMsg is null))
                    {
                        await FlushAndAckAsync(pending, ct);
                        pendingStarted = null;
                    }
                }
            }
        }
        finally
        {
            await producerTask;
        }

        if (pending.Count > 0)
        {
            try { await FlushAndAckAsync(pending, CancellationToken.None); }
            catch (Exception ex) { _logger.LogError(ex, "[consumer] Error during final flush on shutdown."); }
        }
    }

    // Exponential backoff delays for flush retries: 2s → 4s → 8s → 16s → 30s
    private static readonly TimeSpan[] FlushRetryDelays =
    [
        TimeSpan.FromSeconds(2),  TimeSpan.FromSeconds(4),
        TimeSpan.FromSeconds(8),  TimeSpan.FromSeconds(16),
        TimeSpan.FromSeconds(30),
    ];
    private const int MaxFlushAttempts = 5;

    private async Task FlushAndAckAsync(
        List<(DbChangeEvent evt, INatsJSMsg<byte[]> msg)> pending,
        CancellationToken ct)
    {
        _logger.LogInformation("[consumer] Flushing {Count} event(s) to driver.", pending.Count);
        _logger.LogDebug("[consumer] Flush batch: {Tables}",
            string.Join(", ", pending.Select(p => $"{p.evt.Operation}:{p.evt.Table}")));

        var sw = System.Diagnostics.Stopwatch.StartNew();
        Exception? lastEx = null;

        for (int attempt = 1; attempt <= MaxFlushAttempts; attempt++)
        {
            try
            {
                await _driver.FlushAsync(_logger, ct);
                sw.Stop();

                _logger.LogInformation("[consumer] Flush completed in {Ms}ms — {Count} event(s) ACKed.",
                    sw.ElapsedMilliseconds, pending.Count);

                EdsMetrics.FlushDurationSeconds.Observe(sw.Elapsed.TotalSeconds);
                EdsMetrics.FlushCount.Observe(pending.Count);
                EdsMetrics.TotalEvents.Inc(pending.Count);
                EdsMetrics.PendingEvents.Dec(pending.Count);

                foreach (var (_, msg) in pending)
                    await msg.AckAsync(cancellationToken: ct);

                pending.Clear();
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // Graceful shutdown — NAK without counting as a failure.
                _logger.LogInformation("[consumer] Flush cancelled during shutdown — NAKing {Count} message(s).", pending.Count);
                foreach (var (_, msg) in pending)
                {
                    try { await msg.NakAsync(cancellationToken: CancellationToken.None); } catch { }
                }
                pending.Clear();
                return;
            }
            catch (Exception ex)
            {
                lastEx = ex;
                if (attempt < MaxFlushAttempts)
                {
                    var delay = FlushRetryDelays[Math.Min(attempt - 1, FlushRetryDelays.Length - 1)];
                    _logger.LogWarning(ex,
                        "[consumer] Flush attempt {Attempt}/{Max} failed — retrying in {Delay}s: {Error}",
                        attempt, MaxFlushAttempts, (int)delay.TotalSeconds, ex.Message);
                    try { await Task.Delay(delay, ct); }
                    catch (OperationCanceledException) { break; }

                    // FlushAsync always clears its internal buffer on failure; re-queue the events
                    // so the next attempt has data to commit.
                    foreach (var (evt, _) in pending)
                    {
                        try { await _driver.ProcessAsync(_logger, evt, ct); }
                        catch (Exception requeueEx)
                        {
                            _logger.LogWarning(requeueEx,
                                "[consumer] Failed to re-queue {Table}/{Id} for retry — event may be skipped.",
                                evt.Table, evt.Id);
                        }
                    }
                }
            }
        }

        // ── All attempts exhausted ─────────────────────────────────────────────
        _logger.LogError(lastEx,
            "[consumer] Flush failed after {Max} attempt(s) — NAKing {Count} message(s) and stopping consumer.",
            MaxFlushAttempts, pending.Count);
        _logger.LogCritical("[consumer] Unrecoverable flush error. Process will exit with a failing status code.");

        foreach (var (_, msg) in pending)
        {
            try { await msg.NakAsync(cancellationToken: CancellationToken.None); } catch { }
        }
        pending.Clear();

        // Signal a non-zero exit code so the process manager (systemd, Docker, etc.) will restart us.
        Environment.ExitCode = 1;
        throw lastEx!;
    }

    // ── Schema migration (mirrors Go handlePossibleMigration) ─────────────────

    private async Task<bool> HandlePossibleMigrationAsync(
        IDriverMigration migration,
        ISchemaRegistry registry,
        DbChangeEvent evt,
        CancellationToken ct)
    {
        var (found, currentVersion) = await registry.GetTableVersionAsync(evt.Table, ct);

        if (found && currentVersion == evt.ModelVersion)
            return false; // Nothing changed

        var newSchema = await registry.GetSchemaAsync(evt.Table, evt.ModelVersion, ct);
        if (newSchema is null)
        {
            _logger.LogWarning("[consumer] Schema not found for {Table} v{Version}. Skipping migration.",
                evt.Table, evt.ModelVersion);
            return false;
        }

        if (!found)
        {
            // Brand-new table
            _logger.LogInformation("[consumer] Migrating new table: {Table} v{Version}", evt.Table, evt.ModelVersion);
            await migration.MigrateNewTableAsync(_logger, newSchema, ct);
            await registry.SetTableVersionAsync(evt.Table, evt.ModelVersion, ct);
            return true;
        }

        // Table exists but model version changed — find new columns
        var oldSchema = await registry.GetSchemaAsync(evt.Table, currentVersion, ct);
        if (oldSchema is not null)
        {
            var newCols = newSchema.Columns()
                .Where(c => !oldSchema.Properties.ContainsKey(c))
                .ToList();

            if (newCols.Count > 0)
            {
                _logger.LogInformation("[consumer] Migrating {Count} new column(s) for {Table} v{Version}: {Columns}",
                    newCols.Count, evt.Table, evt.ModelVersion, string.Join(", ", newCols));
                await migration.MigrateNewColumnsAsync(_logger, newSchema, newCols, ct);
            }
        }

        await registry.SetTableVersionAsync(evt.Table, evt.ModelVersion, ct);
        return true;
    }

    private static async Task<DbChangeEvent> DecodeMessageAsync(INatsJSMsg<byte[]> msg, CancellationToken ct)
    {
        var payload = msg.Data ?? [];

        if (msg.Headers != null
            && msg.Headers.TryGetValue("Content-Encoding", out var encodings)
            && encodings.Any(e => e.Contains("gzip", StringComparison.OrdinalIgnoreCase)))
        {
            payload = await DecompressGzipAsync(payload, ct);
        }

        bool isMsgPack = msg.Headers != null
            && msg.Headers.TryGetValue("Content-Type", out var contentTypes)
            && contentTypes.Any(ct2 => ct2.Contains("msgpack", StringComparison.OrdinalIgnoreCase));

        if (isMsgPack)
            return MessagePackSerializer.Deserialize<DbChangeEvent>(payload);

        return JsonSerializer.Deserialize<DbChangeEvent>(payload)
            ?? throw new InvalidDataException("Failed to deserialize DbChangeEvent from JSON.");
    }

    private static async Task<byte[]> DecompressGzipAsync(byte[] compressed, CancellationToken ct)
    {
        using var input  = new MemoryStream(compressed);
        await using var gz = new GZipStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        await gz.CopyToAsync(output, ct);
        return output.ToArray();
    }

    // ── Credential & subject helpers ──────────────────────────────────────────

    private static string BuildConsumerName(CredentialInfo credInfo)
    {
        return $"eds-{credInfo.ServerID}";
    }

    private static List<string> BuildFilterSubjects(CredentialInfo credInfo)
    {
        if (credInfo.CompanyIds.Count == 0)
            return ["dbchange.>"];
        return credInfo.CompanyIds
            .Select(id => $"dbchange.*.*.{id}.*.PUBLIC.>")
            .ToList();
    }

    /// <summary>
    /// Parses the NATS credentials file to extract serverID (JWT <c>name</c> claim),
    /// sessionID (from <c>eds.notify.{id}.&gt;</c> subscription), and
    /// companyIDs (from <c>dbchange.*.*.{id}.*.PUBLIC.&gt;</c> subscriptions).
    /// Mirrors credentials.go from the Go implementation.
    /// </summary>
    private static CredentialInfo ParseCredentialInfo(string credsFile)
    {
        if (string.IsNullOrEmpty(credsFile) || !File.Exists(credsFile))
            throw new InvalidOperationException(
                $"NATS credentials file not found: '{credsFile}'. " +
                "Ensure the server is enrolled and the credentials file is present.");

        try
        {
            var content = File.ReadAllText(credsFile);

            // Extract JWT from NKS decorated credential file
            var jwtMatch = Regex.Match(content,
                @"-----BEGIN NATS USER JWT-----\s*(.+?)\s*-+END NATS USER JWT-+",
                RegexOptions.Singleline);
            if (!jwtMatch.Success)
                throw new InvalidOperationException("Could not locate JWT in credentials file.");

            var rawJwt = jwtMatch.Groups[1].Value.Trim();
            var parts  = rawJwt.Split('.');
            if (parts.Length != 3)
                throw new InvalidOperationException("JWT in credentials file is not 3-part.");

            var payloadBytes = Base64UrlDecode(parts[1]);
            using var doc    = JsonDocument.Parse(payloadBytes);
            var root         = doc.RootElement;

            // serverID = JWT `name` claim
            var serverID = root.TryGetProperty("name", out var nameProp)
                ? nameProp.GetString() ?? ""
                : "";

            if (string.IsNullOrEmpty(serverID))
                throw new InvalidOperationException("Missing 'name' claim in JWT (server ID).");

            // Extract sessionID and companyIDs from nats.sub.allow
            var companyIds = new List<string>();
            var sessionID  = "";

            if (root.TryGetProperty("nats", out var natsProp)
                && natsProp.TryGetProperty("sub",  out var subProp)
                && subProp.TryGetProperty("allow", out var allowProp)
                && allowProp.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in allowProp.EnumerateArray())
                {
                    var sub         = item.GetString() ?? "";
                    var sessionMatch = Regex.Match(sub, @"^eds\.notify\.([a-f0-9-]+)\.");
                    if (sessionMatch.Success) { sessionID = sessionMatch.Groups[1].Value; continue; }

                    var companyMatch = Regex.Match(sub, @"^dbchange\.\*\.\*\.([a-f0-9-]+)\.");
                    if (companyMatch.Success) companyIds.Add(companyMatch.Groups[1].Value);
                }
            }

            if (companyIds.Count == 0)
                throw new InvalidOperationException(
                    "No company IDs found in JWT subscription allow-list. Ensure the credential has the correct permissions.");

            return new CredentialInfo
            {
                CompanyIds = companyIds,
                ServerID   = serverID,
                SessionID  = sessionID
            };
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Failed to parse credential info from '{credsFile}': {ex.Message}", ex);
        }
    }

    private static byte[] Base64UrlDecode(string input)
    {
        var padded = input.Replace('-', '+').Replace('_', '/');
        padded += (padded.Length % 4) switch { 2 => "==", 3 => "=", _ => "" };
        return Convert.FromBase64String(padded);
    }
}
