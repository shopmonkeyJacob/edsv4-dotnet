using MessagePack;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using System.Text;
using System.Text.Json;

namespace EDS.Infrastructure.Notification;

/// <summary>
/// Delegates wired up by the host (Program.cs) and called when a notification
/// arrives. All fields are optional — unhandled actions are silently ignored.
/// </summary>
public sealed class NotificationHandlers
{
    public Func<Task>?                                                        Restart     { get; set; }
    public Func<string, bool, Task>?                                          Shutdown    { get; set; }
    public Func<Task>?                                                        Pause       { get; set; }
    public Func<Task>?                                                        Unpause     { get; set; }
    public Func<string, Task<UpgradeResponse>>?                               Upgrade     { get; set; }
    public Func<Task<SendLogsResponse>>?                                      SendLogs    { get; set; }
    /// <summary>Returns the full driver catalog to Shopmonkey HQ.</summary>
    public Func<DriverConfigNotificationResponse>?                            DriverConfig { get; set; }
    /// <summary>Validates driver connection fields and returns the built URL.</summary>
    public Func<string, Dictionary<string, object?>, ValidateNotificationResponse>? Validate { get; set; }
    /// <summary>Persists a validated driver URL to config and restarts the consumer.</summary>
    public Func<string, bool, Task<ConfigureNotificationResponse>>?           Configure   { get; set; }
    /// <summary>Triggers a data import/backfill.</summary>
    public Func<bool, Task>?                                                  Import      { get; set; }
}

public sealed record UpgradeResponse(bool Success, string? Error);
public sealed record SendLogsResponse(bool Success, string? Error);

/// <summary>
/// Connects to NATS and dispatches inbound Shopmonkey HQ commands.
/// Subscribes to <c>eds.notify.{sessionId}.&gt;</c>.
/// Mirrors internal/notification/notification.go from the Go implementation.
/// </summary>
public sealed class NotificationService : BackgroundService
{
    private readonly string               _natsUrl;
    private readonly string               _credentialsFile;
    private readonly string               _sessionId;
    private readonly NotificationHandlers _handlers;
    private readonly ILogger<NotificationService> _logger;

    // Stored after connecting so handlers can publish back to HQ.
    private NatsConnection? _nats;

    public NotificationService(
        string natsUrl,
        string credentialsFile,
        string sessionId,
        NotificationHandlers handlers,
        ILogger<NotificationService> logger)
    {
        _natsUrl         = natsUrl;
        _credentialsFile = credentialsFile;
        _sessionId       = sessionId;
        _handlers        = handlers;
        _logger          = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var opts = new NatsOpts
        {
            Url      = _natsUrl,
            AuthOpts = new NatsAuthOpts
            {
                AuthCredCallback = (_, _) =>
                    ValueTask.FromResult(NatsAuthCred.FromCredsFile(_credentialsFile))
            }
        };

        await using var nats = new NatsConnection(opts);
        _nats = nats;
        await nats.ConnectAsync();

        var subject = $"eds.notify.{_sessionId}.>";
        _logger.LogInformation("Listening for notifications on {Subject}", subject);

        await foreach (var msg in nats.SubscribeAsync<byte[]>(subject, cancellationToken: stoppingToken))
        {
            // Fire-and-forget each command so the subscription loop is never blocked.
            // A per-handler timeout prevents a stuck handler from leaking indefinitely.
            _ = Task.Run(async () =>
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                cts.CancelAfter(TimeSpan.FromMinutes(5));
                try
                {
                    await HandleCommandAsync(msg, cts.Token);
                }
                catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
                {
                    var parts  = msg.Subject.Split('.');
                    var action = parts.Length >= 4 ? parts[3] : "unknown";
                    _logger.LogWarning("Notification handler '{Action}' timed out after 5 minutes.", action);
                }
            }, stoppingToken);
        }
    }

    // ── Dispatch ─────────────────────────────────────────────────────────────

    private async Task HandleCommandAsync(NatsMsg<byte[]> msg, CancellationToken ct)
    {
        // Extract action from the subject: eds.notify.{sessionId}.{action}
        var parts  = msg.Subject.Split('.');
        var action = parts.Length >= 4 ? parts[3] : "unknown";

        _logger.LogDebug("Received notification: {Action} on {Subject}", action, msg.Subject);

        try
        {
            var payload = ParsePayload(msg);

            switch (action)
            {
                case "ping":
                    await HandlePingAsync(payload, ct);
                    break;

                case "driverconfig":
                    await HandleDriverConfigAsync(msg, ct);
                    break;

                case "validate":
                    await HandleValidateAsync(msg, payload, ct);
                    break;

                case "configure":
                    await HandleConfigureAsync(msg, payload, ct);
                    break;

                case "restart":
                    if (_handlers.Restart is not null) await _handlers.Restart();
                    await PublishResponseAsync(action, new GenericNotificationResponse
                        { Success = true, SessionId = _sessionId, Action = action }, ct);
                    break;

                case "pause":
                    if (_handlers.Pause is not null) await _handlers.Pause();
                    await PublishResponseAsync(action, new GenericNotificationResponse
                        { Success = true, SessionId = _sessionId, Action = action }, ct);
                    break;

                case "unpause":
                    if (_handlers.Unpause is not null) await _handlers.Unpause();
                    await PublishResponseAsync(action, new GenericNotificationResponse
                        { Success = true, SessionId = _sessionId, Action = action }, ct);
                    break;

                case "shutdown":
                    var shutdownMsg     = GetString(payload, "message") ?? "";
                    var shutdownDeleted = GetBool(payload, "deleted");
                    if (_handlers.Shutdown is not null)
                        await _handlers.Shutdown(shutdownMsg, shutdownDeleted);
                    break;

                case "upgrade":
                    var version = GetString(payload, "version");
                    if (version is not null && _handlers.Upgrade is not null)
                    {
                        var result = await _handlers.Upgrade(version);
                        await PublishResponseAsync(action, new GenericNotificationResponse
                        {
                            Success   = result.Success,
                            Message   = result.Error,
                            SessionId = _sessionId,
                            Action    = action
                        }, ct);
                    }
                    break;

                case "sendlogs":
                    if (_handlers.SendLogs is not null)
                    {
                        var logsResult = await _handlers.SendLogs();
                        await PublishResponseAsync(action, new GenericNotificationResponse
                        {
                            Success   = logsResult.Success,
                            Message   = logsResult.Error,
                            SessionId = _sessionId,
                            Action    = action
                        }, ct);
                    }
                    break;

                case "import":
                    var backfill = GetBool(payload, "backfill");
                    if (_handlers.Import is not null)
                        await _handlers.Import(backfill);
                    await PublishResponseAsync(action, new GenericNotificationResponse
                        { Success = true, SessionId = _sessionId, Action = action }, ct);
                    break;

                default:
                    _logger.LogWarning("Unknown notification action: {Action}", action);
                    break;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error handling notification '{Action}'", action);
        }
    }

    // ── Action-specific handlers ──────────────────────────────────────────────

    /// <summary>ping — replies "pong" directly to the subject embedded in the data.</summary>
    private async Task HandlePingAsync(NotificationPayload payload, CancellationToken ct)
    {
        var replySubject = GetString(payload, "subject");
        if (replySubject is null || _nats is null)
        {
            _logger.LogWarning("ping notification missing reply subject");
            return;
        }
        await _nats.PublishAsync(replySubject, "pong"u8.ToArray(), cancellationToken: ct);
    }

    /// <summary>
    /// driverconfig — returns the full catalog of registered drivers to HQ so the
    /// configuration UI can be rendered. Mirrors Go's driverconfig handler.
    /// </summary>
    private async Task HandleDriverConfigAsync(NatsMsg<byte[]> msg, CancellationToken ct)
    {
        if (_handlers.DriverConfig is null)
        {
            _logger.LogWarning("No DriverConfig handler registered.");
            return;
        }

        var response = _handlers.DriverConfig();
        _logger.LogDebug("Responding to driverconfig with {Count} driver(s)", response.Drivers.Count);
        await ReplyMsgPackAsync(msg, response, ct);
    }

    /// <summary>validate — validates driver connection fields and returns the constructed URL.</summary>
    private async Task HandleValidateAsync(NatsMsg<byte[]> msg, NotificationPayload payload, CancellationToken ct)
    {
        if (_handlers.Validate is null) return;

        var driver = GetString(payload, "driver") ?? "";
        var config = GetDict(payload, "config");

        var response = _handlers.Validate(driver, config);
        await ReplyMsgPackAsync(msg, response, ct);
    }

    /// <summary>configure — validates and persists a driver URL, then restarts the consumer.</summary>
    private async Task HandleConfigureAsync(NatsMsg<byte[]> msg, NotificationPayload payload, CancellationToken ct)
    {
        if (_handlers.Configure is null) return;

        var url      = GetString(payload, "url") ?? "";
        var backfill = GetBool(payload, "backfill");

        var response = await _handlers.Configure(url, backfill);
        await ReplyMsgPackAsync(msg, response, ct);
    }

    // ── Transport helpers ─────────────────────────────────────────────────────

    /// <summary>
    /// Publishes a msgpack-encoded response to <c>eds.client.{sessionId}.{action}-response</c>.
    /// Used for fire-and-forget actions (pause, unpause, restart, import).
    /// </summary>
    private async Task PublishResponseAsync<T>(string action, T payload, CancellationToken ct)
    {
        if (_nats is null) return;
        var subject = $"eds.client.{_sessionId}.{action}-response";
        var data    = EncodeMsgPack(payload);
        var headers = new NatsHeaders { ["content-encoding"] = "msgpack" };
        await _nats.PublishAsync(subject, data, headers: headers, cancellationToken: ct);
        _logger.LogDebug("Published {Action} response to {Subject}", action, subject);
    }

    /// <summary>
    /// Sends a msgpack-encoded direct reply using the message's ReplyTo subject.
    /// Used for request-response actions (driverconfig, validate, configure).
    /// </summary>
    private static async Task ReplyMsgPackAsync<T>(NatsMsg<byte[]> msg, T payload, CancellationToken ct)
    {
        var data    = EncodeMsgPack(payload);
        var headers = new NatsHeaders { ["content-encoding"] = "msgpack" };
        await msg.ReplyAsync(data: data, headers: headers, cancellationToken: ct);
    }

    private static byte[] EncodeMsgPack<T>(T value) =>
        MessagePackSerializer.Serialize(value);

    // ── Payload parsing ───────────────────────────────────────────────────────

    /// <summary>
    /// Decodes the raw NATS message body into a <see cref="NotificationPayload"/>.
    /// Supports both JSON and msgpack (identified by the <c>Content-Type</c> header).
    /// </summary>
    private static NotificationPayload ParsePayload(NatsMsg<byte[]> msg)
    {
        var data = msg.Data ?? [];
        if (data.Length == 0) return new NotificationPayload();

        try
        {
            var isMsgPack = msg.Headers is not null
                && msg.Headers.TryGetValue("Content-Type", out var enc)
#pragma warning disable CS8602
                && enc.Any(e => e.Contains("msgpack", StringComparison.OrdinalIgnoreCase));
#pragma warning restore CS8602

            string json = isMsgPack
                ? MessagePackSerializer.ConvertToJson(data)
                : Encoding.UTF8.GetString(data);

            return JsonSerializer.Deserialize<NotificationPayload>(json)
                ?? new NotificationPayload();
        }
        catch
        {
            return new NotificationPayload();
        }
    }

    private static string? GetString(NotificationPayload p, string key) =>
        p.Data.TryGetValue(key, out var el) && el.ValueKind == JsonValueKind.String
            ? el.GetString()
            : null;

    private static bool GetBool(NotificationPayload p, string key) =>
        p.Data.TryGetValue(key, out var el) && el.ValueKind == JsonValueKind.True;

    private static Dictionary<string, object?> GetDict(NotificationPayload p, string key)
    {
        var result = new Dictionary<string, object?>();
        if (!p.Data.TryGetValue(key, out var el) || el.ValueKind != JsonValueKind.Object)
            return result;
        foreach (var prop in el.EnumerateObject())
            result[prop.Name] = prop.Value.ValueKind == JsonValueKind.String
                ? prop.Value.GetString()
                : prop.Value.GetRawText();
        return result;
    }
}
