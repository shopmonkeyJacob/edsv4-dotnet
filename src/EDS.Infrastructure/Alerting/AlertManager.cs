using EDS.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Net.Mail;
using System.Text.Json;

namespace EDS.Infrastructure.Alerting;

/// <summary>A single outbound alert event.</summary>
public sealed record Alert(
    string Title,
    string Body,
    string Severity,
    IReadOnlyDictionary<string, string>? Tags = null);

public interface IAlertManager
{
    /// <summary>Route <paramref name="alert"/> to all eligible channels, respecting the configured cooldown.</summary>
    Task FireAsync(Alert alert, CancellationToken ct = default);
}

/// <summary>No-op implementation used when no channels are configured.</summary>
public sealed class NullAlertManager : IAlertManager
{
    public static readonly NullAlertManager Instance = new();
    public Task FireAsync(Alert alert, CancellationToken ct = default) => Task.CompletedTask;
}

/// <summary>
/// Routes alert events to configured channels with per-channel, per-title cooldown
/// deduplication. All eligible channels for a single fire are dispatched concurrently;
/// failures are logged but never rethrown so the consumer loop is never interrupted.
/// </summary>
public sealed class AlertManager : IAlertManager, IDisposable
{
    private readonly AlertsOptions _options;
    private readonly ILogger<AlertManager> _logger;
    private readonly HttpClient _http;
    private readonly Dictionary<(int ChannelIndex, string Title), DateTime> _lastFired = new();
    private readonly object _cooldownLock = new();

    public AlertManager(IOptions<AlertsOptions> options, ILogger<AlertManager> logger, HttpClient? http = null)
    {
        _options   = options.Value;
        _logger    = logger;
        _http      = http ?? new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
        _ownsHttp  = http is null;
    }

    private readonly bool _ownsHttp;

    public async Task FireAsync(Alert alert, CancellationToken ct = default)
    {
        if (_options.Channels.Count == 0) return;

        var now   = DateTime.UtcNow;
        var tasks = new List<Task>();

        for (int i = 0; i < _options.Channels.Count; i++)
        {
            var ch = _options.Channels[i];
            if (!ch.Severity.Contains(alert.Severity, StringComparer.OrdinalIgnoreCase))
                continue;

            var key = (i, alert.Title);
            lock (_cooldownLock)
            {
                if (_lastFired.TryGetValue(key, out var last)
                    && (now - last).TotalSeconds < _options.CooldownSeconds)
                {
                    _logger.LogDebug("[alerting] Suppressing '{Title}' on channel {Idx} (cooldown).",
                        alert.Title, i);
                    continue;
                }
                _lastFired[key] = now;
            }

            var capturedCh = ch;
            tasks.Add(SendToChannelAsync(capturedCh, alert, ct));
        }

        if (tasks.Count > 0)
            await Task.WhenAll(tasks);   // individual senders catch + log their own exceptions
    }

    private async Task SendToChannelAsync(ChannelOptions ch, Alert alert, CancellationToken ct)
    {
        try
        {
            await (ch.Type.ToLowerInvariant() switch
            {
                "slack"     => SendSlackAsync(ch, alert, ct),
                "pagerduty" => SendPagerDutyAsync(ch, alert, ct),
                "webhook"   => SendWebhookAsync(ch, alert, ct),
                "email"     => SendEmailAsync(ch, alert),
                _           => LogUnknownAsync(ch.Type),
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[alerting] Failed to deliver to {Type} channel: {Title}",
                ch.Type, alert.Title);
        }
    }

    private Task LogUnknownAsync(string type)
    {
        _logger.LogWarning("[alerting] Unknown channel type '{Type}' — skipping.", type);
        return Task.CompletedTask;
    }

    // ── Slack ─────────────────────────────────────────────────────────────────

    private async Task SendSlackAsync(ChannelOptions ch, Alert alert, CancellationToken ct)
    {
        var emoji = alert.Severity switch
        {
            "info"     => ":information_source:",
            "warning"  => ":warning:",
            "critical" => ":rotating_light:",
            _          => ":bell:",
        };
        var payload = new { text = $"{emoji} *{alert.Title}*\n{alert.Body}" };
        using var content = JsonContent(payload);
        var resp = await _http.PostAsync(ch.Url, content, ct);
        resp.EnsureSuccessStatusCode();
        _logger.LogInformation("[alerting] Slack alert sent: {Title}", alert.Title);
    }

    // ── PagerDuty Events API v2 ────────────────────────────────────────────────

    private async Task SendPagerDutyAsync(ChannelOptions ch, Alert alert, CancellationToken ct)
    {
        var details = new Dictionary<string, string> { ["detail"] = alert.Body };
        if (alert.Tags is not null) foreach (var (k, v) in alert.Tags) details[k] = v;

        var payload = new
        {
            routing_key  = ch.RoutingKey,
            event_action = "trigger",
            payload      = new
            {
                summary        = alert.Title,
                severity       = alert.Severity,
                source         = "eds",
                custom_details = details,
            },
        };
        using var content = JsonContent(payload);
        var resp = await _http.PostAsync("https://events.pagerduty.com/v2/enqueue", content, ct);
        resp.EnsureSuccessStatusCode();
        _logger.LogInformation("[alerting] PagerDuty alert sent: {Title}", alert.Title);
    }

    // ── Generic webhook ───────────────────────────────────────────────────────

    private async Task SendWebhookAsync(ChannelOptions ch, Alert alert, CancellationToken ct)
    {
        var body = new Dictionary<string, string>
        {
            ["title"]    = alert.Title,
            ["body"]     = alert.Body,
            ["severity"] = alert.Severity,
        };
        if (alert.Tags is not null) foreach (var (k, v) in alert.Tags) body[k] = v;

        using var req = new HttpRequestMessage(HttpMethod.Post, ch.Url)
        {
            Content = JsonContent(body),
        };
        foreach (var (k, v) in ch.Headers) req.Headers.TryAddWithoutValidation(k, v);
        var resp = await _http.SendAsync(req, ct);
        resp.EnsureSuccessStatusCode();
        _logger.LogInformation("[alerting] Webhook alert sent: {Title}", alert.Title);
    }

    // ── Email (SMTP) ──────────────────────────────────────────────────────────

    private async Task SendEmailAsync(ChannelOptions ch, Alert alert)
    {
        var from = string.IsNullOrEmpty(ch.From) ? ch.Username : ch.From;
        using var mail = new MailMessage
        {
            From    = new MailAddress(from),
            Subject = $"[EDS {alert.Severity.ToUpperInvariant()}] {alert.Title}",
            Body    = $"{alert.Title}\n\n{alert.Body}",
        };
        foreach (var to in ch.To) mail.To.Add(to);

        using var smtp = new SmtpClient(ch.Host, ch.Port)
        {
            EnableSsl             = ch.UseTls,
            UseDefaultCredentials = false,
        };
        if (!string.IsNullOrEmpty(ch.Username))
            smtp.Credentials = new System.Net.NetworkCredential(ch.Username, ch.Password);

        await Task.Run(() => smtp.Send(mail));
        _logger.LogInformation("[alerting] Email alert sent: {Title} → {To}",
            alert.Title, string.Join(", ", ch.To));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static StringContent JsonContent<T>(T payload)
    {
        var json = JsonSerializer.Serialize(payload);
        return new StringContent(json, System.Text.Encoding.UTF8, "application/json");
    }

    public void Dispose() { if (_ownsHttp) _http.Dispose(); }
}
