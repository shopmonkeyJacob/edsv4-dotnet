namespace EDS.Infrastructure.Configuration;

/// <summary>
/// Top-level options for outbound alerting channels.
/// Bound from the <c>[alerts]</c> section of config.toml.
///
/// Example config.toml:
/// <code>
/// [alerts]
/// cooldown_seconds = 300
///
/// [[alerts.channels]]
/// type = "slack"
/// url  = "https://hooks.slack.com/services/..."
/// severity = ["warning", "critical"]
///
/// [[alerts.channels]]
/// type = "pagerduty"
/// routing_key = "your-routing-key"
/// severity = ["critical"]
///
/// [[alerts.channels]]
/// type = "webhook"
/// url  = "https://example.com/hooks/eds"
/// headers = { Authorization = "Bearer secret" }
///
/// [[alerts.channels]]
/// type     = "email"
/// host     = "smtp.example.com"
/// port     = 587
/// username = "eds@example.com"
/// password = "..."
/// from     = "eds@example.com"
/// to       = ["oncall@example.com"]
/// severity = ["critical"]
/// </code>
/// </summary>
public sealed class AlertsOptions
{
    /// <summary>Minimum seconds between repeated alerts with the same title on the same channel.</summary>
    public int CooldownSeconds { get; set; } = 300;

    public List<ChannelOptions> Channels { get; set; } = [];
}

/// <summary>Configuration for a single alert delivery channel.</summary>
public sealed class ChannelOptions
{
    /// <summary>Channel type: <c>slack</c> | <c>pagerduty</c> | <c>webhook</c> | <c>email</c></summary>
    public string Type { get; set; } = "";

    /// <summary>Severity levels that this channel should receive. Defaults to warning + critical.</summary>
    public List<string> Severity { get; set; } = ["warning", "critical"];

    // ── Slack / generic webhook ───────────────────────────────────────────────

    /// <summary>Incoming webhook URL (Slack) or generic HTTP endpoint (webhook).</summary>
    public string Url { get; set; } = "";

    /// <summary>Additional HTTP headers sent with webhook requests.</summary>
    public Dictionary<string, string> Headers { get; set; } = new();

    // ── PagerDuty Events API v2 ───────────────────────────────────────────────

    /// <summary>PagerDuty service integration routing key.</summary>
    public string RoutingKey { get; set; } = "";

    // ── Email (SMTP) ──────────────────────────────────────────────────────────

    public string Host     { get; set; } = "";
    public int    Port     { get; set; } = 587;
    public string Username { get; set; } = "";
    public string Password { get; set; } = "";
    public string From     { get; set; } = "";
    public List<string> To { get; set; } = [];

    /// <summary>Use STARTTLS when connecting to the SMTP server (default true).</summary>
    public bool UseTls { get; set; } = true;
}
