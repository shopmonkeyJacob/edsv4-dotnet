using EDS.Infrastructure.Alerting;
using EDS.Infrastructure.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Net;

namespace EDS.Infrastructure.Tests;

public class AlertManagerTests
{
    // ── NullAlertManager ──────────────────────────────────────────────────────

    [Fact]
    public async Task NullAlertManager_FireAsync_CompletesWithoutThrowing()
    {
        var mgr = NullAlertManager.Instance;
        await mgr.FireAsync(new Alert("title", "body", "critical"));
    }

    // ── No channels configured ────────────────────────────────────────────────

    [Fact]
    public async Task AlertManager_NoChannels_NoHttpRequests()
    {
        var (mgr, handler) = Build([]);
        await mgr.FireAsync(new Alert("test", "body", "critical"));
        Assert.Empty(handler.Requests);
    }

    // ── Severity filtering ────────────────────────────────────────────────────

    [Fact]
    public async Task AlertManager_WrongSeverity_ChannelNotCalled()
    {
        // Channel only accepts "critical" — firing a "warning" should not reach it.
        var (mgr, handler) = BuildWithSlack(["critical"]);
        await mgr.FireAsync(new Alert("test", "body", "warning"));
        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task AlertManager_MatchingSeverity_ChannelCalled()
    {
        var (mgr, handler) = BuildWithSlack(["critical"]);
        await mgr.FireAsync(new Alert("test", "body", "critical"));
        Assert.Single(handler.Requests);
    }

    [Fact]
    public async Task AlertManager_MultiSeverityChannel_ReceivesBoth()
    {
        var (mgr, handler) = BuildWithSlack(["warning", "critical"]);
        await mgr.FireAsync(new Alert("w", "body", "warning"));
        await mgr.FireAsync(new Alert("c", "body", "critical"));
        Assert.Equal(2, handler.Requests.Count);
    }

    // ── Cooldown deduplication ────────────────────────────────────────────────

    [Fact]
    public async Task AlertManager_SameAlertWithinCooldown_SentOnce()
    {
        var (mgr, handler) = BuildWithSlack(["critical"], cooldownSeconds: 300);
        await mgr.FireAsync(new Alert("same-title", "body", "critical"));
        await mgr.FireAsync(new Alert("same-title", "body", "critical"));  // within cooldown
        Assert.Single(handler.Requests);
    }

    [Fact]
    public async Task AlertManager_ZeroCooldown_SameAlertSentEveryTime()
    {
        var (mgr, handler) = BuildWithSlack(["critical"], cooldownSeconds: 0);
        await mgr.FireAsync(new Alert("same-title", "body", "critical"));
        await mgr.FireAsync(new Alert("same-title", "body", "critical"));
        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task AlertManager_DifferentTitlesWithinCooldown_BothDelivered()
    {
        var (mgr, handler) = BuildWithSlack(["critical"], cooldownSeconds: 300);
        await mgr.FireAsync(new Alert("alert-1", "body", "critical"));
        await mgr.FireAsync(new Alert("alert-2", "body", "critical"));
        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task AlertManager_SameTitleDifferentChannels_CooldownIsPerChannel()
    {
        // Two channels — each has its own independent cooldown bucket.
        var handler = new FakeHttpMessageHandler();
        var http    = new HttpClient(handler);
        var opts    = Options.Create(new AlertsOptions
        {
            CooldownSeconds = 300,
            Channels =
            [
                new ChannelOptions { Type = "slack",   Url = "http://ch1", Severity = ["critical"] },
                new ChannelOptions { Type = "webhook", Url = "http://ch2", Severity = ["critical"] },
            ]
        });
        using var mgr = new AlertManager(opts, NullLogger<AlertManager>.Instance, http);

        await mgr.FireAsync(new Alert("same-title", "body", "critical"));
        // Both channels receive the first fire — one request each.
        Assert.Equal(2, handler.Requests.Count);

        await mgr.FireAsync(new Alert("same-title", "body", "critical"));
        // Both suppressed by cooldown — no new requests.
        Assert.Equal(2, handler.Requests.Count);
    }

    // ── Channel errors do not propagate ──────────────────────────────────────

    [Fact]
    public async Task AlertManager_ChannelReturns500_DoesNotThrow()
    {
        var (mgr, _) = BuildWithSlack(["critical"], statusCode: HttpStatusCode.InternalServerError);
        await mgr.FireAsync(new Alert("test", "body", "critical"));  // must not throw
    }

    [Fact]
    public async Task AlertManager_OneChannelFails_OtherChannelStillReceives()
    {
        // Two-channel setup: first always fails, second succeeds.
        var handler = new FakeHttpMessageHandler();
        var http    = new HttpClient(handler);

        handler.StatusByUrl["http://bad"]  = HttpStatusCode.ServiceUnavailable;
        handler.StatusByUrl["http://good"] = HttpStatusCode.OK;

        var opts = Options.Create(new AlertsOptions
        {
            CooldownSeconds = 0,
            Channels =
            [
                new ChannelOptions { Type = "slack", Url = "http://bad",  Severity = ["critical"] },
                new ChannelOptions { Type = "slack", Url = "http://good", Severity = ["critical"] },
            ]
        });
        using var mgr = new AlertManager(opts, NullLogger<AlertManager>.Instance, http);

        await mgr.FireAsync(new Alert("test", "body", "critical"));

        Assert.Equal(2, handler.Requests.Count);  // both attempted, even though first failed
    }

    // ── Unknown channel type ──────────────────────────────────────────────────

    [Fact]
    public async Task AlertManager_UnknownChannelType_DoesNotThrow()
    {
        var opts = Options.Create(new AlertsOptions
        {
            Channels = [new ChannelOptions { Type = "carrier-pigeon", Severity = ["critical"] }]
        });
        var handler = new FakeHttpMessageHandler();
        using var mgr = new AlertManager(opts, NullLogger<AlertManager>.Instance, new HttpClient(handler));
        await mgr.FireAsync(new Alert("test", "body", "critical"));
        Assert.Empty(handler.Requests);  // no HTTP call for unknown type
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static (AlertManager Manager, FakeHttpMessageHandler Handler) Build(
        List<ChannelOptions> channels,
        int cooldownSeconds = 300,
        HttpStatusCode statusCode = HttpStatusCode.OK)
    {
        var handler = new FakeHttpMessageHandler { DefaultStatus = statusCode };
        var http    = new HttpClient(handler);
        var opts    = Options.Create(new AlertsOptions { Channels = channels, CooldownSeconds = cooldownSeconds });
        return (new AlertManager(opts, NullLogger<AlertManager>.Instance, http), handler);
    }

    private static (AlertManager Manager, FakeHttpMessageHandler Handler) BuildWithSlack(
        List<string> severity,
        int cooldownSeconds = 300,
        HttpStatusCode statusCode = HttpStatusCode.OK)
    {
        return Build(
            [new ChannelOptions { Type = "slack", Url = "http://fake.example.com/hook", Severity = severity }],
            cooldownSeconds,
            statusCode);
    }
}

// ── Test double ───────────────────────────────────────────────────────────────

/// <summary>
/// Captures outbound HTTP requests without hitting the network.
/// Per-URL status overrides let individual test scenarios simulate failures.
/// </summary>
internal sealed class FakeHttpMessageHandler : HttpMessageHandler
{
    public List<HttpRequestMessage> Requests { get; } = [];
    public HttpStatusCode DefaultStatus { get; set; } = HttpStatusCode.OK;
    public Dictionary<string, HttpStatusCode> StatusByUrl { get; } = new();

    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request, CancellationToken ct)
    {
        Requests.Add(request);
        var url    = request.RequestUri?.ToString() ?? "";
        var status = StatusByUrl.TryGetValue(url, out var s) ? s : DefaultStatus;
        return Task.FromResult(new HttpResponseMessage(status));
    }
}
