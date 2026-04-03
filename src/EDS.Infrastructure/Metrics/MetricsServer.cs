using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Net;
using System.Text.Json;
using PrometheusMetrics = Prometheus.Metrics;

namespace EDS.Infrastructure.Metrics;

public sealed class MetricsOptions
{
    public int Port { get; set; } = 8080;
    public string Host { get; set; } = "+";
}

/// <summary>
/// Standalone HTTP server (no ASP.NET Core required) that exposes two endpoints:
///   GET /metrics  — Prometheus exposition format
///   GET /status   — JSON runtime status snapshot
/// </summary>
public sealed class MetricsServer : BackgroundService
{
    private readonly MetricsOptions _options;
    private readonly StatusProvider _status;
    private readonly ILogger<MetricsServer> _logger;

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.Never
    };

    public MetricsServer(
        IOptions<MetricsOptions> options,
        StatusProvider status,
        ILogger<MetricsServer> logger)
    {
        _options = options.Value;
        _status  = status;
        _logger  = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var listener = new HttpListener();
        listener.Prefixes.Add($"http://{_options.Host}:{_options.Port}/");

        try
        {
            listener.Start();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to start metrics/status server on port {Port}.", _options.Port);
            return;
        }

        _logger.LogInformation(
            "Metrics/status server listening on port {Port}. Endpoints: /metrics  /status",
            _options.Port);

        stoppingToken.Register(() =>
        {
            try { listener.Stop(); } catch { /* ignore on shutdown */ }
        });

        while (!stoppingToken.IsCancellationRequested)
        {
            HttpListenerContext ctx;
            try
            {
                ctx = await listener.GetContextAsync();
            }
            catch (HttpListenerException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }

            // Handle each request on the thread-pool so we never block the accept loop.
            _ = Task.Run(() => HandleAsync(ctx, stoppingToken), stoppingToken);
        }

        _logger.LogInformation("Metrics/status server stopped.");
    }

    private async Task HandleAsync(HttpListenerContext ctx, CancellationToken ct)
    {
        var path = ctx.Request.Url?.AbsolutePath.TrimEnd('/') ?? string.Empty;

        try
        {
            if (path is "/metrics" or "")
            {
                ctx.Response.StatusCode  = 200;
                ctx.Response.ContentType = "text/plain; version=0.0.4; charset=utf-8";
                await PrometheusMetrics.DefaultRegistry.CollectAndExportAsTextAsync(
                    ctx.Response.OutputStream, ct);
            }
            else if (path == "/status")
            {
                var snapshot = _status.GetSnapshot();
                var json     = JsonSerializer.SerializeToUtf8Bytes(snapshot, JsonOpts);
                ctx.Response.StatusCode  = 200;
                ctx.Response.ContentType = "application/json; charset=utf-8";
                ctx.Response.ContentLength64 = json.Length;
                await ctx.Response.OutputStream.WriteAsync(json, ct);
            }
            else
            {
                ctx.Response.StatusCode = 404;
            }
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            _logger.LogWarning(ex, "Error handling {Method} {Path}.",
                ctx.Request.HttpMethod, ctx.Request.Url?.AbsolutePath);
            try { ctx.Response.StatusCode = 500; } catch { /* already sent */ }
        }
        finally
        {
            try { ctx.Response.Close(); } catch { /* ignore */ }
        }
    }
}
