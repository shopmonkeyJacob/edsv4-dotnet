using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Prometheus;

namespace EDS.Infrastructure.Metrics;

public sealed class MetricsOptions
{
    public int Port { get; set; } = 8080;
    public string Host { get; set; } = "+";
}

/// <summary>
/// Standalone Prometheus HTTP metrics server (no ASP.NET Core required).
/// Exposes /metrics on the configured port.
/// </summary>
public sealed class MetricsServer : BackgroundService
{
    private readonly MetricsOptions _options;
    private readonly ILogger<MetricsServer> _logger;
    private IMetricServer? _server;

    public MetricsServer(IOptions<MetricsOptions> options, ILogger<MetricsServer> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Run Start() on a thread-pool thread so that HttpListener binding (which can
        // take several seconds on some systems) does not block the host's sequential
        // hosted-service startup chain.
        _ = Task.Run(() =>
        {
            _server = new MetricServer(hostname: _options.Host, port: _options.Port, url: "metrics/");
            _server.Start();
            _logger.LogInformation("Metrics server started on port {Port}", _options.Port);

            stoppingToken.Register(() =>
            {
                _server.StopAsync().GetAwaiter().GetResult();
                _logger.LogInformation("Metrics server stopped.");
            });
        }, stoppingToken);

        return Task.CompletedTask;
    }
}
