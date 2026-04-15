using EDS.Core;
using EDS.Core.Abstractions;
using EDS.Core.Registry;
using EDS.Infrastructure.Configuration;
using EDS.Infrastructure.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;

namespace EDS.Cli;

internal static class ServerCommand
{
    internal const string ConsoleTemplate = "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}";
    internal const string FileTemplate    = "[{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} {Level:u3}] {Message:lj}{NewLine}{Exception}";

    public static async Task RunServerAsync(
        string configPath,
        string dataDir,
        bool verbose,
        TimeSpan renewInterval,
        EDS.Infrastructure.Tracking.SqliteTracker tracker,
        DriverMode driverMode = DriverMode.TimeSeries,
        string eventsSchema = "eds_events",
        bool dryRun = false,
        CancellationToken ct = default)
    {
        var config = new ConfigurationBuilder()
            .AddTomlFile(configPath, optional: true)
            .AddEnvironmentVariables("EDS_")
            .Build();

        var apiKey    = config["token"] ?? string.Empty;
        var serverId  = config["server_id"] ?? string.Empty;
        var driverUrl = config["url"] ?? string.Empty;

        var minPendingLatency = int.TryParse(config["min_pending_latency"], out var minSec)
            ? TimeSpan.FromSeconds(minSec)
            : TimeSpan.FromSeconds(2);
        var maxPendingLatency = int.TryParse(config["max_pending_latency"], out var maxSec)
            ? TimeSpan.FromSeconds(maxSec)
            : TimeSpan.FromSeconds(30);
        var maxAckPending = int.TryParse(config["max_ack_pending"], out var maxAck)
            ? maxAck
            : 16384;

        if (string.IsNullOrEmpty(apiKey))
        {
            Log.Fatal("[server] API key not found. Make sure you run enroll before continuing.");
            return;
        }

        var apiUrl  = SessionService.GetApiUrlFromJwt(apiKey);
        var natsUrl = SessionService.GetNatsUrl(apiUrl, config["server"]);
        Log.Information("[server] using API url: {ApiUrl}", apiUrl);

        var registry = ServiceRegistration.BuildDriverRegistry();

        if (dryRun)
            Log.Warning("[dry-run] Running in dry-run mode — events will be received and decoded but not written to any destination.");
        if (dryRun && !string.IsNullOrEmpty(driverUrl))
            Log.Warning("[dry-run] Driver URL is configured ({Url}) but will be bypassed.", SessionService.MaskUrl(driverUrl));

        EDS.Infrastructure.Schema.ApiSchemaRegistry? schemaRegistry = null;
        if (!string.IsNullOrEmpty(driverUrl))
        {
            var earlyFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(b => b.AddSerilog(Log.Logger));
            var schemaLogger = new Microsoft.Extensions.Logging.Logger<EDS.Infrastructure.Schema.ApiSchemaRegistry>(earlyFactory);
            schemaRegistry = await EDS.Infrastructure.Schema.ApiSchemaRegistry.CreateAsync(
                tracker, schemaLogger, apiUrl, EdsVersion.Current, ct);
        }

        while (!ct.IsCancellationRequested)
        {
            string sessionId, credsFile;
            while (true)
            {
                try
                {
                    (sessionId, credsFile) = await SessionService.SendStartAsync(
                        apiUrl, apiKey, serverId, driverUrl, dataDir, registry, ct);
                    break;
                }
                catch (InvalidOperationException ex) when (ex.Message.Contains("already running"))
                {
                    Log.Information("[server] {Message}", ex.Message);
                    await Task.Delay(TimeSpan.FromSeconds(5), ct);
                }
            }
            await tracker.SetKeyAsync(SessionService.LastCredsFileKey, credsFile, ct);

            Log.Information("[server] session started: {SessionId}", sessionId);

            if (string.IsNullOrEmpty(driverUrl))
                Log.Information("[server] Return to HQ and continue with configuring your server.");

            var serverLogFile = Path.Combine(dataDir, $"{sessionId}.log");
            Log.Information("[server] Session log: {LogFile}", serverLogFile);

            var backgroundImportCts = new CancellationTokenSource();
            var handlers = NotificationHandlerBuilder.Build(sessionId, configPath, dataDir, verbose, driverUrl, apiKey, tracker, registry, backgroundImportCts, driverMode, eventsSchema, dryRun);

            var exitProcess = false;

            var host = Host.CreateDefaultBuilder()
                .UseSerilog((_, __, lc) => lc
                    .MinimumLevel.Debug()
                    .Enrich.FromLogContext()
                    .WriteTo.Console(
                        restrictedToMinimumLevel: verbose
                            ? Serilog.Events.LogEventLevel.Debug
                            : Serilog.Events.LogEventLevel.Information,
                        outputTemplate: ConsoleTemplate)
                    .WriteTo.File(
                        serverLogFile,
                        restrictedToMinimumLevel: Serilog.Events.LogEventLevel.Debug,
                        outputTemplate: FileTemplate,
                        rollingInterval: RollingInterval.Day,
                        retainedFileCountLimit: 7))
                .ConfigureAppConfiguration(b => b
                    .AddTomlFile(configPath, optional: true)
                    .AddEnvironmentVariables("EDS_"))
                .ConfigureServices((ctx, svc) =>
                {
                    svc.Configure<MetricsOptions>(ctx.Configuration.GetSection("metrics"));
                    svc.Configure<EDS.Infrastructure.Configuration.AlertsOptions>(ctx.Configuration.GetSection("alerts"));
                    svc.AddSingleton<EDS.Infrastructure.Metrics.StatusProvider>();
                    svc.AddSingleton<EDS.Infrastructure.Alerting.IAlertManager, EDS.Infrastructure.Alerting.AlertManager>();
                    svc.AddHostedService<MetricsServer>();
                    svc.AddSingleton(registry);
                    svc.AddSingleton<EDS.Infrastructure.Tracking.SqliteTracker>(tracker);
                    svc.AddSingleton<ITracker>(tracker);

                    svc.AddHostedService(sp => new EDS.Infrastructure.Notification.NotificationService(
                        natsUrl,
                        credsFile,
                        sessionId,
                        handlers,
                        sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<
                            EDS.Infrastructure.Notification.NotificationService>>()));

                    if (dryRun || !string.IsNullOrEmpty(driverUrl))
                    {
                        if (!string.IsNullOrEmpty(driverUrl) && schemaRegistry is not null)
                            svc.AddSingleton<ISchemaRegistry>(_ => schemaRegistry);

                        if (dryRun)
                        {
                            svc.AddSingleton<IDriver>(_ => new NullDriver());
                        }
                        else
                        {
                            var scheme = new Uri(driverUrl).Scheme;
                            var driver = registry.Resolve(scheme)
                                ?? throw new InvalidOperationException($"No driver registered for scheme '{scheme}'.");
                            svc.AddSingleton<IDriver>(_ => driver);
                        }

                        svc.AddSingleton<EDS.Infrastructure.Nats.ConsumerConfig>(sp =>
                        {
                            var t            = sp.GetRequiredService<ITracker>();
                            var dlq          = sp.GetRequiredService<EDS.Infrastructure.Tracking.SqliteTracker>();
                            var driverLogger = sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<
                                EDS.Infrastructure.Nats.NatsConsumerService>>();
                            var exportInfos  = ImportService.TryLoadTableExportInfoAsync(t, CancellationToken.None)
                                                   .GetAwaiter().GetResult();
                            var exportTs     = exportInfos is not null
                                ? exportInfos.ToDictionary(i => i.Table, i => i.Timestamp)
                                : new Dictionary<string, DateTimeOffset>();
                            var schemaReg = sp.GetService<ISchemaRegistry>();
                            return new EDS.Infrastructure.Nats.ConsumerConfig
                            {
                                Url                   = natsUrl,
                                CredentialsFile       = credsFile,
                                Registry              = schemaReg,
                                ExportTableTimestamps = exportTs,
                                Dlq                   = dlq,
                                MinPendingLatency     = minPendingLatency,
                                MaxPendingLatency     = maxPendingLatency,
                                MaxAckPending         = maxAckPending,
                                DriverConfig          = dryRun || string.IsNullOrEmpty(driverUrl) ? null
                                    : new DriverConfig
                                    {
                                        Url            = driverUrl,
                                        Logger         = driverLogger,
                                        SchemaRegistry = schemaReg!,
                                        Tracker        = t,
                                        DataDir        = dataDir,
                                        Mode           = driverMode,
                                        EventsSchema   = eventsSchema,
                                    }
                            };
                        });

                        svc.AddHostedService<EDS.Infrastructure.Nats.NatsConsumerService>();
                    }
                }).Build();

            var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();

            var statusProvider = host.Services.GetRequiredService<EDS.Infrastructure.Metrics.StatusProvider>();
            statusProvider.Version   = EdsVersion.Current;
            statusProvider.SessionId = sessionId;
            statusProvider.Driver    = EDS.Infrastructure.Metrics.StatusProvider.SanitizeUrl(driverUrl);

            handlers.Shutdown = (message, deleted) =>
            {
                if (deleted)
                    Log.Warning("[server] Server removed from Shopmonkey HQ: {Message}", message);
                else
                    Log.Information("[server] Shutdown requested from HQ: {Message}", message);
                lifetime.StopApplication();
                return Task.CompletedTask;
            };

            handlers.Restart = () =>
            {
                Log.Information("[server] Restart requested from HQ — renewing session.");
                Environment.ExitCode = EdsExitCodes.IntentionalRestart;
                lifetime.StopApplication();
                return Task.CompletedTask;
            };

            handlers.Upgrade = async (version) =>
            {
                Log.Information("[server] Upgrade to version {Version} requested from HQ.", version);
                try
                {
                    if (!System.Text.RegularExpressions.Regex.IsMatch(version, @"^[\w.\-]+$")
                        || version.Contains(".."))
                        throw new ArgumentException($"Invalid version string: '{version}'.");

                    var platform    = ServiceRegistration.GetPlatformId();
                    var currentExe  = System.Diagnostics.Process.GetCurrentProcess().MainModule?.FileName
                        ?? throw new InvalidOperationException("Cannot determine current executable path.");
                    var upgradeBase = $"https://download.shopmonkey.cloud/eds/{version}";
                    var upgradeConfig = new EDS.Infrastructure.Upgrade.UpgradeConfig
                    {
                        BinaryUrl       = $"{upgradeBase}/eds-{platform}.tar.gz",
                        SignatureUrl    = $"{upgradeBase}/eds-{platform}.tar.gz.sig",
                        PublicKey       = EdsVersion.ShopmonkeyPublicPgpKey,
                        DestinationPath = currentExe,
                    };
                    using var upgradeFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(
                        b => b.AddSerilog(Log.Logger));
                    var upgradeLogger = new Microsoft.Extensions.Logging.Logger<EDS.Infrastructure.Upgrade.UpgradeService>(upgradeFactory);
                    var upgradeService = new EDS.Infrastructure.Upgrade.UpgradeService(
                        upgradeLogger, new HttpClient { Timeout = TimeSpan.FromMinutes(10) });
                    await upgradeService.UpgradeAsync(upgradeConfig, CancellationToken.None);
                    Log.Information("[server] Upgrade to {Version} complete. Restarting...", version);
                    exitProcess = true;
                    Environment.ExitCode = EdsExitCodes.IntentionalRestart;
                    lifetime.StopApplication();
                    return new EDS.Infrastructure.Notification.UpgradeResponse(Success: true, Error: null);
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "[server] Upgrade to version {Version} failed.", version);
                    return new EDS.Infrastructure.Notification.UpgradeResponse(Success: false, Error: ex.Message);
                }
            };

            var consumer = host.Services.GetServices<IHostedService>()
                .OfType<EDS.Infrastructure.Nats.NatsConsumerService>()
                .FirstOrDefault();
            if (consumer is not null)
            {
                handlers.Pause   = () => { consumer.SetPaused(true);  return Task.CompletedTask; };
                handlers.Unpause = () => { consumer.SetPaused(false); return Task.CompletedTask; };
            }
            else
            {
                Log.Debug("[server] No NATS consumer registered — Pause/Unpause notifications will be ignored.");
            }

            lifetime.ApplicationStopping.Register(() =>
            {
                backgroundImportCts.Cancel();
                SessionService.SendLogsAsync(apiUrl, apiKey, sessionId, dataDir, CancellationToken.None)
                    .GetAwaiter().GetResult();
                SessionService.SendEndAsync(apiUrl, apiKey, sessionId, errored: false,
                    CancellationToken.None).GetAwaiter().GetResult();
                Log.Information("[server] session ended: {SessionId}", sessionId);
            });

            using var sessionCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var sessionCt = sessionCts.Token;

            Log.Debug("[server] Session renewal scheduled every {Hours}h.", renewInterval.TotalHours);
            _ = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(renewInterval, sessionCt);
                    if (!sessionCt.IsCancellationRequested)
                    {
                        Log.Information("[server] Renew interval elapsed ({Hours}h) — renewing session.",
                            renewInterval.TotalHours);
                        Environment.ExitCode = EdsExitCodes.IntentionalRestart;
                        lifetime.StopApplication();
                    }
                }
                catch (OperationCanceledException) { /* session ended */ }
            }, sessionCt);

            _ = Task.Run(async () =>
            {
                try
                {
                    while (!sessionCt.IsCancellationRequested)
                    {
                        await Task.Delay(TimeSpan.FromMinutes(15), sessionCt);
                        var expiry    = SessionService.GetCredentialExpiry(credsFile);
                        var remaining = expiry - DateTimeOffset.UtcNow;
                        if (remaining <= TimeSpan.FromHours(1))
                        {
                            Log.Warning("[server] NATS credential expires in {Minutes} min — renewing session.",
                                (int)remaining.TotalMinutes < 0 ? 0 : (int)remaining.TotalMinutes);
                            Environment.ExitCode = EdsExitCodes.IntentionalRestart;
                            lifetime.StopApplication();
                            return;
                        }
                        Log.Debug("[server] NATS credential valid for {Hours}h {Min}m.",
                            (int)remaining.TotalHours, remaining.Minutes);
                    }
                }
                catch (OperationCanceledException) { /* session ended */ }
            }, sessionCt);

            await host.RunAsync(ct);
            await sessionCts.CancelAsync();

            if (ct.IsCancellationRequested || exitProcess)
                break;

            if (Environment.ExitCode == EdsExitCodes.IntentionalRestart)
            {
                Environment.ExitCode = EdsExitCodes.Success;
                Log.Information("[server] Restarting with fresh session...");
                continue;
            }

            break;
        }
    }
}
