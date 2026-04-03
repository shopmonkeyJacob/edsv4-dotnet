using EDS.Core.Registry;
using EDS.Drivers.AzureBlob;
using EDS.Drivers.EventHub;
using EDS.Drivers.File;
using EDS.Drivers.Kafka;
using EDS.Drivers.MySQL;
using EDS.Drivers.PostgreSQL;
using EDS.Drivers.S3;
using EDS.Drivers.Snowflake;
using EDS.Drivers.SqlServer;
using EDS.Infrastructure.Configuration;
using EDS.Infrastructure.Metrics;
using EDS.Infrastructure.Schema;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using EDS.Cli;
using Microsoft.Extensions.Configuration;
using Spectre.Console;
using System.CommandLine;
using System.Runtime.InteropServices;

// ── Shared log output templates ───────────────────────────────────────────────
// Console: compact — timestamps only, no date.
const string ConsoleTemplate = "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}";
// File: full — date + time + timezone so log files are self-contained.
const string FileTemplate    = "[{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} {Level:u3}] {Message:lj}{NewLine}{Exception}";

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.Console(outputTemplate: ConsoleTemplate)
    .Enrich.FromLogContext()
    .CreateBootstrapLogger();

try { return await BuildRootCommand().Parse(args).InvokeAsync(); }
catch (Exception ex) { Log.Fatal(ex, "Unhandled exception"); return 1; }
finally { await Log.CloseAndFlushAsync(); }

static RootCommand BuildRootCommand()
{
    var configOption = new Option<FileInfo?>("--config", ["-c"]) { Description = "Path to eds.toml" };
    var verboseOption = new Option<bool>("--verbose", ["-v"]) { Description = "Enable debug logging" };
    var dataDirOption = new Option<string>("--data-dir")
    {
        Description = "State/log directory",
        DefaultValueFactory = _ => Path.Combine(Directory.GetCurrentDirectory(), "data")
    };

    var root = new RootCommand("EDS — Enterprise Data Streaming");
    root.Options.Add(configOption);
    root.Options.Add(verboseOption);
    root.Options.Add(dataDirOption);
    root.Subcommands.Add(BuildServerCommand(configOption, verboseOption, dataDirOption));
    root.Subcommands.Add(BuildImportCommand(configOption, verboseOption, dataDirOption));
    root.Subcommands.Add(BuildVersionCommand());
    root.Subcommands.Add(BuildEnrollCommand(configOption));
    root.Subcommands.Add(BuildDriverCommand());
    root.Subcommands.Add(BuildPublicKeyCommand());
    return root;
}

static Command BuildServerCommand(Option<FileInfo?> cfgOpt, Option<bool> verbOpt, Option<string> ddOpt)
{
    var cmd = new Command("server") { Description = "Start the EDS streaming server." };
    cmd.Options.Add(cfgOpt);
    cmd.Options.Add(verbOpt);
    cmd.Options.Add(ddOpt);
    cmd.SetAction(async (parseResult, ct) =>
    {
        var cfg     = parseResult.GetValue(cfgOpt);
        var verbose = parseResult.GetValue(verbOpt);
        var dataDir = parseResult.GetValue(ddOpt) ?? Path.Combine(Directory.GetCurrentDirectory(), "data");

        Directory.CreateDirectory(dataDir);
        var configPath = cfg?.FullName ?? Path.Combine(dataDir, "config.toml");

        var (enrolled, justEnrolled) = await EnrollmentFlow.EnrollIfNeededAsync(dataDir, configPath, ct);
        if (!enrolled) return;

        var tracker = new EDS.Infrastructure.Tracking.SqliteTracker(Path.Combine(dataDir, "state.db"));

        if (justEnrolled)
        {
            AnsiConsole.WriteLine();
            var runImport = AnsiConsole.Confirm(
                "Would you like to run an initial data import before starting the server?",
                defaultValue: false);
            AnsiConsole.WriteLine();

            Log.Information("[server] Post-enrollment import prompt: user chose {Choice}",
                runImport ? "yes" : "no");

            if (runImport)
            {
                // Load config written by EnrollmentFlow (token + server_id are now present)
                var enrollConfig = new ConfigurationBuilder()
                    .AddTomlFile(configPath, optional: true)
                    .AddEnvironmentVariables("EDS_")
                    .Build();

                var apiKey    = enrollConfig["token"] ?? string.Empty;
                var driverUrl = enrollConfig["url"]   ?? string.Empty;

                if (string.IsNullOrEmpty(driverUrl))
                {
                    driverUrl = AnsiConsole.Ask<string>("[bold white]Enter driver URL:[/]").Trim();
                    if (!string.IsNullOrEmpty(driverUrl))
                        await PersistConfigValueAsync(configPath, "url", driverUrl);
                }

                if (!string.IsNullOrEmpty(driverUrl) && !string.IsNullOrEmpty(apiKey))
                {
                    var importOpts = new ImportRunOptions
                    {
                        DataDir          = dataDir,
                        ConfigPath       = configPath,
                        Verbose          = verbose,
                        DriverUrl        = driverUrl,
                        ApiKey           = apiKey,
                        IsPostEnrollment = true,
                    };
                    await RunImportPipelineAsync(importOpts, tracker, ct);
                }
                else
                {
                    Log.Warning("[server] Skipping import — driver URL or API key is missing.");
                }
            }
        }

        await RunServerAsync(configPath, dataDir, verbose, tracker, ct);
    });
    return cmd;
}

/// <summary>
/// Core server startup — loads config, establishes a NATS session, and runs the host.
/// Called by both the server command and the import command (post-import transition).
/// </summary>
static async Task RunServerAsync(
    string configPath,
    string dataDir,
    bool verbose,
    EDS.Infrastructure.Tracking.SqliteTracker tracker,
    CancellationToken ct)
{
    // ── Load config ───────────────────────────────────────────────────────────
    var config = new ConfigurationBuilder()
        .AddTomlFile(configPath, optional: true)
        .AddEnvironmentVariables("EDS_")
        .Build();

    var apiKey    = config["token"] ?? string.Empty;
    var serverId  = config["server_id"] ?? string.Empty;
    var driverUrl = config["url"] ?? string.Empty;

    if (string.IsNullOrEmpty(apiKey))
    {
        Log.Fatal("[server] API key not found. Make sure you run enroll before continuing.");
        return;
    }

    // ── Resolve API + NATS URLs from JWT ──────────────────────────────────────
    var apiUrl  = SessionService.GetApiUrlFromJwt(apiKey);
    var natsUrl = SessionService.GetNatsUrl(apiUrl, config["server"]);
    Log.Information("[server] using API url: {ApiUrl}", apiUrl);

    // ── Build shared state + start schema fetch in parallel with session ──────
    var registry = BuildDriverRegistry();

    Task<EDS.Infrastructure.Schema.ApiSchemaRegistry>? schemaTask = null;
    if (!string.IsNullOrEmpty(driverUrl))
    {
        var earlyFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(b => b.AddSerilog(Log.Logger));
        var schemaLogger = new Microsoft.Extensions.Logging.Logger<EDS.Infrastructure.Schema.ApiSchemaRegistry>(earlyFactory);
        schemaTask = EDS.Infrastructure.Schema.ApiSchemaRegistry.CreateAsync(
            tracker, schemaLogger, apiUrl, EdsVersion.Current, ct);
    }

    // ── Fast path: reuse credential from the last session ────────────────────
    var resumed = await SessionService.TryResumeAsync(tracker, ct);

    string sessionId, credsFile;
    if (resumed is not null)
    {
        (sessionId, credsFile) = resumed.Value;
    }
    else
    {
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
    }

    var schemaRegistry = schemaTask is not null ? await schemaTask : null;

    Log.Information("[server] session {Action}: {SessionId}",
        resumed is not null ? "resumed" : "started", sessionId);

    if (string.IsNullOrEmpty(driverUrl))
        Log.Information("[server] Return to HQ and continue with configuring your server.");

    // ── Build and run host ────────────────────────────────────────────────────
    // Log file is named after the session ID and always captures Debug so the full
    // picture is preserved regardless of whether --verbose was passed.
    var serverLogFile = Path.Combine(dataDir, $"{sessionId}.log");
    Log.Information("[server] Session log: {LogFile}", serverLogFile);

    // CTS used to cancel any background import that was started by a notification
    // handler. Cancelled during ApplicationStopping so imports don't outlive the host.
    var backgroundImportCts = new CancellationTokenSource();

    // Build the handlers object before the host so we can set the five handlers
    // that require IHostApplicationLifetime or NatsConsumerService (available only
    // after host.Build()) by mutating the same reference the NotificationService holds.
    var handlers = BuildNotificationHandlers(sessionId, configPath, dataDir, verbose, driverUrl, apiKey, tracker, registry, backgroundImportCts);

    var host = Host.CreateDefaultBuilder()
        .UseSerilog((_, __, lc) => lc
            .MinimumLevel.Debug()
            .Enrich.FromLogContext()
            // Console: Information+ normally; Debug+ when --verbose.
            .WriteTo.Console(
                restrictedToMinimumLevel: verbose
                    ? Serilog.Events.LogEventLevel.Debug
                    : Serilog.Events.LogEventLevel.Information,
                outputTemplate: ConsoleTemplate)
            // File: always Debug — full detail, regardless of --verbose.
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
            svc.AddSingleton<EDS.Infrastructure.Metrics.StatusProvider>();
            svc.AddHostedService<MetricsServer>();
            svc.AddSingleton(registry);
            svc.AddSingleton<EDS.Infrastructure.Tracking.SqliteTracker>(_ => tracker);
            svc.AddSingleton<EDS.Core.Abstractions.ITracker>(_ => tracker);

            svc.AddHostedService(sp => new EDS.Infrastructure.Notification.NotificationService(
                natsUrl,
                credsFile,
                sessionId,
                handlers,
                sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<
                    EDS.Infrastructure.Notification.NotificationService>>()));

            if (!string.IsNullOrEmpty(driverUrl))
            {
                svc.AddSingleton<EDS.Core.Abstractions.ISchemaRegistry>(_ => schemaRegistry!);

                var scheme = new Uri(driverUrl).Scheme;
                var driver = registry.Resolve(scheme)
                    ?? throw new InvalidOperationException($"No driver registered for scheme '{scheme}'.");
                svc.AddSingleton<EDS.Core.Abstractions.IDriver>(_ => driver);

                svc.AddSingleton<EDS.Infrastructure.Nats.ConsumerConfig>(sp =>
                {
                    var t             = sp.GetRequiredService<EDS.Core.Abstractions.ITracker>();
                    var schemaReg     = sp.GetRequiredService<EDS.Core.Abstractions.ISchemaRegistry>();
                    var driverLogger  = sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<
                        EDS.Infrastructure.Nats.NatsConsumerService>>();
                    return new EDS.Infrastructure.Nats.ConsumerConfig
                    {
                        Url             = natsUrl,
                        CredentialsFile = credsFile,
                        Registry        = schemaReg,
                        DriverConfig    = new EDS.Core.Abstractions.DriverConfig
                        {
                            Url            = driverUrl,
                            Logger         = driverLogger,
                            SchemaRegistry = schemaReg,
                            Tracker        = t,
                            DataDir        = dataDir,
                        }
                    };
                });

                svc.AddHostedService<EDS.Infrastructure.Nats.NatsConsumerService>();
            }
        }).Build();

    var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();

    // ── Populate status provider ──────────────────────────────────────────────
    var statusProvider = host.Services.GetRequiredService<EDS.Infrastructure.Metrics.StatusProvider>();
    statusProvider.Version   = EdsVersion.Current;
    statusProvider.SessionId = sessionId;
    statusProvider.Driver    = EDS.Infrastructure.Metrics.StatusProvider.SanitizeUrl(driverUrl);

    // ── Wire handlers that need host services ─────────────────────────────────
    // NotificationHandlers is a class (reference type), so mutating it here is
    // immediately visible to the NotificationService that holds the same reference.

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
        Log.Information("[server] Restart requested from HQ. Stopping — process manager should restart.");
        lifetime.StopApplication();
        return Task.CompletedTask;
    };

    handlers.Upgrade = async (version) =>
    {
        Log.Information("[server] Upgrade to version {Version} requested from HQ.", version);
        try
        {
            var platform    = GetPlatformId();
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
            lifetime.StopApplication();
            return new EDS.Infrastructure.Notification.UpgradeResponse(Success: true, Error: null);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "[server] Upgrade to version {Version} failed.", version);
            return new EDS.Infrastructure.Notification.UpgradeResponse(Success: false, Error: ex.Message);
        }
    };

    // Pause/Unpause delegate to the NatsConsumerService if one is running.
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
        // Cancel any in-flight background import so it stops cleanly.
        backgroundImportCts.Cancel();

        // Upload logs to HQ before saying goodbye (best-effort).
        SessionService.SendLogsAsync(apiUrl, apiKey, sessionId, dataDir, CancellationToken.None)
            .GetAwaiter().GetResult();

        SessionService.SendEndAsync(apiUrl, apiKey, sessionId, errored: false,
            CancellationToken.None).GetAwaiter().GetResult();
        Log.Information("[server] session ended: {SessionId}", sessionId);
    });

    // ── Background: periodic heartbeat ────────────────────────────────────────
    _ = Task.Run(async () =>
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(60), ct);
                await SessionService.SendHeartbeatAsync(apiUrl, apiKey, sessionId, ct);
            }
        }
        catch (OperationCanceledException) { /* normal shutdown */ }
    }, ct);

    // ── Background: credential expiry watcher ────────────────────────────────
    // Re-establishes the session when the NATS credential is within 1 hour of
    // expiring. Stops the host so the process manager can restart with fresh creds.
    _ = Task.Run(async () =>
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromMinutes(15), ct);
                var expiry    = SessionService.GetCredentialExpiry(credsFile);
                var remaining = expiry - DateTimeOffset.UtcNow;
                if (remaining <= TimeSpan.FromHours(1))
                {
                    Log.Warning("[server] NATS credential expires in {Minutes} min — stopping for renewal.",
                        (int)remaining.TotalMinutes < 0 ? 0 : (int)remaining.TotalMinutes);
                    lifetime.StopApplication();
                    return;
                }
                Log.Debug("[server] NATS credential valid for {Hours}h {Min}m.",
                    (int)remaining.TotalHours, remaining.Minutes);
            }
        }
        catch (OperationCanceledException) { /* normal shutdown */ }
    }, ct);

    await host.RunAsync(ct);
}

/// <summary>
/// Core import pipeline — logging setup through cleanup.
/// Does NOT persist config values or call RunServerAsync; callers handle that.
/// Returns true on success, false if the pipeline aborted early (e.g. connectivity failure).
/// </summary>
static async Task<bool> RunImportPipelineAsync(
    ImportRunOptions opts,
    EDS.Infrastructure.Tracking.SqliteTracker tracker,
    CancellationToken ct)
{
    // ── Setup logging ─────────────────────────────────────────────────────────
    var importLogFile = opts.LogFile ?? Path.Combine(opts.DataDir, $"import-{DateTime.UtcNow:yyyyMMdd-HHmmss}.log");
    Log.Logger = new LoggerConfiguration()
        .MinimumLevel.Debug()
        .Enrich.FromLogContext()
        .WriteTo.Console(
            restrictedToMinimumLevel: opts.Verbose
                ? Serilog.Events.LogEventLevel.Debug
                : Serilog.Events.LogEventLevel.Information,
            outputTemplate: ConsoleTemplate)
        .WriteTo.File(
            importLogFile,
            restrictedToMinimumLevel: Serilog.Events.LogEventLevel.Debug,
            outputTemplate: FileTemplate)
        .CreateLogger();

    Log.Debug("[import] Import log: {LogFile}", importLogFile);

    if (opts.IsPostEnrollment)
        Log.Information("[import] Running post-enrollment import (triggered by user confirmation).");

    // ── Resolve API URL ───────────────────────────────────────────────────────
    var apiUrl = SessionService.GetApiUrlFromJwt(opts.ApiKey);
    Log.Information("[import] Using API url: {ApiUrl}", apiUrl);

    // ── Resolve driver ────────────────────────────────────────────────────────
    var driverRegistry = BuildDriverRegistry();
    var scheme = new Uri(opts.DriverUrl).Scheme;
    var driver = driverRegistry.Resolve(scheme)
        ?? throw new InvalidOperationException($"No driver registered for scheme '{scheme}'.");

    var importDriver     = driver as EDS.Core.Abstractions.IDriverImport;
    var directImporter   = driver as EDS.Core.Abstractions.IDriverDirectImport;

    if (importDriver is null && directImporter is null)
        throw new InvalidOperationException(
            $"Driver '{scheme}' does not support import (implements neither IDriverImport nor IDriverDirectImport).");

    // ── Build schema registry ─────────────────────────────────────────────────
    using var loggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(b => b.AddSerilog(Log.Logger));
    var schemaLogger  = new Microsoft.Extensions.Logging.Logger<EDS.Infrastructure.Schema.ApiSchemaRegistry>(loggerFactory);
    var schemaRegistry = await EDS.Infrastructure.Schema.ApiSchemaRegistry.CreateAsync(
        tracker, schemaLogger, apiUrl, EdsVersion.Current, ct);

    // ── Test driver connectivity ──────────────────────────────────────────────
    var importLogger = loggerFactory.CreateLogger("import");
    Log.Information("[import] Testing driver connection...");
    try
    {
        await driver.TestAsync(importLogger, opts.DriverUrl, ct);
        Log.Information("[import] Driver connection successful.");
    }
    catch (Exception ex)
    {
        Log.Fatal("[import] Driver connection test failed: {Error}", ex.Message);
        return false;
    }

    // ── Delete confirmation (database drivers only) ───────────────────────────
    if (importDriver is not null && !opts.NoDelete && !opts.NoConfirm && !opts.SchemaOnly && !opts.DryRun)
    {
        Log.Warning("[import] This will DROP and RECREATE all destination tables. Existing data will be lost.");
        Console.Write("Continue? [y/N] ");
        var answer = Console.ReadLine()?.Trim().ToLowerInvariant();
        if (answer != "y" && answer != "yes")
        {
            Log.Information("[import] Aborted.");
            return false;
        }
    }

    // ── Initialise driver for import ──────────────────────────────────────────
    if (importDriver is not null)
        await importDriver.InitForImportAsync(importLogger, schemaRegistry, opts.DriverUrl, ct);
    else if (directImporter is not null)
        await directImporter.InitForDirectImportAsync(importLogger, opts.DriverUrl, ct);

    // ── Load checkpoint when --resume is requested ────────────────────────────
    ImportCheckpoint? existingCheckpoint = null;
    IReadOnlySet<string> completedFiles  = new HashSet<string>();

    if (opts.Resume)
    {
        existingCheckpoint = await ImportService.TryLoadCheckpointAsync(tracker, ct);
        if (existingCheckpoint is null)
        {
            Log.Warning("[import] --resume specified but no checkpoint found; starting fresh.");
        }
        else
        {
            completedFiles = new HashSet<string>(existingCheckpoint.CompletedFiles, StringComparer.OrdinalIgnoreCase);
            Log.Information("[import] Resuming import job {JobId} — {N} file(s) already done.",
                existingCheckpoint.JobId, completedFiles.Count);
        }
    }

    // ── Resolve download directory ────────────────────────────────────────────
    string downloadDir;
    bool   cleanupDir = false;
    IReadOnlyList<TableExportInfo>? tableInfos = null;
    var jobId = opts.JobId;

    if (opts.Resume && existingCheckpoint is not null && string.IsNullOrEmpty(opts.Dir) && string.IsNullOrEmpty(jobId))
    {
        // Auto-recover dir and jobId from the saved checkpoint.
        jobId       = existingCheckpoint.JobId;
        downloadDir = existingCheckpoint.DownloadDir;
        cleanupDir  = false;   // keep files so we can resume again if needed
        tableInfos  = await ImportService.TryLoadTableExportInfoAsync(tracker, ct);
        Log.Information("[import] Resuming from: {Dir}", downloadDir);

        if (!Directory.Exists(downloadDir))
        {
            Log.Fatal("[import] Resume failed — download directory no longer exists: {Dir}", downloadDir);
            return false;
        }
    }
    else if (!string.IsNullOrEmpty(opts.Dir))
    {
        cleanupDir  = false;
        downloadDir = opts.Dir;
        tableInfos  = await ImportService.TryLoadTableExportInfoAsync(tracker, ct);
        Log.Information("[import] Using existing directory: {Dir}", downloadDir);
    }
    else if (!opts.SchemaOnly)
    {
        var request = new ExportJobCreateRequest
        {
            Tables      = opts.Only.Length        > 0 ? opts.Only        : null,
            CompanyIds  = opts.CompanyIds.Length  > 0 ? opts.CompanyIds  : null,
            LocationIds = opts.LocationIds.Length > 0 ? opts.LocationIds : null,
            TimeOffset  = opts.TimeOffset,
        };

        if (!string.IsNullOrEmpty(jobId))
        {
            Log.Information("[import] Reusing export job: {JobId}", jobId);
        }
        else
        {
            Log.Information("[import] Requesting export...");
            jobId = await ImportService.CreateExportJobAsync(apiUrl, opts.ApiKey, request, ct);
            Log.Information("[import] Export job created: {JobId}", jobId);
        }

        Log.Information("[import] Waiting for export to complete...");
        var exportLogger = loggerFactory.CreateLogger("import.export");
        var job = await ImportService.PollUntilCompleteAsync(apiUrl, opts.ApiKey, jobId!, exportLogger, ct);

        downloadDir = Path.Combine(opts.DataDir, $"import-{jobId}");
        Directory.CreateDirectory(downloadDir);
        cleanupDir = !opts.NoCleanup;

        Log.Information("[import] Downloading export data...");
        tableInfos = await ImportService.BulkDownloadAsync(job, downloadDir, exportLogger, ct);
    }
    else
    {
        downloadDir = Path.Combine(opts.DataDir, "import-schema-only");
        Directory.CreateDirectory(downloadDir);
    }

    // ── Initialise / update checkpoint record ─────────────────────────────────
    // Save a checkpoint record as soon as the download dir is known so that a
    // crash during the import phase can be resumed with `eds import --resume`.
    var checkpointKey = ImportService.CheckpointTrackerKey;
    if (!opts.DryRun && !opts.SchemaOnly && !string.IsNullOrEmpty(jobId))
    {
        var checkpoint = new ImportCheckpoint
        {
            JobId          = jobId!,
            DownloadDir    = downloadDir,
            CompletedFiles = [..completedFiles],
            StartedAt      = existingCheckpoint?.StartedAt ?? DateTimeOffset.UtcNow,
        };
        await ImportService.SaveCheckpointAsync(tracker, checkpoint, ct);
    }

    // ── Derive tables list from download info, filtered by --only ─────────────
    IReadOnlyList<string> importTables;
    if (tableInfos is { Count: > 0 })
    {
        importTables = tableInfos
            .Select(t => t.Table)
            .Where(t => opts.Only.Length == 0 || opts.Only.Contains(t, StringComparer.OrdinalIgnoreCase))
            .ToList();
    }
    else
    {
        importTables = opts.Only;
    }

    if (importTables.Count > 0)
        Log.Information("[import] Importing data to tables: {Tables}", string.Join(", ", importTables));

    // ── Run importer ──────────────────────────────────────────────────────────
    if (directImporter is not null)
    {
        // Storage drivers (File, S3): transfer raw .ndjson.gz files as-is.
        var directFiles = new List<(string Table, string FilePath)>();
        foreach (var f in Directory.EnumerateFiles(downloadDir, "*.ndjson*", SearchOption.AllDirectories))
        {
            if (!EDS.Importer.CrdbFileParser.TryParse(Path.GetFileName(f), out var table, out _))
                continue;
            if (importTables.Count > 0 && !importTables.Contains(table, StringComparer.OrdinalIgnoreCase))
                continue;
            directFiles.Add((table, f));
        }

        Log.Information("[import] Direct transfer: {Count} file(s) to {Driver}.", directFiles.Count, scheme);
        if (!opts.DryRun)
            await directImporter.ImportFilesAsync(importLogger, directFiles, ct);
    }
    else
    {
        // Database drivers: parse and upsert row by row.
        var importerConfig = new EDS.Importer.ImporterConfig
        {
            DataDir        = downloadDir,
            SchemaRegistry = schemaRegistry,
            Tables         = importTables,
            DryRun         = opts.DryRun,
            Single         = opts.Single,
            SchemaOnly     = opts.SchemaOnly,
            NoDelete       = opts.NoDelete || opts.Resume,
            JobId          = jobId,
            MaxParallel    = opts.Parallel,
            Logger         = importLogger,
            Tracker        = (!opts.DryRun && !opts.SchemaOnly && !string.IsNullOrEmpty(jobId))
                                 ? tracker : null,
            CheckpointKey  = checkpointKey,
            CompletedFiles = completedFiles,
        };

        var importer = new EDS.Importer.NdjsonGzImporter(importerConfig);
        await importer.RunAsync(importDriver!, ct);
    }

    // ── Set table versions in registry ────────────────────────────────────────
    if (!opts.DryRun && !opts.SchemaOnly && tableInfos is not null)
    {
        var schemaMap = await schemaRegistry.GetLatestSchemaAsync(ct);
        foreach (var info in tableInfos)
        {
            if (importTables.Count > 0 && !importTables.Contains(info.Table, StringComparer.OrdinalIgnoreCase))
                continue;
            if (schemaMap.TryGetValue(info.Table, out var tableSchema))
                await schemaRegistry.SetTableVersionAsync(info.Table, tableSchema.ModelVersion, ct);
        }
        await ImportService.SaveTableExportInfoAsync(tracker, tableInfos, ct);
        Log.Information("[import] Table versions saved for {Count} table(s).", tableInfos.Count);
    }

    // ── Clear checkpoint after a successful full import ───────────────────────
    // A completed import doesn't need to be resumed, so remove the checkpoint
    // to avoid accidentally resuming a stale run next time.
    if (!opts.DryRun && !opts.SchemaOnly && !string.IsNullOrEmpty(jobId))
        await tracker.DeleteKeyAsync(checkpointKey, ct);

    // ── Cleanup ───────────────────────────────────────────────────────────────
    if (cleanupDir && Directory.Exists(downloadDir))
    {
        Directory.Delete(downloadDir, recursive: true);
        Log.Debug("[import] Cleaned up temp directory: {Dir}", downloadDir);
    }
    else if (!cleanupDir && !string.IsNullOrEmpty(opts.Dir) && Directory.Exists(downloadDir))
    {
        Log.Information("[import] Downloaded files saved to: {Dir}", downloadDir);
    }

    return true;
}

static Command BuildImportCommand(Option<FileInfo?> cfgOpt, Option<bool> verbOpt, Option<string> ddOpt)
{
    var urlOpt         = new Option<string?>("--url")          { Description = "Destination driver URL (falls back to 'url' in config.toml)" };
    var keyOpt         = new Option<string?>("--api-key")      { Description = "Shopmonkey API key (falls back to 'token' in config.toml)" };
    var jobIdOpt       = new Option<string?>("--job-id")      { Description = "Reuse an existing export job ID" };
    var dirOpt         = new Option<string?>("--dir")         { Description = "Path to already-downloaded export files (skips API export)" };
    var onlyOpt        = new Option<string[]>("--only")       { Description = "Comma-separated table names to import", AllowMultipleArgumentsPerToken = true };
    var companyOpt     = new Option<string[]>("--company-ids"){ Description = "Filter export by company IDs", AllowMultipleArgumentsPerToken = true };
    var locationOpt    = new Option<string[]>("--location-ids"){ Description = "Filter export by location IDs", AllowMultipleArgumentsPerToken = true };
    var timeOffsetOpt  = new Option<long?>("--time-offset")   { Description = "Time offset (nanoseconds) for change tracking cursor" };
    var parallelOpt    = new Option<int>("--parallel")        { Description = "Max parallel table goroutines", DefaultValueFactory = _ => 4 };
    var dryRunOpt      = new Option<bool>("--dry-run")        { Description = "Parse and validate but do not write any rows" };
    var noConfirmOpt   = new Option<bool>("--no-confirm")     { Description = "Skip the interactive delete confirmation" };
    var noCleanupOpt   = new Option<bool>("--no-cleanup")     { Description = "Keep the temporary download directory after import" };
    var noDeleteOpt    = new Option<bool>("--no-delete")      { Description = "Do not drop and recreate tables; only insert rows" };
    var schemaOnlyOpt  = new Option<bool>("--schema-only")    { Description = "Create tables only; do not import any rows" };
    var singleOpt      = new Option<bool>("--single")         { Description = "Process one table at a time (no parallelism)" };
    var resumeOpt      = new Option<bool>("--resume")         { Description = "Resume the last interrupted import from the first unfinished file (implies --no-delete --no-cleanup)" };

    var cmd = new Command("import") { Description = "Import a snapshot of Shopmonkey data." };
    cmd.Options.Add(cfgOpt);
    cmd.Options.Add(verbOpt);
    cmd.Options.Add(ddOpt);
    cmd.Options.Add(urlOpt);
    cmd.Options.Add(keyOpt);
    cmd.Options.Add(jobIdOpt);
    cmd.Options.Add(dirOpt);
    cmd.Options.Add(onlyOpt);
    cmd.Options.Add(companyOpt);
    cmd.Options.Add(locationOpt);
    cmd.Options.Add(timeOffsetOpt);
    cmd.Options.Add(parallelOpt);
    cmd.Options.Add(dryRunOpt);
    cmd.Options.Add(noConfirmOpt);
    cmd.Options.Add(noCleanupOpt);
    cmd.Options.Add(noDeleteOpt);
    cmd.Options.Add(schemaOnlyOpt);
    cmd.Options.Add(singleOpt);
    cmd.Options.Add(resumeOpt);

    cmd.SetAction(async (parseResult, ct) =>
    {
        var cfg        = parseResult.GetValue(cfgOpt);
        var verbose    = parseResult.GetValue(verbOpt);
        var dataDir    = parseResult.GetValue(ddOpt) ?? Path.Combine(Directory.GetCurrentDirectory(), "data");
        var urlArg     = parseResult.GetValue(urlOpt);
        var keyArg     = parseResult.GetValue(keyOpt);

        Directory.CreateDirectory(dataDir);
        var configPath = cfg?.FullName ?? Path.Combine(dataDir, "config.toml");

        // ── Resolve url + api-key: args take precedence, then fall back to config ──
        var fileConfig = new ConfigurationBuilder()
            .AddTomlFile(configPath, optional: true)
            .AddEnvironmentVariables("EDS_")
            .Build();

        var driverUrl = urlArg ?? fileConfig["url"];
        var apiKey    = keyArg ?? fileConfig["token"];

        if (string.IsNullOrEmpty(driverUrl))
        {
            Log.Fatal("[import] No driver URL provided. Pass --url or set 'url' in {Config}.", configPath);
            return;
        }
        if (string.IsNullOrEmpty(apiKey))
        {
            Log.Fatal("[import] No API key provided. Pass --api-key or set 'token' in {Config}.", configPath);
            return;
        }

        var tracker = new EDS.Infrastructure.Tracking.SqliteTracker(Path.Combine(dataDir, "state.db"));

        var resume = parseResult.GetValue(resumeOpt);
        var opts = new ImportRunOptions
        {
            DataDir     = dataDir,
            ConfigPath  = configPath,
            Verbose     = verbose,
            DriverUrl   = driverUrl,
            ApiKey      = apiKey,
            JobId       = parseResult.GetValue(jobIdOpt),
            Dir         = parseResult.GetValue(dirOpt),
            Only        = parseResult.GetValue(onlyOpt) ?? [],
            CompanyIds  = parseResult.GetValue(companyOpt) ?? [],
            LocationIds = parseResult.GetValue(locationOpt) ?? [],
            TimeOffset  = parseResult.GetValue(timeOffsetOpt),
            Parallel    = parseResult.GetValue(parallelOpt),
            DryRun      = parseResult.GetValue(dryRunOpt),
            NoConfirm   = parseResult.GetValue(noConfirmOpt),
            NoCleanup   = parseResult.GetValue(noCleanupOpt) || resume,
            NoDelete    = parseResult.GetValue(noDeleteOpt),
            SchemaOnly  = parseResult.GetValue(schemaOnlyOpt),
            Single      = parseResult.GetValue(singleOpt),
            Resume      = resume,
        };

        if (!await RunImportPipelineAsync(opts, tracker, ct)) return;

        // ── Persist credentials to config so RunServerAsync can read them ─────
        await PersistConfigValueAsync(configPath, "token", apiKey);
        await PersistConfigValueAsync(configPath, "url", driverUrl);

        // ── Transition to server ──────────────────────────────────────────────
        Log.Information("[import] Import complete. Starting server...");
        await RunServerAsync(configPath, dataDir, verbose, tracker, ct);
    });

    return cmd;
}

static Command BuildVersionCommand()
{
    var cmd = new Command("version") { Description = "Print the EDS version." };
    cmd.SetAction(_ => Console.WriteLine($"EDS version {EdsVersion.Current}"));
    return cmd;
}

static Command BuildEnrollCommand(Option<FileInfo?> cfgOpt)
{
    var keyOpt = new Option<string>("--api-key") { Description = "Shopmonkey API key (JWT token)", Required = true };
    var sidOpt = new Option<string?>("--server-id") { Description = "Override server ID (optional)" };
    var cmd = new Command("enroll") { Description = "Save API credentials to config without an enrollment code." };
    cmd.Options.Add(keyOpt);
    cmd.Options.Add(sidOpt);
    cmd.Options.Add(cfgOpt);
    cmd.SetAction(async (parseResult, ct) =>
    {
        var key     = parseResult.GetValue(keyOpt);
        var sid     = parseResult.GetValue(sidOpt);
        var cfg     = parseResult.GetValue(cfgOpt);
        var dataDir = Path.Combine(Directory.GetCurrentDirectory(), "data");
        Directory.CreateDirectory(dataDir);
        var configPath = cfg?.FullName ?? Path.Combine(dataDir, "config.toml");

        await ConfigFileHelper.SetValueAsync(configPath, "token", key);
        if (!string.IsNullOrEmpty(sid))
            await ConfigFileHelper.SetValueAsync(configPath, "server_id", sid);

        Log.Information("[enroll] Credentials saved to {ConfigPath}.", configPath);
        Log.Information("[enroll] Run 'eds server' to start the streaming server.");
    });
    return cmd;
}

static Command BuildDriverCommand()
{
    var cmd = new Command("driver") { Description = "Driver management." };

    var listCmd = new Command("list") { Description = "List available drivers." };
    listCmd.SetAction(_ =>
    {
        Console.WriteLine("Available drivers:");
        foreach (var m in BuildDriverRegistry().GetAllMetadata())
            Console.WriteLine($"  {m.Scheme,-15} {m.Name} — {m.Description}");
    });

    var drv = new Argument<string>("driver") { Description = "Driver scheme" };
    var helpCmd = new Command("help") { Description = "Driver-specific help." };
    helpCmd.Arguments.Add(drv);
    helpCmd.SetAction(parseResult =>
    {
        var d = parseResult.GetValue(drv);
        var m = BuildDriverRegistry().GetAllMetadata().FirstOrDefault(x => x.Scheme.Equals(d, StringComparison.OrdinalIgnoreCase));
        if (m is null) { Console.Error.WriteLine($"Unknown driver: {d}"); return; }
        Console.WriteLine($"{m.Name}\n  {m.Description}\n  Example: {m.ExampleUrl}\n\n{m.Help}");
    });

    cmd.Subcommands.Add(listCmd);
    cmd.Subcommands.Add(helpCmd);
    return cmd;
}

static Command BuildPublicKeyCommand()
{
    var cmd = new Command("publickey") { Description = "Print the Shopmonkey PGP public key." };
    cmd.SetAction(_ => Console.WriteLine(EdsVersion.ShopmonkeyPublicPgpKey));
    return cmd;
}

static EDS.Infrastructure.Notification.NotificationHandlers BuildNotificationHandlers(
    string sessionId,
    string configPath,
    string dataDir,
    bool verbose,
    string driverUrl,
    string apiKey,
    EDS.Infrastructure.Tracking.SqliteTracker tracker,
    DriverRegistry registry,
    CancellationTokenSource backgroundImportCts)
{
    // Prevents concurrent imports (Configure+backfill and Import notifications could
    // arrive close together). If an import is already running, subsequent requests skip.
    var importSemaphore = new SemaphoreSlim(1, 1);

    return new EDS.Infrastructure.Notification.NotificationHandlers
    {
        // ── driverconfig: return full catalog of drivers + field definitions ──
        DriverConfig = () =>
        {
            var configs = registry.GetConfigurations();
            var drivers = configs.ToDictionary(
                kvp => kvp.Key,
                kvp => new EDS.Infrastructure.Notification.DriverConfiguratorDto
                {
                    Metadata = new EDS.Infrastructure.Notification.DriverMetadataDto
                    {
                        Scheme            = kvp.Value.Metadata.Scheme,
                        Name              = kvp.Value.Metadata.Name,
                        Description       = kvp.Value.Metadata.Description,
                        ExampleUrl        = kvp.Value.Metadata.ExampleUrl,
                        Help              = kvp.Value.Metadata.Help,
                        SupportsImport    = kvp.Value.Metadata.SupportsImport,
                        SupportsMigration = kvp.Value.Metadata.SupportsMigration,
                    },
                    Fields = kvp.Value.Fields.Select(f => new EDS.Infrastructure.Notification.DriverFieldDto
                    {
                        Name        = f.Name,
                        Type        = f.Type.ToString().ToLowerInvariant(),
                        Format      = f.Format == EDS.Core.Abstractions.DriverFieldFormat.Password ? "password" : "",
                        Default     = f.Default,
                        Description = f.Description,
                        Required    = f.Required,
                    }).ToList()
                });
            return new EDS.Infrastructure.Notification.DriverConfigNotificationResponse
            {
                SessionId = sessionId,
                Drivers   = drivers
            };
        },

        // ── validate: validate driver config fields, return URL or field errors ──
        Validate = (driver, config) =>
        {
            var (url, errors) = registry.Validate(driver, config);
            return new EDS.Infrastructure.Notification.ValidateNotificationResponse
            {
                Success     = errors.Count == 0 && !string.IsNullOrEmpty(url),
                SessionId   = sessionId,
                Url         = url,
                FieldErrors = errors.Select(e => new EDS.Infrastructure.Notification.FieldErrorDto
                    { Field = e.Field, Error = e.Message }).ToList()
            };
        },

        // ── configure: validate URL, persist to config.toml; trigger import if backfill=true ──
        Configure = async (url, backfill) =>
        {
            try
            {
                var scheme = new Uri(url).Scheme;
                var driver = registry.Resolve(scheme);
                if (driver is null)
                    return new EDS.Infrastructure.Notification.ConfigureNotificationResponse
                    {
                        Success   = false,
                        SessionId = sessionId,
                        Message   = $"No driver registered for scheme '{scheme}'.",
                        Backfill  = backfill
                    };

                // Persist url to config.toml
                await PersistConfigValueAsync(configPath, "url", url);
                var maskedUrl = SessionService.MaskUrl(url);

                // If the web UI requested backfill, kick off an import in the background.
                // The log path is pre-computed so we can include it in the response.
                string? importLogPath = null;
                if (backfill && !string.IsNullOrEmpty(apiKey))
                {
                    if (!importSemaphore.Wait(0))
                    {
                        Log.Warning("[server] Configure+backfill received but an import is already in progress. Skipping.");
                    }
                    else
                    {
                        importLogPath = Path.Combine(dataDir, $"import-{DateTime.UtcNow:yyyyMMdd-HHmmss}.log");
                        Log.Information("[server] Configure+backfill received from HQ. Triggering import (log: {LogFile}).", importLogPath);
                        var savedLogger = Log.Logger;
                        var importOpts = new ImportRunOptions
                        {
                            DataDir    = dataDir,
                            ConfigPath = configPath,
                            Verbose    = verbose,
                            DriverUrl  = url,
                            ApiKey     = apiKey,
                            NoConfirm  = true,
                            LogFile    = importLogPath,
                        };
                        _ = Task.Run(async () =>
                        {
                            try
                            {
                                await RunImportPipelineAsync(importOpts, tracker, backgroundImportCts.Token);
                            }
                            catch (OperationCanceledException)
                            {
                                Log.Warning("[server] Import (Configure+backfill) was cancelled on shutdown.");
                            }
                            catch (Exception ex)
                            {
                                Log.Error(ex, "[server] Import triggered by Configure+backfill failed.");
                            }
                            finally
                            {
                                Log.Logger = savedLogger;
                                importSemaphore.Release();
                            }
                        });
                    }
                }

                return new EDS.Infrastructure.Notification.ConfigureNotificationResponse
                {
                    Success   = true,
                    SessionId = sessionId,
                    MaskedUrl = maskedUrl,
                    Backfill  = backfill,
                    LogPath   = importLogPath,
                };
            }
            catch (Exception ex)
            {
                return new EDS.Infrastructure.Notification.ConfigureNotificationResponse
                {
                    Success   = false,
                    SessionId = sessionId,
                    Message   = ex.Message,
                    Backfill  = backfill
                };
            }
        },

        // ── sendlogs: HQ requests log files for remote diagnostics ───────────────
        SendLogs = async () =>
        {
            Log.Information("[sendlogs] Log upload requested by HQ.");
            var currentApiKey = new ConfigurationBuilder()
                .AddTomlFile(configPath, optional: true)
                .AddEnvironmentVariables("EDS_")
                .Build()["token"] ?? apiKey;
            var currentApiUrl = SessionService.GetApiUrlFromJwt(currentApiKey);
            var (success, error) = await SessionService.SendLogsAsync(
                currentApiUrl, currentApiKey, sessionId, dataDir, CancellationToken.None);
            return new EDS.Infrastructure.Notification.SendLogsResponse(success, error);
        },

        // ── import: triggered directly from the Shopmonkey web UI ────────────────
        // Runs without any console interaction (NoConfirm = true) — the server may
        // be running headless. Config is re-read at invocation time so that a URL
        // configured via a Configure notification after server start is picked up.
        Import = async (backfill) =>
        {
            // Re-read config: the driver URL may have been set via a Configure
            // notification after this handler was registered.
            var currentConfig = new ConfigurationBuilder()
                .AddTomlFile(configPath, optional: true)
                .AddEnvironmentVariables("EDS_")
                .Build();
            var currentDriverUrl = currentConfig["url"]   ?? string.Empty;
            var currentApiKey    = currentConfig["token"] ?? string.Empty;

            if (string.IsNullOrEmpty(currentDriverUrl) || string.IsNullOrEmpty(currentApiKey))
            {
                Log.Warning("[server] Import notification received from HQ but driver is not configured. Skipping.");
                return;
            }

            if (!importSemaphore.Wait(0))
            {
                Log.Warning("[server] Import notification received but an import is already in progress. Skipping.");
                return;
            }

            Log.Information("[server] Import command received from Shopmonkey HQ (backfill={Backfill}). Starting import...", backfill);
            var savedLogger = Log.Logger;
            try
            {
                var importOpts = new ImportRunOptions
                {
                    DataDir    = dataDir,
                    ConfigPath = configPath,
                    Verbose    = verbose,
                    DriverUrl  = currentDriverUrl,
                    ApiKey     = currentApiKey,
                    NoConfirm  = true,
                };
                await RunImportPipelineAsync(importOpts, tracker, backgroundImportCts.Token);
            }
            catch (OperationCanceledException)
            {
                Log.Warning("[server] Import triggered by HQ notification was cancelled on shutdown.");
            }
            catch (Exception ex)
            {
                Log.Error(ex, "[server] Import triggered by HQ notification failed.");
            }
            finally
            {
                Log.Logger = savedLogger;
                importSemaphore.Release();
            }
        }
    };
}

static Task PersistConfigValueAsync(string configPath, string key, string value) =>
    ConfigFileHelper.SetValueAsync(configPath, key, value);

/// <summary>Returns the runtime identifier used to select the correct EDS binary download.</summary>
static string GetPlatformId() =>
    (RuntimeInformation.IsOSPlatform(OSPlatform.Windows),
     RuntimeInformation.IsOSPlatform(OSPlatform.OSX),
     RuntimeInformation.OSArchitecture) switch
    {
        (true, _, _)                      => "win-x64",
        (_, true, Architecture.Arm64)     => "osx-arm64",
        (_, true, _)                      => "osx-x64",
        (_, _, Architecture.Arm64)        => "linux-arm64",
        _                                 => "linux-x64",
    };

static DriverRegistry BuildDriverRegistry()
{
    var r = new DriverRegistry();
    r.Register("postgres",   new PostgreSqlDriver());
    r.Register("mysql",      new MySqlDriver());
    r.Register("sqlserver",  new SqlServerDriver());
    r.Register("snowflake",  new SnowflakeDriver());
    r.Register("s3",          new S3Driver());
    r.Register("azureblob",  new AzureBlobDriver());
    r.Register("kafka",      new KafkaDriver());
    r.Register("eventhub",   new EventHubDriver());
    r.Register("file",       new FileDriver());
    return r;
}
