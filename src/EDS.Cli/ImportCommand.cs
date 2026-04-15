using EDS.Core.Abstractions;
using Serilog;
using System.Text.Json;

namespace EDS.Cli;

internal static class ImportCommand
{
    public static async Task<bool> RunImportPipelineAsync(
        ImportRunOptions opts,
        EDS.Infrastructure.Tracking.SqliteTracker tracker,
        CancellationToken ct)
    {
        var importLogFile = opts.LogFile ?? Path.Combine(opts.DataDir, $"import-{DateTime.UtcNow:yyyyMMdd-HHmmss}.log");
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .Enrich.FromLogContext()
            .WriteTo.Console(
                restrictedToMinimumLevel: opts.Verbose
                    ? Serilog.Events.LogEventLevel.Debug
                    : Serilog.Events.LogEventLevel.Information,
                outputTemplate: ServerCommand.ConsoleTemplate)
            .WriteTo.File(
                importLogFile,
                restrictedToMinimumLevel: Serilog.Events.LogEventLevel.Debug,
                outputTemplate: ServerCommand.FileTemplate)
            .CreateLogger();

        Log.Debug("[import] Import log: {LogFile}", importLogFile);

        if (opts.IsPostEnrollment)
            Log.Information("[import] Running post-enrollment import (triggered by user confirmation).");

        var apiUrl = SessionService.GetApiUrlFromJwt(opts.ApiKey);
        Log.Information("[import] Using API url: {ApiUrl}", apiUrl);

        var driverRegistry = ServiceRegistration.BuildDriverRegistry();
        var scheme = new Uri(opts.DriverUrl).Scheme;
        var driver = driverRegistry.Resolve(scheme)
            ?? throw new InvalidOperationException($"No driver registered for scheme '{scheme}'.");

        var importDriver     = driver as IDriverImport;
        var directImporter   = driver as IDriverDirectImport;

        if (importDriver is null && directImporter is null)
            throw new InvalidOperationException(
                $"Driver '{scheme}' does not support import (implements neither IDriverImport nor IDriverDirectImport).");

        using var loggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(b => b.AddSerilog(Log.Logger));
        var schemaLogger  = new Microsoft.Extensions.Logging.Logger<EDS.Infrastructure.Schema.ApiSchemaRegistry>(loggerFactory);
        var schemaRegistry = await EDS.Infrastructure.Schema.ApiSchemaRegistry.CreateAsync(
            tracker, schemaLogger, apiUrl, EdsVersion.Current, ct);

        await schemaRegistry.ForceRefreshAsync(ct);

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

        if (importDriver is not null)
            await importDriver.InitForImportAsync(importLogger, schemaRegistry, opts.DriverUrl, opts.Mode, opts.EventsSchema, ct);
        else if (directImporter is not null)
            await directImporter.InitForDirectImportAsync(importLogger, opts.DriverUrl, ct);

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
                var completedFilesJson = await tracker.GetKeyAsync(ImportService.CompletedFilesTrackerKey, ct);
                if (completedFilesJson is not null)
                {
                    try
                    {
                        var saved = JsonSerializer.Deserialize<HashSet<string>>(completedFilesJson);
                        if (saved is not null)
                            completedFiles = new HashSet<string>(saved, StringComparer.OrdinalIgnoreCase);
                    }
                    catch { /* corrupt entry — start with empty set */ }
                }
                else
                {
                    completedFiles = new HashSet<string>(existingCheckpoint.CompletedFiles, StringComparer.OrdinalIgnoreCase);
                }

                Log.Information("[import] Resuming import job {JobId} — {N} file(s) already done.",
                    existingCheckpoint.JobId, completedFiles.Count);
            }
        }

        string downloadDir;
        bool   cleanupDir = false;
        IReadOnlyList<TableExportInfo>? tableInfos = null;
        var jobId = opts.JobId;

        if (opts.Resume && existingCheckpoint is not null && string.IsNullOrEmpty(opts.Dir) && string.IsNullOrEmpty(jobId))
        {
            jobId       = existingCheckpoint.JobId;
            downloadDir = existingCheckpoint.DownloadDir;
            cleanupDir  = false;

            if (!Directory.Exists(downloadDir))
            {
                Log.Warning("[import] Download directory missing — recreating: {Dir}", downloadDir);
                Directory.CreateDirectory(downloadDir);
            }

            Log.Information("[import] Resuming import job {JobId} from: {Dir}", jobId, downloadDir);

            var hasFiles = Directory.EnumerateFiles(downloadDir, "*.ndjson*", SearchOption.AllDirectories).Any();
            if (!hasFiles && !string.IsNullOrEmpty(jobId))
            {
                var exportLogger = loggerFactory.CreateLogger("import.export");
                Log.Information("[import] No downloaded files found — re-polling export job {JobId}...", jobId);
                tableInfos = await ImportService.PollDownloadWithRetryAsync(
                    apiUrl, opts.ApiKey, jobId!, downloadDir,
                    companyIds:  opts.CompanyIds.Length  > 0 ? opts.CompanyIds  : null,
                    locationIds: opts.LocationIds.Length > 0 ? opts.LocationIds : null,
                    exportLogger, ct: ct);
            }
            else
            {
                tableInfos = await ImportService.TryLoadTableExportInfoAsync(tracker, ct);
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

            downloadDir = Path.Combine(opts.DataDir, $"import-{jobId}");
            Directory.CreateDirectory(downloadDir);
            cleanupDir = !opts.NoCleanup;

            if (!opts.DryRun)
            {
                var earlyCheckpoint = new ImportCheckpoint
                {
                    JobId          = jobId!,
                    DownloadDir    = downloadDir,
                    CompletedFiles = [],
                    StartedAt      = DateTimeOffset.UtcNow,
                };
                await ImportService.SaveCheckpointAsync(tracker, earlyCheckpoint, ct);
            }

            Log.Information("[import] Waiting for export to complete...");
            var exportLogger = loggerFactory.CreateLogger("import.export");
            tableInfos = await ImportService.PollDownloadWithRetryAsync(
                apiUrl, opts.ApiKey, jobId!, downloadDir,
                companyIds:  opts.CompanyIds.Length  > 0 ? opts.CompanyIds  : null,
                locationIds: opts.LocationIds.Length > 0 ? opts.LocationIds : null,
                exportLogger, ct: ct);
        }
        else
        {
            downloadDir = Path.Combine(opts.DataDir, "import-schema-only");
            Directory.CreateDirectory(downloadDir);
        }

        var completedFilesKey = ImportService.CompletedFilesTrackerKey;
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

        if (directImporter is not null)
        {
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
                CheckpointKey  = completedFilesKey,
                CompletedFiles = completedFiles,
            };

            var importer = new EDS.Importer.NdjsonGzImporter(importerConfig);
            await importer.RunAsync(importDriver!, ct);
        }

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

        if (!opts.DryRun && !opts.SchemaOnly && !string.IsNullOrEmpty(jobId))
        {
            await tracker.DeleteKeyAsync(ImportService.CheckpointTrackerKey, ct);
            await tracker.DeleteKeyAsync(ImportService.CompletedFilesTrackerKey, ct);
        }

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
}
