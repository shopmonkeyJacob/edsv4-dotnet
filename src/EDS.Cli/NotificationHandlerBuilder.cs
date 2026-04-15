using EDS.Core.Abstractions;
using EDS.Core.Registry;
using EDS.Infrastructure.Configuration;
using Microsoft.Extensions.Configuration;
using Serilog;

namespace EDS.Cli;

internal static class NotificationHandlerBuilder
{
    public static EDS.Infrastructure.Notification.NotificationHandlers Build(
        string sessionId,
        string configPath,
        string dataDir,
        bool verbose,
        string driverUrl,
        string apiKey,
        EDS.Infrastructure.Tracking.SqliteTracker tracker,
        DriverRegistry registry,
        CancellationTokenSource backgroundImportCts,
        DriverMode driverMode = DriverMode.TimeSeries,
        string eventsSchema = "eds_events",
        bool dryRun = false)
    {
        var importSemaphore = new SemaphoreSlim(1, 1);

        return new EDS.Infrastructure.Notification.NotificationHandlers
        {
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
                            Format      = f.Format == DriverFieldFormat.Password ? "password" : "",
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

            Configure = async (url, backfill) =>
            {
                if (dryRun)
                    return new EDS.Infrastructure.Notification.ConfigureNotificationResponse
                    {
                        Success   = false,
                        SessionId = sessionId,
                        Message   = "Cannot reconfigure driver in dry-run mode.",
                        Backfill  = backfill,
                    };

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

                    await ServiceRegistration.PersistConfigValueAsync(configPath, "url", url);
                    var maskedUrl = SessionService.MaskUrl(url);

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
                                DataDir      = dataDir,
                                ConfigPath   = configPath,
                                Verbose      = verbose,
                                DriverUrl    = url,
                                ApiKey       = apiKey,
                                NoConfirm    = true,
                                LogFile      = importLogPath,
                                Mode         = driverMode,
                                EventsSchema = eventsSchema,
                            };
                            _ = Task.Run(async () =>
                            {
                                try
                                {
                                    await ImportCommand.RunImportPipelineAsync(importOpts, tracker, backgroundImportCts.Token);
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

            SendLogs = async () =>
            {
                Log.Information("[sendlogs] Log upload requested by HQ.");
                var currentApiKey = new ConfigurationBuilder()
                    .AddTomlFile(configPath, optional: true)
                    .AddEnvironmentVariables("EDS_")
                    .Build()["token"] ?? apiKey;
                var currentApiUrl = SessionService.GetApiUrlFromJwt(currentApiKey);
                (bool success, string? error) = await SessionService.SendLogsAsync(
                    currentApiUrl, currentApiKey, sessionId, dataDir, CancellationToken.None);
                return new EDS.Infrastructure.Notification.SendLogsResponse(success, error);
            },

            Import = async (backfill) =>
            {
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
                        DataDir      = dataDir,
                        ConfigPath   = configPath,
                        Verbose      = verbose,
                        DriverUrl    = currentDriverUrl,
                        ApiKey       = currentApiKey,
                        NoConfirm    = true,
                        Mode         = driverMode,
                        EventsSchema = eventsSchema,
                    };
                    await ImportCommand.RunImportPipelineAsync(importOpts, tracker, backgroundImportCts.Token);
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
}
