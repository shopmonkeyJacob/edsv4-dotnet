using EDS.Core.Abstractions;
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

namespace EDS.Cli;

internal static class ServiceRegistration
{
    public static DriverRegistry BuildDriverRegistry()
    {
        var r = new DriverRegistry();
        r.Register("postgres",   () => new PostgreSqlDriver());
        r.Register("mysql",      () => new MySqlDriver());
        r.Register("sqlserver",  () => new SqlServerDriver());
        r.Register("snowflake",  () => new SnowflakeDriver());
        r.Register("s3",         () => new S3Driver());
        r.Register("azureblob",  () => new AzureBlobDriver());
        r.Register("kafka",      () => new KafkaDriver());
        r.Register("eventhub",   () => new EventHubDriver());
        r.Register("file",       () => new FileDriver());
        return r;
    }

    public static DriverMode ParseDriverMode(string? value) =>
        value?.ToLowerInvariant() switch
        {
            "timeseries" or "time-series" or "time_series" => DriverMode.TimeSeries,
            "upsert"                                       => DriverMode.Upsert,
            _                                              => DriverMode.TimeSeries,
        };

    public static async Task<DriverMode> ResolveDriverModeAsync(
        string? flagValue,
        string? storedValue,
        string configPath,
        bool noConfirm = false,
        CancellationToken ct = default)
    {
        if (flagValue is null)
            return ParseDriverMode(storedValue);

        var flagMode       = ParseDriverMode(flagValue);
        var flagNormalized = flagMode == DriverMode.TimeSeries ? "timeseries" : "upsert";

        if (storedValue is not null && storedValue != flagNormalized)
        {
            if (!noConfirm)
            {
                Serilog.Log.Warning(
                    "[config] Conflict: config.toml has driver_mode={Stored} but --driver-mode={Flag} was specified.",
                    storedValue, flagNormalized);
                var confirmed = Spectre.Console.AnsiConsole.Confirm(
                    $"config.toml has driver_mode=[bold]{storedValue}[/] but you passed " +
                    $"--driver-mode=[bold]{flagNormalized}[/]. Change it?",
                    defaultValue: false);
                if (!confirmed)
                {
                    Serilog.Log.Information("[config] Keeping existing driver_mode={Mode}.", storedValue);
                    return ParseDriverMode(storedValue);
                }
            }
            Serilog.Log.Information("[config] Updating driver_mode from {Old} to {New}.", storedValue, flagNormalized);
        }

        await ConfigFileHelper.SetValueAsync(configPath, "driver_mode", flagNormalized);
        return flagMode;
    }

    public static async Task<string> ResolveEventsSchemaAsync(
        string? flagValue,
        string? storedValue,
        string configPath,
        CancellationToken ct = default)
    {
        if (flagValue is null)
            return storedValue ?? "eds_events";

        if (flagValue != storedValue)
            await ConfigFileHelper.SetValueAsync(configPath, "events_schema", flagValue);

        return flagValue;
    }

    public static Task PersistConfigValueAsync(string configPath, string key, string value) =>
        ConfigFileHelper.SetValueAsync(configPath, key, value);

    public static string GetPlatformId() =>
        (System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows),
         System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.OSX),
         System.Runtime.InteropServices.RuntimeInformation.OSArchitecture) switch
        {
            (true, _, _)                                                              => "win-x64",
            (_, true, System.Runtime.InteropServices.Architecture.Arm64)             => "osx-arm64",
            (_, true, _)                                                              => "osx-x64",
            (_, _, System.Runtime.InteropServices.Architecture.Arm64)                => "linux-arm64",
            _                                                                         => "linux-x64",
        };
}
