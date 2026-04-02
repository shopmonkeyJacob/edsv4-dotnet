using EDS.Core.Abstractions;
using Microsoft.Extensions.Logging;

namespace EDS.Core.Registry;

public sealed class DriverMetadata
{
    public required string Scheme { get; init; }
    public required string Name { get; init; }
    public string Description { get; init; } = string.Empty;
    public string ExampleUrl { get; init; } = string.Empty;
    public string Help { get; init; } = string.Empty;
    public bool SupportsImport { get; init; }
    public bool SupportsMigration { get; init; }
}

public sealed class DriverConfigurator
{
    public required DriverMetadata Metadata { get; init; }
    public required IReadOnlyList<DriverField> Fields { get; init; }
}

/// <summary>
/// Central registry for all driver implementations.
/// Mirrors the Go driverRegistry + driverAliasRegistry maps.
/// </summary>
public sealed class DriverRegistry
{
    private readonly Dictionary<string, IDriver> _drivers = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, string> _aliases = new(StringComparer.OrdinalIgnoreCase);

    public void Register(string scheme, IDriver driver)
    {
        _drivers[scheme] = driver;
        if (driver is IDriverAlias aliased)
        {
            foreach (var alias in aliased.Aliases)
                _aliases[alias] = scheme;
        }
    }

    public IDriver? Resolve(string scheme)
    {
        if (_drivers.TryGetValue(scheme, out var driver))
            return driver;
        if (_aliases.TryGetValue(scheme, out var canonical) && _drivers.TryGetValue(canonical, out driver))
            return driver;
        return null;
    }

    public IReadOnlyList<DriverMetadata> GetAllMetadata()
    {
        return _drivers.Select(kvp => BuildMetadata(kvp.Key, kvp.Value)).ToList();
    }

    public DriverMetadata? GetMetadataForUrl(string urlString)
    {
        if (!Uri.TryCreate(urlString, UriKind.Absolute, out var uri))
            return null;
        var driver = Resolve(uri.Scheme);
        return driver is null ? null : BuildMetadata(uri.Scheme, driver);
    }

    public Dictionary<string, DriverConfigurator> GetConfigurations()
    {
        return _drivers.ToDictionary(
            kvp => kvp.Key,
            kvp => new DriverConfigurator
            {
                Metadata = BuildMetadata(kvp.Key, kvp.Value),
                Fields = kvp.Value.Configuration()
            },
            StringComparer.OrdinalIgnoreCase);
    }

    public async Task<IDriver> CreateDriverAsync(
        string urlString,
        ILogger logger,
        ISchemaRegistry schemaRegistry,
        ITracker tracker,
        string dataDir,
        CancellationToken ct = default)
    {
        var uri = new Uri(urlString);
        var driver = Resolve(uri.Scheme)
            ?? throw new InvalidOperationException($"No driver registered for scheme '{uri.Scheme}'.");

        if (driver is IDriverLifecycle lifecycle)
        {
            await lifecycle.StartAsync(new DriverConfig
            {
                Url = urlString,
                Logger = logger,
                SchemaRegistry = schemaRegistry,
                Tracker = tracker,
                DataDir = dataDir
            }, ct);
        }

        return driver;
    }

    public (string url, IReadOnlyList<FieldError> errors) Validate(string scheme, Dictionary<string, object?> values)
    {
        var driver = Resolve(scheme)
            ?? throw new InvalidOperationException($"No driver registered for scheme '{scheme}'.");
        return driver.Validate(values);
    }

    private static DriverMetadata BuildMetadata(string scheme, IDriver driver)
    {
        var help = driver as IDriverHelp;
        return new DriverMetadata
        {
            Scheme = scheme,
            Name = help?.Name ?? scheme,
            Description = help?.Description ?? string.Empty,
            ExampleUrl = help?.ExampleUrl ?? string.Empty,
            Help = help?.Help ?? string.Empty,
            SupportsMigration = driver is IDriverMigration
        };
    }
}
