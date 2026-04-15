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
    private readonly Dictionary<string, Func<IDriver>> _factories = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, DriverMetadata> _metadata = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, string> _aliases = new(StringComparer.OrdinalIgnoreCase);

    public void Register(string scheme, Func<IDriver> factory)
    {
        _factories[scheme] = factory;

        // Instantiate a temporary instance for metadata introspection, then discard it.
        var probe = factory();
        _metadata[scheme] = BuildMetadata(scheme, probe);

        if (probe is IDriverAlias aliased)
        {
            foreach (var alias in aliased.Aliases)
                _aliases[alias] = scheme;
        }
    }

    public IDriver? Resolve(string scheme)
    {
        if (_factories.TryGetValue(scheme, out var factory))
            return factory();
        if (_aliases.TryGetValue(scheme, out var canonical) && _factories.TryGetValue(canonical, out factory))
            return factory();
        return null;
    }

    public IReadOnlyList<DriverMetadata> GetAllMetadata()
    {
        return _metadata.Values.ToList();
    }

    public DriverMetadata? GetMetadataForUrl(string urlString)
    {
        if (!Uri.TryCreate(urlString, UriKind.Absolute, out var uri))
            return null;
        var scheme = ResolveScheme(uri.Scheme);
        return scheme is not null && _metadata.TryGetValue(scheme, out var meta) ? meta : null;
    }

    public Dictionary<string, DriverConfigurator> GetConfigurations()
    {
        return _factories.ToDictionary(
            kvp => kvp.Key,
            kvp =>
            {
                var driver = kvp.Value();
                return new DriverConfigurator
                {
                    Metadata = _metadata[kvp.Key],
                    Fields = driver.Configuration()
                };
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

    private string? ResolveScheme(string scheme)
    {
        if (_factories.ContainsKey(scheme))
            return scheme;
        if (_aliases.TryGetValue(scheme, out var canonical) && _factories.ContainsKey(canonical))
            return canonical;
        return null;
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
            SupportsImport = driver is IDriverImport or IDriverDirectImport,
            SupportsMigration = driver is IDriverMigration
        };
    }
}
