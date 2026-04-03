using EDS.Core.Abstractions;
using EDS.Core.Models;
using EDS.Core.Registry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace EDS.Core.Tests;

public class DriverRegistryTests
{
    // ── Register / Resolve ────────────────────────────────────────────────────

    [Fact]
    public void Resolve_RegisteredScheme_ReturnsDriver()
    {
        var registry = new DriverRegistry();
        var driver   = new StubDriver();
        registry.Register("stub", driver);

        Assert.Same(driver, registry.Resolve("stub"));
    }

    [Fact]
    public void Resolve_UnknownScheme_ReturnsNull()
    {
        var registry = new DriverRegistry();
        Assert.Null(registry.Resolve("unknown"));
    }

    [Fact]
    public void Resolve_CaseInsensitive()
    {
        var registry = new DriverRegistry();
        registry.Register("MySQL", new StubDriver());

        Assert.NotNull(registry.Resolve("mysql"));
        Assert.NotNull(registry.Resolve("MYSQL"));
    }

    // ── Aliases ───────────────────────────────────────────────────────────────

    [Fact]
    public void Resolve_Alias_ReturnsCanonicalDriver()
    {
        var registry = new DriverRegistry();
        var driver   = new AliasedDriver();
        registry.Register("postgresql", driver);

        Assert.Same(driver, registry.Resolve("pg"));
        Assert.Same(driver, registry.Resolve("postgres"));
    }

    [Fact]
    public void Resolve_AliasIsCaseInsensitive()
    {
        var registry = new DriverRegistry();
        registry.Register("postgresql", new AliasedDriver());

        Assert.NotNull(registry.Resolve("PG"));
    }

    // ── GetAllMetadata ────────────────────────────────────────────────────────

    [Fact]
    public void GetAllMetadata_ReturnsOnePerRegisteredDriver()
    {
        var registry = new DriverRegistry();
        registry.Register("a", new StubDriver());
        registry.Register("b", new StubDriver());

        Assert.Equal(2, registry.GetAllMetadata().Count);
    }

    [Fact]
    public void GetAllMetadata_NamedDriver_UsesFriendlyName()
    {
        var registry = new DriverRegistry();
        registry.Register("named", new NamedDriver());

        var meta = registry.GetAllMetadata().Single();

        Assert.Equal("My Driver",     meta.Name);
        Assert.Equal("A test driver", meta.Description);
        Assert.Equal("named://host/db", meta.ExampleUrl);
    }

    [Fact]
    public void GetAllMetadata_StubDriver_FallsBackToScheme()
    {
        var registry = new DriverRegistry();
        registry.Register("stub", new StubDriver());

        Assert.Equal("stub", registry.GetAllMetadata().Single().Name);
    }

    [Fact]
    public void GetAllMetadata_MigrationDriver_SupportsMigrationTrue()
    {
        var registry = new DriverRegistry();
        registry.Register("mig", new MigrationDriver());

        Assert.True(registry.GetAllMetadata().Single().SupportsMigration);
    }

    [Fact]
    public void GetAllMetadata_PlainDriver_SupportsMigrationFalse()
    {
        var registry = new DriverRegistry();
        registry.Register("stub", new StubDriver());

        Assert.False(registry.GetAllMetadata().Single().SupportsMigration);
    }

    // ── GetMetadataForUrl ─────────────────────────────────────────────────────

    [Fact]
    public void GetMetadataForUrl_ValidUrl_ReturnsMetadata()
    {
        var registry = new DriverRegistry();
        registry.Register("stub", new StubDriver());

        var meta = registry.GetMetadataForUrl("stub://localhost/db");

        Assert.NotNull(meta);
        Assert.Equal("stub", meta!.Scheme);
    }

    [Fact]
    public void GetMetadataForUrl_UnknownScheme_ReturnsNull()
    {
        var registry = new DriverRegistry();
        Assert.Null(registry.GetMetadataForUrl("unknown://host/db"));
    }

    [Fact]
    public void GetMetadataForUrl_InvalidUrl_ReturnsNull()
    {
        var registry = new DriverRegistry();
        Assert.Null(registry.GetMetadataForUrl("not-a-url"));
    }

    // ── GetConfigurations ─────────────────────────────────────────────────────

    [Fact]
    public void GetConfigurations_IncludesAllRegisteredDrivers()
    {
        var registry = new DriverRegistry();
        registry.Register("a", new StubDriver());
        registry.Register("b", new StubDriver());

        var configs = registry.GetConfigurations();

        Assert.True(configs.ContainsKey("a"));
        Assert.True(configs.ContainsKey("b"));
    }

    // ── Validate ──────────────────────────────────────────────────────────────

    [Fact]
    public void Validate_ValidConfig_ReturnsUrl()
    {
        var registry = new DriverRegistry();
        registry.Register("test", new FieldValidatingDriver());

        var (url, errors) = registry.Validate("test", new() { ["host"] = "dbhost" });

        Assert.Empty(errors);
        Assert.Contains("dbhost", url);
    }

    [Fact]
    public void Validate_MissingRequiredField_ReturnsError()
    {
        var registry = new DriverRegistry();
        registry.Register("test", new FieldValidatingDriver());

        var (_, errors) = registry.Validate("test", []);

        Assert.Single(errors);
        Assert.Equal("host", errors[0].Field);
    }

    [Fact]
    public void Validate_UnknownScheme_Throws()
    {
        var registry = new DriverRegistry();

        Assert.Throws<InvalidOperationException>(() =>
            registry.Validate("unknown", []));
    }

    // ── CreateDriverAsync ─────────────────────────────────────────────────────

    [Fact]
    public async Task CreateDriverAsync_UnknownScheme_Throws()
    {
        var registry = new DriverRegistry();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            registry.CreateDriverAsync(
                "unknown://host/db",
                NullLogger.Instance,
                null!, null!, ""));
    }

    [Fact]
    public async Task CreateDriverAsync_LifecycleDriver_CallsStartAsync()
    {
        var registry = new DriverRegistry();
        var driver   = new LifecycleDriver();
        registry.Register("lc", driver);

        var returned = await registry.CreateDriverAsync(
            "lc://localhost/db",
            NullLogger.Instance,
            null!, null!, "/tmp");

        Assert.Same(driver, returned);
        Assert.NotNull(driver.ReceivedConfig);
        Assert.Equal("lc://localhost/db", driver.ReceivedConfig!.Url);
    }

    [Fact]
    public async Task CreateDriverAsync_NoLifecycle_ReturnsDriverDirectly()
    {
        var registry = new DriverRegistry();
        var driver   = new StubDriver();
        registry.Register("stub", driver);

        var returned = await registry.CreateDriverAsync(
            "stub://localhost/db",
            NullLogger.Instance,
            null!, null!, "");

        Assert.Same(driver, returned);
    }
}

// ── Stub driver implementations (file-scoped to avoid polluting other tests) ──

file sealed class StubDriver : IDriver
{
    public int MaxBatchSize => 100;
    public Task StopAsync(CancellationToken ct = default) => Task.CompletedTask;
    public Task<bool> ProcessAsync(ILogger logger, DbChangeEvent evt, CancellationToken ct = default) => Task.FromResult(false);
    public Task FlushAsync(ILogger logger, CancellationToken ct = default) => Task.CompletedTask;
    public Task TestAsync(ILogger logger, string url, CancellationToken ct = default) => Task.CompletedTask;
    public IReadOnlyList<DriverField> Configuration() => [];
    public (string url, IReadOnlyList<FieldError> errors) Validate(Dictionary<string, object?> values)
        => ("stub://localhost", []);
}

file sealed class NamedDriver : IDriver, IDriverHelp
{
    public int MaxBatchSize => 100;
    public string Name        => "My Driver";
    public string Description => "A test driver";
    public string ExampleUrl  => "named://host/db";
    public string Help        => "No help.";
    public Task StopAsync(CancellationToken ct = default) => Task.CompletedTask;
    public Task<bool> ProcessAsync(ILogger logger, DbChangeEvent evt, CancellationToken ct = default) => Task.FromResult(false);
    public Task FlushAsync(ILogger logger, CancellationToken ct = default) => Task.CompletedTask;
    public Task TestAsync(ILogger logger, string url, CancellationToken ct = default) => Task.CompletedTask;
    public IReadOnlyList<DriverField> Configuration() => [];
    public (string url, IReadOnlyList<FieldError> errors) Validate(Dictionary<string, object?> values)
        => ("named://localhost", []);
}

file sealed class AliasedDriver : IDriver, IDriverAlias
{
    public int MaxBatchSize => 100;
    public IReadOnlyList<string> Aliases => ["pg", "postgres"];
    public Task StopAsync(CancellationToken ct = default) => Task.CompletedTask;
    public Task<bool> ProcessAsync(ILogger logger, DbChangeEvent evt, CancellationToken ct = default) => Task.FromResult(false);
    public Task FlushAsync(ILogger logger, CancellationToken ct = default) => Task.CompletedTask;
    public Task TestAsync(ILogger logger, string url, CancellationToken ct = default) => Task.CompletedTask;
    public IReadOnlyList<DriverField> Configuration() => [];
    public (string url, IReadOnlyList<FieldError> errors) Validate(Dictionary<string, object?> values)
        => ("postgresql://localhost", []);
}

file sealed class MigrationDriver : IDriver, IDriverMigration
{
    public int MaxBatchSize => 100;
    public Task StopAsync(CancellationToken ct = default) => Task.CompletedTask;
    public Task<bool> ProcessAsync(ILogger logger, DbChangeEvent evt, CancellationToken ct = default) => Task.FromResult(false);
    public Task FlushAsync(ILogger logger, CancellationToken ct = default) => Task.CompletedTask;
    public Task TestAsync(ILogger logger, string url, CancellationToken ct = default) => Task.CompletedTask;
    public IReadOnlyList<DriverField> Configuration() => [];
    public (string url, IReadOnlyList<FieldError> errors) Validate(Dictionary<string, object?> values)
        => ("mig://localhost", []);
    public Task MigrateNewTableAsync(ILogger logger, Schema schema, CancellationToken ct = default)
        => Task.CompletedTask;
    public Task MigrateNewColumnsAsync(ILogger logger, Schema schema, IReadOnlyList<string> newColumns, CancellationToken ct = default)
        => Task.CompletedTask;
}

file sealed class LifecycleDriver : IDriver, IDriverLifecycle
{
    public int MaxBatchSize => 100;
    public DriverConfig? ReceivedConfig { get; private set; }
    public Task StopAsync(CancellationToken ct = default) => Task.CompletedTask;
    public Task<bool> ProcessAsync(ILogger logger, DbChangeEvent evt, CancellationToken ct = default) => Task.FromResult(false);
    public Task FlushAsync(ILogger logger, CancellationToken ct = default) => Task.CompletedTask;
    public Task TestAsync(ILogger logger, string url, CancellationToken ct = default) => Task.CompletedTask;
    public IReadOnlyList<DriverField> Configuration() => [];
    public (string url, IReadOnlyList<FieldError> errors) Validate(Dictionary<string, object?> values)
        => ("lc://localhost", []);
    public Task StartAsync(DriverConfig config, CancellationToken ct = default)
    {
        ReceivedConfig = config;
        return Task.CompletedTask;
    }
}

file sealed class FieldValidatingDriver : IDriver
{
    public int MaxBatchSize => -1;
    public Task StopAsync(CancellationToken ct = default) => Task.CompletedTask;
    public Task<bool> ProcessAsync(ILogger logger, DbChangeEvent evt, CancellationToken ct = default) => Task.FromResult(false);
    public Task FlushAsync(ILogger logger, CancellationToken ct = default) => Task.CompletedTask;
    public Task TestAsync(ILogger logger, string url, CancellationToken ct = default) => Task.CompletedTask;
    public IReadOnlyList<DriverField> Configuration() =>
    [
        new DriverField { Name = "host", Description = "Database host", Required = true },
    ];
    public (string url, IReadOnlyList<FieldError> errors) Validate(Dictionary<string, object?> values)
    {
        if (!values.TryGetValue("host", out var h) || h is null)
            return ("", [new FieldError { Field = "host", Message = "required" }]);
        return ($"test://{h}/db", []);
    }
}
