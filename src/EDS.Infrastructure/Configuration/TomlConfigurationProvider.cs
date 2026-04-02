using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.FileProviders;
using Tomlyn;
using Tomlyn.Model;

namespace EDS.Infrastructure.Configuration;

/// <summary>
/// Microsoft.Extensions.Configuration provider for TOML files.
/// Replaces Viper + BurntSushi/toml from the Go implementation.
/// </summary>
public sealed class TomlConfigurationProvider : FileConfigurationProvider
{
    public TomlConfigurationProvider(TomlConfigurationSource source) : base(source) { }

    public override void Load(Stream stream)
    {
        using var reader = new StreamReader(stream);
        var content = reader.ReadToEnd();
        var table = TomlSerializer.Deserialize<TomlTable>(content);
        Data = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        FlattenToml(table, string.Empty, Data);
    }

    private static void FlattenToml(TomlTable table, string prefix, IDictionary<string, string?> data)
    {
        foreach (var (rawKey, value) in table)
        {
            var key = prefix.Length > 0 ? $"{prefix}:{rawKey}" : rawKey;
            switch (value)
            {
                case TomlTable nested:
                    FlattenToml(nested, key, data);
                    break;
                case TomlArray arr:
                    for (int i = 0; i < arr.Count; i++)
                    {
                        var arrKey = $"{key}:{i}";
                        if (arr[i] is TomlTable arrTable)
                            FlattenToml(arrTable, arrKey, data);
                        else
                            data[arrKey] = arr[i]?.ToString();
                    }
                    break;
                default:
                    data[key] = value?.ToString();
                    break;
            }
        }
    }
}

public sealed class TomlConfigurationSource : FileConfigurationSource
{
    public override IConfigurationProvider Build(IConfigurationBuilder builder)
    {
        EnsureDefaults(builder);
        return new TomlConfigurationProvider(this);
    }
}

public static class TomlConfigurationExtensions
{
    public static IConfigurationBuilder AddTomlFile(
        this IConfigurationBuilder builder,
        string path,
        bool optional = false,
        bool reloadOnChange = false)
    {
        // Mirror AddJsonFile: when the path is absolute, create a PhysicalFileProvider
        // for the parent directory so the provider can locate the file correctly.
        IFileProvider? fileProvider = null;
        if (Path.IsPathRooted(path))
        {
            fileProvider = new PhysicalFileProvider(Path.GetDirectoryName(path)!);
            path = Path.GetFileName(path);
        }

        return builder.Add(new TomlConfigurationSource
        {
            FileProvider = fileProvider,
            Path = path,
            Optional = optional,
            ReloadOnChange = reloadOnChange
        });
    }
}
