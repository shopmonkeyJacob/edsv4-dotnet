using System.Text.Json.Serialization;

namespace EDS.Core.Models;

public sealed class ItemsType
{
    [JsonPropertyName("type")]
    public string Type { get; init; } = string.Empty;

    [JsonPropertyName("enum")]
    public string[]? Enum { get; init; }

    [JsonPropertyName("format")]
    public string? Format { get; init; }
}

public sealed class SchemaProperty
{
    [JsonPropertyName("type")]
    public string Type { get; init; } = string.Empty;

    [JsonPropertyName("format")]
    public string? Format { get; init; }

    [JsonPropertyName("nullable")]
    public bool Nullable { get; init; }

    [JsonPropertyName("items")]
    public ItemsType? Items { get; init; }

    [JsonPropertyName("additionalProperties")]
    public bool? AdditionalProperties { get; init; }

    [JsonPropertyName("$comment")]
    public string? Comment { get; init; }

    [JsonPropertyName("deprecated")]
    public bool? Deprecated { get; init; }

    public bool IsNotNull => !Nullable || Type == "array";
    public bool IsArrayOrJson => Type is "object" or "array";
}

public sealed class Schema
{
    [JsonPropertyName("properties")]
    public Dictionary<string, SchemaProperty> Properties { get; init; } = new();

    [JsonPropertyName("required")]
    public List<string> Required { get; init; } = new();

    [JsonPropertyName("primaryKeys")]
    public List<string> PrimaryKeys { get; init; } = new();

    [JsonPropertyName("table")]
    public string Table { get; init; } = string.Empty;

    [JsonPropertyName("modelVersion")]
    public string ModelVersion { get; init; } = string.Empty;

    private List<string>? _columns;

    /// <summary>
    /// Returns the ordered column list: primary keys first, then remaining columns alphabetically.
    /// </summary>
    public IReadOnlyList<string> Columns()
    {
        if (_columns is not null)
            return _columns;

        var nonPk = Properties.Keys
            .Where(name => !PrimaryKeys.Contains(name))
            .OrderBy(name => name)
            .ToList();

        _columns = [.. PrimaryKeys, .. nonPk];
        return _columns;
    }
}

/// <summary>Map of table name → Schema.</summary>
public sealed class SchemaMap : Dictionary<string, Schema>
{
}

/// <summary>Map of table name → (column name → column type).</summary>
public sealed class DatabaseSchema : Dictionary<string, Dictionary<string, string>>
{
    public IReadOnlyList<string> GetColumns(string table)
    {
        if (TryGetValue(table, out var cols))
            return cols.Keys.OrderBy(c => c).ToList();
        return [];
    }

    public (bool found, string type) GetColumnType(string table, string column)
    {
        if (TryGetValue(table, out var cols) && cols.TryGetValue(column, out var type))
            return (true, type);
        return (false, string.Empty);
    }
}
