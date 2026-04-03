using EDS.Core.Models;
using System.Text.Json;

namespace EDS.Integration.Tests.Helpers;

internal static class DriverTestHelpers
{
    /// <summary>Builds a minimal Schema for driver tests.</summary>
    public static Schema MakeSchema(
        string table,
        string[] primaryKeys,
        Dictionary<string, SchemaProperty> properties) =>
        new()
        {
            Table        = table,
            ModelVersion = "1",
            PrimaryKeys  = [.. primaryKeys],
            Required     = [.. primaryKeys],
            Properties   = properties,
        };

    /// <summary>Standard mixed-type schema covering all SQL type mapping paths.</summary>
    public static Schema OrdersSchema(string table = "eds_test_orders") => MakeSchema(
        table,
        ["id"],
        new Dictionary<string, SchemaProperty>
        {
            ["id"]        = new() { Type = "string",  Nullable = false },
            ["name"]      = new() { Type = "string",  Nullable = true  },
            ["amount"]    = new() { Type = "number",  Nullable = true  },
            ["qty"]       = new() { Type = "integer", Nullable = true  },
            ["active"]    = new() { Type = "boolean", Nullable = true  },
            ["createdAt"] = new() { Type = "string",  Nullable = true, Format = "date-time" },
            ["meta"]      = new() { Type = "object",  Nullable = true  },
            ["tags"]      = new() { Type = "array",   Nullable = true  },
        });

    /// <summary>Builds an INSERT DbChangeEvent from an anonymous object payload.</summary>
    public static DbChangeEvent MakeInsert(string table, string id, object payload, string? modelVersion = "1") =>
        new()
        {
            Operation    = "insert",
            Id           = Guid.NewGuid().ToString(),
            Table        = table,
            Key          = [id],
            ModelVersion = modelVersion ?? "1",
            After        = JsonDocument.Parse(JsonSerializer.Serialize(payload)).RootElement.Clone(),
        };

    /// <summary>Builds an UPDATE DbChangeEvent with an optional diff list.</summary>
    public static DbChangeEvent MakeUpdate(
        string table,
        string id,
        object payload,
        string[]? diff = null,
        string? modelVersion = "1") =>
        new()
        {
            Operation    = "update",
            Id           = Guid.NewGuid().ToString(),
            Table        = table,
            Key          = [id],
            ModelVersion = modelVersion ?? "1",
            After        = JsonDocument.Parse(JsonSerializer.Serialize(payload)).RootElement.Clone(),
            Diff         = diff,
        };

    /// <summary>Builds a DELETE DbChangeEvent.</summary>
    public static DbChangeEvent MakeDelete(string table, string id, string? modelVersion = "1") =>
        new()
        {
            Operation    = "delete",
            Id           = Guid.NewGuid().ToString(),
            Table        = table,
            Key          = ["", id],   // Key[0] is empty prefix, Key[^1] is the PK value
            ModelVersion = modelVersion ?? "1",
        };

    /// <summary>Builds a SchemaMap from a single schema (keyed by table name).</summary>
    public static SchemaMap ToSchemaMap(Schema schema)
    {
        var map = new SchemaMap();
        map[schema.Table] = schema;
        return map;
    }
}
