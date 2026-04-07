using System.Text.Json;
using System.Text.Json.Serialization;
using MessagePack;

namespace EDS.Core.Models;

/// <summary>
/// Represents a change data capture event from the Shopmonkey database.
/// </summary>
[MessagePackObject]
public sealed class DbChangeEvent
{
    [Key("operation")]
    [JsonPropertyName("operation")]
    public required string Operation { get; init; }

    [Key("id")]
    [JsonPropertyName("id")]
    public required string Id { get; init; }

    [Key("table")]
    [JsonPropertyName("table")]
    public required string Table { get; init; }

    [Key("key")]
    [JsonPropertyName("key")]
    public required string[] Key { get; init; }

    [Key("modelVersion")]
    [JsonPropertyName("modelVersion")]
    public string? ModelVersion { get; init; }

    [Key("companyId")]
    [JsonPropertyName("companyId")]
    public string? CompanyId { get; init; }

    [Key("locationId")]
    [JsonPropertyName("locationId")]
    public string? LocationId { get; init; }

    [Key("userId")]
    [JsonPropertyName("userId")]
    public string? UserId { get; init; }

    [Key("before")]
    [JsonPropertyName("before")]
    public JsonElement? Before { get; init; }

    [Key("after")]
    [JsonPropertyName("after")]
    public JsonElement? After { get; init; }

    [Key("diff")]
    [JsonPropertyName("diff")]
    public string[]? Diff { get; init; }

    [Key("timestamp")]
    [JsonPropertyName("timestamp")]
    public long Timestamp { get; init; }

    [Key("mvccTimestamp")]
    [JsonPropertyName("mvccTimestamp")]
    public string? MvccTimestamp { get; init; }

    /// <summary>Not on the wire — set to true during bulk import.</summary>
    [IgnoreMember]
    [JsonPropertyName("imported")]
    public bool Imported { get; init; }

    /// <summary>The raw NATS message, used to Ack/Nak after processing.</summary>
    [IgnoreMember]
    [JsonIgnore]
    public object? NatsMessage { get; init; }

    /// <summary>Set by the schema validator when the event passes validation.</summary>
    [IgnoreMember]
    [JsonIgnore]
    public string? SchemaValidatedPath { get; set; }

    // Cached object deserialization
    private Dictionary<string, object?>? _object;

    /// <summary>
    /// Routing/partition key for message brokers — stable per entity, preserves ordering.
    /// Format: {table}.{companyId}.{locationId}.{primaryKey}
    /// </summary>
    public string GetPartitionKey() =>
        $"{Table}.{CompanyId ?? "none"}.{LocationId ?? "none"}.{GetPrimaryKey()}";

    public string GetPrimaryKey()
    {
        if (Key.Length >= 1)
            return Key[^1];

        var obj = GetObject();
        if (obj is not null && obj.TryGetValue("id", out var id)
            && id is System.Text.Json.JsonElement el
            && el.ValueKind == System.Text.Json.JsonValueKind.String
            && el.GetString() is string idStr)
            return idStr;

        return string.Empty;
    }

    /// <summary>
    /// Returns the After (or Before for deletes) payload as a mutable dictionary.
    /// </summary>
    public Dictionary<string, object?>? GetObject()
    {
        if (_object is not null)
            return _object;

        JsonElement? source = After ?? Before;
        if (source is null)
            return null;

        _object = JsonSerializer.Deserialize<Dictionary<string, object?>>(source.Value.GetRawText());
        return _object;
    }

    /// <summary>Removes the specified properties from the cached object.</summary>
    public void OmitProperties(params string[] props)
    {
        var obj = GetObject();
        if (obj is null) return;
        foreach (var prop in props)
            obj.Remove(prop);
    }

    public override string ToString() =>
        $"DbChangeEvent[op={Operation},table={Table},id={Id},pk={GetPrimaryKey()}]";
}
