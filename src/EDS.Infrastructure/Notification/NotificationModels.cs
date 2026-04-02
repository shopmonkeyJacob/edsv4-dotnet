using MessagePack;
using System.Text.Json.Serialization;

namespace EDS.Infrastructure.Notification;

// ── Incoming (from Shopmonkey HQ) ─────────────────────────────────────────────

/// <summary>Envelope that wraps every inbound notification message.</summary>
internal sealed class NotificationPayload
{
    [JsonPropertyName("action")] public string Action { get; set; } = "";
    [JsonPropertyName("data")]   public Dictionary<string, System.Text.Json.JsonElement> Data { get; set; } = new();
}

// ── Outgoing response types (msgpack-encoded to match the Go implementation) ──

[MessagePackObject]
public sealed class GenericNotificationResponse
{
    [Key("success")]   [JsonPropertyName("success")]   public bool    Success   { get; set; }
    [Key("message")]   [JsonPropertyName("message")]   public string? Message   { get; set; }
    [Key("sessionId")] [JsonPropertyName("sessionId")] public string  SessionId { get; set; } = "";
    [Key("action")]    [JsonPropertyName("action")]    public string  Action    { get; set; } = "";
}

[MessagePackObject]
public sealed class DriverConfigNotificationResponse
{
    [Key("sessionId")] [JsonPropertyName("sessionId")] public string SessionId { get; set; } = "";
    [Key("drivers")]   [JsonPropertyName("drivers")]
    public Dictionary<string, DriverConfiguratorDto> Drivers { get; set; } = new();
}

[MessagePackObject]
public sealed class DriverConfiguratorDto
{
    [Key("metadata")] [JsonPropertyName("metadata")] public DriverMetadataDto    Metadata { get; set; } = new();
    [Key("fields")]   [JsonPropertyName("fields")]   public List<DriverFieldDto> Fields   { get; set; } = new();
}

[MessagePackObject]
public sealed class DriverMetadataDto
{
    [Key("scheme")]            [JsonPropertyName("scheme")]            public string Scheme            { get; set; } = "";
    [Key("name")]              [JsonPropertyName("name")]              public string Name              { get; set; } = "";
    [Key("description")]       [JsonPropertyName("description")]       public string Description       { get; set; } = "";
    [Key("exampleURL")]        [JsonPropertyName("exampleURL")]        public string ExampleUrl        { get; set; } = "";
    [Key("help")]              [JsonPropertyName("help")]              public string Help              { get; set; } = "";
    [Key("supportsImport")]    [JsonPropertyName("supportsImport")]    public bool   SupportsImport    { get; set; }
    [Key("supportsMigration")] [JsonPropertyName("supportsMigration")] public bool  SupportsMigration { get; set; }
}

[MessagePackObject]
public sealed class DriverFieldDto
{
    [Key("name")]        [JsonPropertyName("name")]        public string  Name        { get; set; } = "";
    [Key("type")]        [JsonPropertyName("type")]        public string  Type        { get; set; } = "string";
    [Key("format")]      [JsonPropertyName("format")]      public string  Format      { get; set; } = "";
    [Key("default")]     [JsonPropertyName("default")]     public string? Default     { get; set; }
    [Key("description")] [JsonPropertyName("description")] public string  Description { get; set; } = "";
    [Key("required")]    [JsonPropertyName("required")]    public bool    Required    { get; set; }
}

[MessagePackObject]
public sealed class ValidateNotificationResponse
{
    [Key("success")]     [JsonPropertyName("success")]      public bool               Success     { get; set; }
    [Key("message")]     [JsonPropertyName("message")]      public string?            Message     { get; set; }
    [Key("sessionId")]   [JsonPropertyName("sessionId")]    public string             SessionId   { get; set; } = "";
    [Key("url")]         [JsonPropertyName("url")]          public string             Url         { get; set; } = "";
    [Key("fieldErrors")] [JsonPropertyName("field_errors")] public List<FieldErrorDto> FieldErrors { get; set; } = new();
}

[MessagePackObject]
public sealed class FieldErrorDto
{
    [Key("field")] [JsonPropertyName("field")] public string Field { get; set; } = "";
    [Key("error")] [JsonPropertyName("error")] public string Error { get; set; } = "";
}

[MessagePackObject]
public sealed class ConfigureNotificationResponse
{
    [Key("success")]   [JsonPropertyName("success")]   public bool    Success   { get; set; }
    [Key("message")]   [JsonPropertyName("message")]   public string? Message   { get; set; }
    [Key("sessionId")] [JsonPropertyName("sessionId")] public string  SessionId { get; set; } = "";
    [Key("maskedURL")] [JsonPropertyName("maskedURL")] public string? MaskedUrl { get; set; }
    [Key("backfill")]  [JsonPropertyName("backfill")]  public bool    Backfill  { get; set; }
    [Key("logPath")]   [JsonPropertyName("logPath")]   public string? LogPath   { get; set; }
}
