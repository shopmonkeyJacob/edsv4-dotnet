using EDS.Core.Abstractions;

namespace EDS.Core.Helpers;

/// <summary>
/// Factory helpers for building DriverField definitions, mirroring the Go helper functions.
/// </summary>
public static class DriverFieldHelpers
{
    public static DriverField RequiredString(string name, string description, string? defaultValue = null) =>
        new() { Name = name, Type = DriverFieldType.String, Description = description, Required = true, Default = defaultValue };

    public static DriverField OptionalString(string name, string description, string? defaultValue = null) =>
        new() { Name = name, Type = DriverFieldType.String, Description = description, Required = false, Default = defaultValue };

    public static DriverField OptionalPassword(string name, string description, string? defaultValue = null) =>
        new() { Name = name, Type = DriverFieldType.String, Format = DriverFieldFormat.Password, Description = description, Required = false, Default = defaultValue };

    public static DriverField OptionalNumber(string name, string description, int? defaultValue = null) =>
        new() { Name = name, Type = DriverFieldType.Number, Description = description, Required = false, Default = defaultValue?.ToString() };

    /// <summary>Standard hostname/port/username/password/database fields for SQL-style drivers.</summary>
    public static IReadOnlyList<DriverField> DatabaseFields(int defaultPort = 0)
    {
        var fields = new List<DriverField>
        {
            RequiredString("Database", "The database name to use"),
            OptionalString("Username", "The username to connect with"),
            OptionalPassword("Password", "The password to connect with"),
            RequiredString("Hostname", "The hostname or IP address"),
        };
        if (defaultPort > 0)
            fields.Add(OptionalNumber("Port", "The port number", defaultPort));
        return fields;
    }

    public static string GetRequiredString(string name, Dictionary<string, object?> values)
    {
        if (values.TryGetValue(name, out var val) && val is string s && s.Length > 0)
            return s;
        throw new InvalidOperationException($"Required field '{name}' is missing.");
    }

    public static string GetOptionalString(string name, string defaultValue, Dictionary<string, object?> values)
    {
        if (values.TryGetValue(name, out var val) && val is string s && s.Length > 0)
            return s;
        return defaultValue;
    }

    public static int GetOptionalInt(string name, int defaultValue, Dictionary<string, object?> values)
    {
        if (!values.TryGetValue(name, out var val)) return defaultValue;
        return val switch
        {
            int i => i,
            long l => (int)l,
            string s when int.TryParse(s, out var parsed) => parsed,
            _ => defaultValue
        };
    }

    public static string BuildDatabaseUrl(string scheme, int defaultPort, Dictionary<string, object?> values)
    {
        var hostname = GetRequiredString("Hostname", values);
        var username = GetOptionalString("Username", string.Empty, values);
        var password = GetOptionalString("Password", string.Empty, values);
        var port = GetOptionalInt("Port", defaultPort, values);
        var database = GetRequiredString("Database", values);

        var builder = new UriBuilder
        {
            Scheme = scheme,
            Host = hostname,
            Path = database
        };
        if (defaultPort > 0) builder.Port = port;
        if (username.Length > 0) builder.UserName = Uri.EscapeDataString(username);
        if (password.Length > 0) builder.Password = Uri.EscapeDataString(password);

        return builder.Uri.ToString();
    }
}
