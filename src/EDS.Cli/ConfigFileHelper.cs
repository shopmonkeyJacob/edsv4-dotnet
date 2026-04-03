namespace EDS.Cli;

/// <summary>
/// Reads and writes key = "value" pairs in a TOML-style config file.
/// Preserves all other lines and comments when updating an existing file.
/// </summary>
internal static class ConfigFileHelper
{
    /// <summary>
    /// Updates the first line whose key matches <paramref name="key"/>, or appends
    /// a new <c>key = "value"</c> line if none exists.
    /// Creates the parent directory if it does not already exist.
    /// </summary>
    public static async Task SetValueAsync(string configPath, string key, string value)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(configPath)!);

        var existing = File.Exists(configPath)
            ? await File.ReadAllTextAsync(configPath)
            : string.Empty;

        var lines   = existing.Split('\n').ToList();
        bool written = false;

        for (int i = 0; i < lines.Count; i++)
        {
            var t = lines[i].TrimStart();
            if (t.StartsWith(key, StringComparison.OrdinalIgnoreCase) && t.Contains('='))
            {
                lines[i] = $"{key} = \"{value}\"";
                written  = true;
            }
        }

        if (!written)
            lines.Add($"{key} = \"{value}\"");

        await File.WriteAllTextAsync(configPath, string.Join('\n', lines));

        // Restrict to owner read/write on Unix — the file may contain the API token.
        if (!OperatingSystem.IsWindows())
            File.SetUnixFileMode(configPath,
                UnixFileMode.UserRead | UnixFileMode.UserWrite);
    }
}
