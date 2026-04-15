using System.Text.RegularExpressions;

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
        var pattern = new Regex(@"^\s*" + Regex.Escape(key) + @"\s*=", RegexOptions.IgnoreCase);

        for (int i = 0; i < lines.Count; i++)
        {
            if (pattern.IsMatch(lines[i]))
            {
                lines[i] = $"{key} = \"{value}\"";
                written  = true;
            }
        }

        if (!written)
            lines.Add($"{key} = \"{value}\"");

        // Write atomically: write to a temp file, then rename over the target.
        // Avoids a corrupt config if the process is killed mid-write.
        var tempPath = configPath + ".tmp";
        await File.WriteAllTextAsync(tempPath, string.Join('\n', lines));

        // Restrict to owner read/write on Unix — the file may contain the API token.
        if (!OperatingSystem.IsWindows())
            File.SetUnixFileMode(tempPath,
                UnixFileMode.UserRead | UnixFileMode.UserWrite);

        File.Move(tempPath, configPath, overwrite: true);
    }
}
