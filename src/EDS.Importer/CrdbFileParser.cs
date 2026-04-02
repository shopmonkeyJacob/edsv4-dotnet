using System.Text.RegularExpressions;

namespace EDS.Importer;

/// <summary>
/// Parses CockroachDB changefeed export filenames.
/// Go reference: internal/util/util.go ParseCRDBExportFile / parsePreciseDate
///
/// Filename format: YYYYMMDDHHMMSSNNNNNNNNNLLLLLLLLLL-{unique}-{topic}-{schema-id}.ndjson[.gz]
///   - 33 digits: 14 date/time + 9 nanoseconds + 10 logical-clock (ignored)
///   - topic maps to the table name (underscores, lowercase)
/// </summary>
public static class CrdbFileParser
{
    // Mirrors Go: `^(\d{33})-\w+-[\w-]+-([a-z0-9_]+)-(\w+)\.ndjson\.gz`
    private static readonly Regex Pattern = new(
        @"^(?<ts>\d{33})-\w+-[\w-]+-(?<topic>[a-z0-9_]+)-\w+\.ndjson(?:\.gz)?$",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>
    /// Tries to parse a CRDB export filename (just the base name, not a full path).
    /// Returns true and sets <paramref name="table"/> + <paramref name="timestamp"/> on success.
    /// </summary>
    public static bool TryParse(string filename, out string table, out DateTimeOffset timestamp)
    {
        table     = string.Empty;
        timestamp = DateTimeOffset.UtcNow;

        var m = Pattern.Match(Path.GetFileName(filename));
        if (!m.Success) return false;

        table     = m.Groups["topic"].Value;
        timestamp = ParseTimestamp(m.Groups["ts"].Value);
        return true;
    }

    /// <summary>
    /// Converts a 33-digit CRDB timestamp string to a <see cref="DateTimeOffset"/>.
    /// Mirrors Go's parsePreciseDate: YYYYMMDDHHMMSS (14) + NNNNNNNNN nanoseconds (9).
    /// </summary>
    public static DateTimeOffset ParseTimestamp(string ts)
    {
        if (ts.Length < 23) return DateTimeOffset.UtcNow;

        if (!DateTime.TryParseExact(ts[..14], "yyyyMMddHHmmss",
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.AssumeUniversal |
                System.Globalization.DateTimeStyles.AdjustToUniversal, out var dt))
            return DateTimeOffset.UtcNow;

        // Nanoseconds portion — convert to 100 ns ticks
        if (!long.TryParse(ts[14..23], out var nanos)) return new DateTimeOffset(dt, TimeSpan.Zero);
        return new DateTimeOffset(dt, TimeSpan.Zero).AddTicks(nanos / 100);
    }
}
