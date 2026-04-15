using EDS.Core.Models;
using System.Text;
using System.Text.RegularExpressions;

namespace EDS.Core.Helpers;

/// <summary>
/// Shared SQL generation utilities used by all relational database drivers.
/// </summary>
public static class SqlHelpers
{
    // Matches valid JSON/SQL numeric literals: optional minus, integer or decimal,
    // optional exponent. Used to guard GetRawText() output before embedding in SQL.
    private static readonly Regex SafeNumericLiteralPattern =
        new(@"^-?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$", RegexOptions.Compiled);

    // Matches safe SQL identifiers: 1–128 characters, letters / digits / underscore only.
    // Mirrors the Go reference validation applied before any SQL identifier is embedded.
    private static readonly Regex SafeIdentifierPattern =
        new(@"^[A-Za-z0-9_]{1,128}$", RegexOptions.Compiled);

    /// <summary>
    /// Returns true when <paramref name="rawText"/> is a safe SQL numeric literal
    /// (integer, decimal, or scientific notation) that can be embedded directly in SQL.
    /// </summary>
    public static bool IsValidNumericLiteral(string rawText) =>
        SafeNumericLiteralPattern.IsMatch(rawText);

    /// <summary>
    /// Throws <see cref="InvalidOperationException"/> if <paramref name="name"/> is not a
    /// safe SQL identifier: 1–128 characters, ASCII letters/digits/underscores only.
    /// Call this before embedding any externally sourced table or column name in SQL.
    /// </summary>
    public static void AssertSafeIdentifier(string name)
    {
        if (!SafeIdentifierPattern.IsMatch(name))
            throw new InvalidOperationException(
                $"Unsafe SQL identifier rejected: '{name}'. " +
                "Identifiers must be 1–128 characters and contain only letters, digits, or underscores.");
    }


    /// <summary>
    /// Generates an UPSERT (INSERT OR UPDATE) SQL statement for a schema + event.
    /// The exact syntax varies by database — use the dialect-specific overloads.
    /// </summary>
    public static string BuildUpsertSql(
        Schema schema,
        DbChangeEvent evt,
        string quoteChar,
        Func<string, string>? paramName = null)
    {
        paramName ??= col => $"@{col}";
        var cols = schema.Columns();
        var q = quoteChar;

        var sb = new StringBuilder();
        sb.Append($"INSERT INTO {q}{schema.Table}{q} (");
        sb.Append(string.Join(", ", cols.Select(c => $"{q}{c}{q}")));
        sb.Append(") VALUES (");
        sb.Append(string.Join(", ", cols.Select(c => paramName(c))));
        sb.Append(')');
        return sb.ToString();
    }

    public static string BuildDeleteSql(Schema schema, string quoteChar, Func<string, string>? paramName = null)
    {
        paramName ??= col => $"@{col}";
        var q = quoteChar;
        var where = string.Join(" AND ", schema.PrimaryKeys.Select(pk => $"{q}{pk}{q} = {paramName(pk)}"));
        return $"DELETE FROM {q}{schema.Table}{q} WHERE {where}";
    }

    public static string BuildCreateTableSql(Schema schema, string quoteChar, Func<string, string> mapType)
    {
        var q = quoteChar;
        var sb = new StringBuilder();
        sb.AppendLine($"CREATE TABLE IF NOT EXISTS {q}{schema.Table}{q} (");

        var cols = schema.Columns();
        for (int i = 0; i < cols.Count; i++)
        {
            var col = cols[i];
            var prop = schema.Properties[col];
            var sqlType = mapType(prop.Type);
            var nullable = prop.IsNotNull ? "NOT NULL" : "NULL";
            var comma = i < cols.Count - 1 ? "," : string.Empty;
            sb.AppendLine($"  {q}{col}{q} {sqlType} {nullable}{comma}");
        }

        if (schema.PrimaryKeys.Count > 0)
        {
            var pks = string.Join(", ", schema.PrimaryKeys.Select(pk => $"{q}{pk}{q}"));
            sb.AppendLine($"  , PRIMARY KEY ({pks})");
        }

        sb.Append(')');
        return sb.ToString();
    }

    public static string BuildAddColumnSql(string table, string column, string sqlType, string quoteChar)
    {
        var q = quoteChar;
        return $"ALTER TABLE {q}{table}{q} ADD COLUMN {q}{column}{q} {sqlType}";
    }
}
