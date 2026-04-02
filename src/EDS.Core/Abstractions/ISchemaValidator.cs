using EDS.Core.Models;

namespace EDS.Core.Abstractions;

public sealed class SchemaValidationResult
{
    /// <summary>True if a schema was found for the event's table.</summary>
    public bool SchemaFound { get; init; }

    /// <summary>True if the event data is valid against the schema.</summary>
    public bool IsValid { get; init; }

    /// <summary>
    /// The transformed output path (used by file-based drivers for output routing).
    /// Empty string if not applicable.
    /// </summary>
    public string Path { get; init; } = string.Empty;
}

public interface ISchemaValidator
{
    /// <summary>
    /// Validates the event against its table's schema.
    /// Returns a result with SchemaFound=false if no schema exists for the table.
    /// </summary>
    SchemaValidationResult Validate(DbChangeEvent evt);
}
