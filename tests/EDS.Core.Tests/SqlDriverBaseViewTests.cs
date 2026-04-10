using EDS.Core.Abstractions;
using EDS.Core.Drivers;
using EDS.Core.Models;
using Microsoft.Extensions.Logging;
using System.Data.Common;
using System.Reflection;

namespace EDS.Core.Tests;

// ── Minimal concrete driver for view-SQL tests (PostgreSQL-style double-quote quoting) ──

internal sealed class ViewTestableSqlDriver : SqlDriverBase
{
    protected override Task<DbConnection> OpenConnectionAsync(CancellationToken ct) =>
        throw new NotImplementedException();
    protected override string GetDatabaseNameSql()    => "SELECT 1";
    protected override string GetSchemaInfoSql()      => "";
    protected override void AddDbNameParameter(DbCommand cmd, string dbName) { }
    protected override string BuildSql(DbChangeEvent evt, Schema schema) => "";
    protected override string BuildCreateTableIfNotExistsSql(Schema schema) => "";
    protected override string BuildDropAndCreateTableSql(Schema schema)    => "";
    protected override string BuildAlterAddColumnSql(string table, string col, string sqlType) => "";
    protected override string PropToSqlType(SchemaProperty prop, bool isPrimaryKey = false) => "TEXT";
    protected override string QuoteId(string name) => $"\"{name}\"";
    public override Task TestAsync(ILogger logger, string url, CancellationToken ct = default) => Task.CompletedTask;
    public override IReadOnlyList<DriverField> Configuration() => [];
    public override (string url, IReadOnlyList<FieldError> errors) Validate(Dictionary<string, object?> values)
        => ("test://localhost", []);
}

public class SqlDriverBaseViewTests
{
    private static readonly ViewTestableSqlDriver Driver = new();

    /// <summary>Invokes the private BuildUnifiedViewSql method via reflection.</summary>
    private static string BuildUnifiedViewSql(string tableName, Schema schema)
    {
        // The method is private on SqlDriverBase (not the concrete subclass), so we must
        // resolve it from the declaring base type rather than the runtime type.
        var method = typeof(SqlDriverBase).GetMethod("BuildUnifiedViewSql", BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new MissingMethodException("BuildUnifiedViewSql not found");
        return (string)method.Invoke(Driver, [tableName, schema])!;
    }

    // ── Schema helpers ────────────────────────────────────────────────────────

    // Schema with no properties and no primary keys → Columns() returns an empty list
    private static Schema EmptySchema(string table = "orders") => new()
    {
        Table       = table,
        PrimaryKeys = [],
        Properties  = new Dictionary<string, SchemaProperty>(),
        Required    = [],
    };

    private static Schema SimpleSchema(string table = "orders", string[]? primaryKeys = null) => new()
    {
        Table       = table,
        PrimaryKeys = (primaryKeys ?? ["id"]).ToList(),
        Properties  = new Dictionary<string, SchemaProperty>
        {
            ["id"]         = new SchemaProperty { Type = "string" },
            ["name"]       = new SchemaProperty { Type = "string", Nullable = true },
            ["amount"]     = new SchemaProperty { Type = "number", Nullable = true },
        },
        Required    = ["id"],
    };

    private static Schema NoPrimaryKeySchema(string table = "orders") => new()
    {
        Table       = table,
        PrimaryKeys = [],
        Properties  = new Dictionary<string, SchemaProperty>
        {
            ["id"]   = new SchemaProperty { Type = "string" },
            ["name"] = new SchemaProperty { Type = "string", Nullable = true },
        },
        Required = ["id"],
    };

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Fact]
    public void UnifiedView_ReturnsEmptyString_WhenSchemaHasNoColumns()
    {
        var sql = BuildUnifiedViewSql("orders", EmptySchema("orders"));
        Assert.Equal(string.Empty, sql);
    }

    [Fact]
    public void UnifiedView_ContainsUnionAll()
    {
        var sql = BuildUnifiedViewSql("orders", SimpleSchema());
        Assert.Contains("UNION ALL", sql);
    }

    [Fact]
    public void UnifiedView_ContainsNotExists()
    {
        var sql = BuildUnifiedViewSql("orders", SimpleSchema());
        Assert.Contains("NOT EXISTS", sql);
    }

    [Fact]
    public void UnifiedView_UsesFirstPrimaryKey_InJoin()
    {
        var schema = SimpleSchema(primaryKeys: ["order_id"]);
        var sql = BuildUnifiedViewSql("orders", schema);
        // The WHERE NOT EXISTS clause should reference the primary key column
        Assert.Contains("\"order_id\"", sql);
    }

    [Fact]
    public void UnifiedView_FallsBackToId_WhenNoPrimaryKeys()
    {
        var sql = BuildUnifiedViewSql("orders", NoPrimaryKeySchema());
        // Should still reference "id" as the fallback entity identifier
        Assert.Contains("\"id\"", sql);
    }

    [Fact]
    public void UnifiedView_ContainsRowNumber()
    {
        var sql = BuildUnifiedViewSql("orders", SimpleSchema());
        Assert.Contains("ROW_NUMBER()", sql);
        Assert.Contains("PARTITION BY _entity_id", sql);
    }

    [Fact]
    public void UnifiedView_ExcludesDeletedEvents()
    {
        var sql = BuildUnifiedViewSql("orders", SimpleSchema());
        Assert.Contains("_operation <> 'DELETE'", sql);
    }

    [Fact]
    public void UnifiedView_ProjectsJsonFields()
    {
        var sql = BuildUnifiedViewSql("orders", SimpleSchema());
        // Each column should appear as a COALESCE of _after / _before JSON extracts
        Assert.Contains("COALESCE(", sql);
        Assert.Contains("_after", sql);
        Assert.Contains("_before", sql);
    }

    [Fact]
    public void UnifiedView_QualifiesEventsTable()
    {
        var sql = BuildUnifiedViewSql("orders", SimpleSchema());
        // The events table must include the events-schema prefix and the _events suffix
        Assert.Contains("eds_events", sql);
        Assert.Contains("orders_events", sql);
    }
}
