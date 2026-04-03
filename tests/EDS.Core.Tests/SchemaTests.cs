using EDS.Core.Models;

namespace EDS.Core.Tests;

public class SchemaTests
{
    // ── Schema.Columns() ─────────────────────────────────────────────────────

    [Fact]
    public void Columns_PrimaryKeysFirst_ThenAlphabetical()
    {
        var schema = new Schema
        {
            Table = "orders",
            PrimaryKeys = ["id"],
            Properties = new Dictionary<string, SchemaProperty>
            {
                ["id"]        = new() { Type = "string" },
                ["createdAt"] = new() { Type = "string" },
                ["amount"]    = new() { Type = "number" },
                ["status"]    = new() { Type = "string" },
            }
        };

        var cols = schema.Columns();

        Assert.Equal("id", cols[0]);
        Assert.Equal(["id", "amount", "createdAt", "status"], cols);
    }

    [Fact]
    public void Columns_MultipleKeys_PreserveKeyOrder_ThenAlpha()
    {
        var schema = new Schema
        {
            Table = "order_items",
            PrimaryKeys = ["orderId", "itemId"],
            Properties = new Dictionary<string, SchemaProperty>
            {
                ["orderId"] = new() { Type = "string" },
                ["itemId"]  = new() { Type = "string" },
                ["qty"]     = new() { Type = "number" },
                ["price"]   = new() { Type = "number" },
            }
        };

        var cols = schema.Columns();

        Assert.Equal("orderId", cols[0]);
        Assert.Equal("itemId", cols[1]);
        Assert.Equal("price", cols[2]);
        Assert.Equal("qty", cols[3]);
    }

    [Fact]
    public void Columns_NoPrimaryKeys_AllAlphabetical()
    {
        var schema = new Schema
        {
            Table = "t",
            PrimaryKeys = [],
            Properties = new Dictionary<string, SchemaProperty>
            {
                ["zzz"] = new() { Type = "string" },
                ["aaa"] = new() { Type = "string" },
                ["mmm"] = new() { Type = "string" },
            }
        };

        var cols = schema.Columns();

        Assert.Equal(["aaa", "mmm", "zzz"], cols);
    }

    [Fact]
    public void Columns_CalledTwice_ReturnsSameInstance()
    {
        var schema = new Schema
        {
            Table = "t",
            PrimaryKeys = ["id"],
            Properties = new Dictionary<string, SchemaProperty>
            {
                ["id"]   = new() { Type = "string" },
                ["name"] = new() { Type = "string" },
            }
        };

        var first  = schema.Columns();
        var second = schema.Columns();

        Assert.Same(first, second);
    }

    // ── SchemaProperty ────────────────────────────────────────────────────────

    [Theory]
    [InlineData("string", false, true)]    // nullable=false → IsNotNull
    [InlineData("string", true,  false)]   // nullable=true  → not IsNotNull
    [InlineData("array",  true,  true)]    // array always IsNotNull regardless of Nullable
    [InlineData("array",  false, true)]    // array + non-nullable → IsNotNull
    public void SchemaProperty_IsNotNull(string type, bool nullable, bool expected)
    {
        var prop = new SchemaProperty { Type = type, Nullable = nullable };
        Assert.Equal(expected, prop.IsNotNull);
    }

    [Theory]
    [InlineData("object", true)]
    [InlineData("array",  true)]
    [InlineData("string", false)]
    [InlineData("number", false)]
    [InlineData("boolean", false)]
    public void SchemaProperty_IsArrayOrJson(string type, bool expected)
    {
        var prop = new SchemaProperty { Type = type };
        Assert.Equal(expected, prop.IsArrayOrJson);
    }

    // ── DatabaseSchema ────────────────────────────────────────────────────────

    [Fact]
    public void DatabaseSchema_GetColumns_ReturnsSortedColumnNames()
    {
        var db = new DatabaseSchema
        {
            ["orders"] = new Dictionary<string, string>
            {
                ["status"] = "TEXT",
                ["id"]     = "TEXT",
                ["amount"] = "REAL",
            }
        };

        var cols = db.GetColumns("orders");

        Assert.Equal(["amount", "id", "status"], cols);
    }

    [Fact]
    public void DatabaseSchema_GetColumns_MissingTable_ReturnsEmpty()
    {
        var db = new DatabaseSchema();

        var cols = db.GetColumns("nonexistent");

        Assert.Empty(cols);
    }

    [Fact]
    public void DatabaseSchema_GetColumnType_Found()
    {
        var db = new DatabaseSchema
        {
            ["orders"] = new Dictionary<string, string> { ["id"] = "TEXT" }
        };

        var (found, type) = db.GetColumnType("orders", "id");

        Assert.True(found);
        Assert.Equal("TEXT", type);
    }

    [Fact]
    public void DatabaseSchema_GetColumnType_TableNotFound()
    {
        var db = new DatabaseSchema();

        var (found, type) = db.GetColumnType("orders", "id");

        Assert.False(found);
        Assert.Equal(string.Empty, type);
    }

    [Fact]
    public void DatabaseSchema_GetColumnType_ColumnNotFound()
    {
        var db = new DatabaseSchema
        {
            ["orders"] = new Dictionary<string, string> { ["id"] = "TEXT" }
        };

        var (found, type) = db.GetColumnType("orders", "missing");

        Assert.False(found);
        Assert.Equal(string.Empty, type);
    }
}
