using EDS.Core.Models;
using System.Text.Json;

namespace EDS.Core.Tests;

public class DbChangeEventTests
{
    private static JsonElement ParseJson(string json) =>
        JsonDocument.Parse(json).RootElement.Clone();

    private static DbChangeEvent Insert(
        string table = "orders",
        string id = "evt-1",
        string[] key = null!,
        string? companyId = "cmp-1",
        string? locationId = "loc-1",
        JsonElement? after = null) =>
        new()
        {
            Operation  = "insert",
            Id         = id,
            Table      = table,
            Key        = key ?? ["row-1"],
            CompanyId  = companyId,
            LocationId = locationId,
            After      = after,
        };

    // ── GetPartitionKey ───────────────────────────────────────────────────────

    [Fact]
    public void GetPartitionKey_AllFieldsPresent_FormatsCorrectly()
    {
        var evt = Insert(table: "orders", key: ["row-99"], companyId: "cmp-1", locationId: "loc-2");

        var key = evt.GetPartitionKey();

        Assert.Equal("orders.cmp-1.loc-2.row-99", key);
    }

    [Fact]
    public void GetPartitionKey_NullCompanyId_UsesNone()
    {
        var evt = Insert(companyId: null, locationId: null, key: ["pk"]);

        var key = evt.GetPartitionKey();

        Assert.Equal("orders.none.none.pk", key);
    }

    // ── GetPrimaryKey ─────────────────────────────────────────────────────────

    [Fact]
    public void GetPrimaryKey_KeyArray_ReturnsLastElement()
    {
        var evt = Insert(key: ["row-42"]);
        Assert.Equal("row-42", evt.GetPrimaryKey());
    }

    [Fact]
    public void GetPrimaryKey_MultiElementKey_ReturnsLastElement()
    {
        var evt = Insert(key: ["part1", "part2", "row-99"]);
        Assert.Equal("row-99", evt.GetPrimaryKey());
    }

    [Fact]
    public void GetPrimaryKey_EmptyKeyNoAfter_ReturnsEmpty()
    {
        var evt = new DbChangeEvent
        {
            Operation = "delete",
            Id = "1",
            Table = "orders",
            Key = [],
        };

        Assert.Equal(string.Empty, evt.GetPrimaryKey());
    }

    [Fact]
    public void GetPrimaryKey_EmptyKeyWithAfter_ReturnsEmpty()
    {
        // GetObject() deserializes values as JsonElement (not string), so the
        // `id is string` branch never matches. Empty key → empty primary key.
        var after = ParseJson("""{"id": "json-id", "name": "test"}""");
        var evt = Insert(key: [], after: after);

        Assert.Equal(string.Empty, evt.GetPrimaryKey());
    }

    // ── GetObject ─────────────────────────────────────────────────────────────

    [Fact]
    public void GetObject_ReturnsAfterPayload()
    {
        var after = ParseJson("""{"id": "1", "name": "Brake Pad"}""");
        var evt = Insert(after: after);

        var obj = evt.GetObject();

        Assert.NotNull(obj);
        Assert.True(obj.ContainsKey("id"));
        Assert.True(obj.ContainsKey("name"));
    }

    [Fact]
    public void GetObject_NoAfter_ReturnsBefore()
    {
        var before = ParseJson("""{"id": "deleted-row"}""");
        var evt = new DbChangeEvent
        {
            Operation  = "delete",
            Id         = "1",
            Table      = "orders",
            Key        = ["deleted-row"],
            Before     = before,
        };

        var obj = evt.GetObject();

        Assert.NotNull(obj);
        Assert.True(obj.ContainsKey("id"));
    }

    [Fact]
    public void GetObject_NeitherAfterNorBefore_ReturnsNull()
    {
        var evt = Insert();

        Assert.Null(evt.GetObject());
    }

    [Fact]
    public void GetObject_CalledTwice_ReturnsSameDictionary()
    {
        var after = ParseJson("""{"id": "1"}""");
        var evt = Insert(after: after);

        var first  = evt.GetObject();
        var second = evt.GetObject();

        Assert.Same(first, second);
    }

    // ── OmitProperties ────────────────────────────────────────────────────────

    [Fact]
    public void OmitProperties_RemovesSpecifiedKeys()
    {
        var after = ParseJson("""{"id": "1", "secret": "pw", "name": "test"}""");
        var evt = Insert(after: after);

        evt.OmitProperties("secret");

        var obj = evt.GetObject()!;
        Assert.False(obj.ContainsKey("secret"));
        Assert.True(obj.ContainsKey("id"));
        Assert.True(obj.ContainsKey("name"));
    }

    [Fact]
    public void OmitProperties_MultipleKeys_AllRemoved()
    {
        var after = ParseJson("""{"id": "1", "a": 1, "b": 2, "c": 3}""");
        var evt = Insert(after: after);

        evt.OmitProperties("a", "b");

        var obj = evt.GetObject()!;
        Assert.False(obj.ContainsKey("a"));
        Assert.False(obj.ContainsKey("b"));
        Assert.True(obj.ContainsKey("id"));
        Assert.True(obj.ContainsKey("c"));
    }

    [Fact]
    public void OmitProperties_NoPayload_DoesNotThrow()
    {
        var evt = Insert();
        evt.OmitProperties("any");   // should be a no-op, not throw
    }

    // ── ToString ──────────────────────────────────────────────────────────────

    [Fact]
    public void ToString_ContainsOperationTableAndId()
    {
        var evt = Insert(table: "orders", id: "evt-1", key: ["row-5"]);

        var str = evt.ToString();

        Assert.Contains("op=insert", str);
        Assert.Contains("table=orders", str);
        Assert.Contains("id=evt-1", str);
        Assert.Contains("pk=row-5", str);
    }
}
