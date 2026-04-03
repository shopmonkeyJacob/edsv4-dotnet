using EDS.Infrastructure.Metrics;

namespace EDS.Infrastructure.Tests;

public class StatusProviderTests
{
    // ── Initial state ─────────────────────────────────────────────────────────

    [Fact]
    public void GetSnapshot_InitialState_ZeroCounters()
    {
        var provider = new StatusProvider();
        var snap     = provider.GetSnapshot();

        Assert.Equal(0, snap.EventsProcessed);
        Assert.Equal(0, snap.PendingFlush);
        Assert.False(snap.Paused);
        Assert.Null(snap.LastEventAt);
        Assert.Equal(string.Empty, snap.LastEventTable);
    }

    [Fact]
    public void GetSnapshot_InitialState_UptimeNonNegative()
    {
        var provider = new StatusProvider();
        Assert.True(provider.GetSnapshot().UptimeSeconds >= 0);
    }

    // ── Paused state ──────────────────────────────────────────────────────────

    [Fact]
    public void SetPaused_True_SnapshotShowsPaused()
    {
        var provider = new StatusProvider();
        provider.SetPaused(true);

        Assert.True(provider.GetSnapshot().Paused);
    }

    [Fact]
    public void SetPaused_False_SnapshotShowsNotPaused()
    {
        var provider = new StatusProvider();
        provider.SetPaused(true);
        provider.SetPaused(false);

        Assert.False(provider.GetSnapshot().Paused);
    }

    // ── PendingFlush ──────────────────────────────────────────────────────────

    [Fact]
    public void SetPendingFlush_SetsValue()
    {
        var provider = new StatusProvider();
        provider.SetPendingFlush(42);

        Assert.Equal(42, provider.GetSnapshot().PendingFlush);
    }

    [Fact]
    public void SetPendingFlush_Overwrite_ReplacesOldValue()
    {
        var provider = new StatusProvider();
        provider.SetPendingFlush(10);
        provider.SetPendingFlush(3);

        Assert.Equal(3, provider.GetSnapshot().PendingFlush);
    }

    // ── EventsProcessed ───────────────────────────────────────────────────────

    [Fact]
    public void RecordFlush_AccumulatesCount()
    {
        var provider = new StatusProvider();
        provider.RecordFlush(100);
        provider.RecordFlush(50);

        Assert.Equal(150, provider.GetSnapshot().EventsProcessed);
    }

    // ── LastEvent ─────────────────────────────────────────────────────────────

    [Fact]
    public void RecordLastEvent_SetsTableAndTimestamp()
    {
        var before   = DateTimeOffset.UtcNow;
        var provider = new StatusProvider();
        provider.RecordLastEvent("orders");

        var snap = provider.GetSnapshot();
        Assert.Equal("orders", snap.LastEventTable);
        Assert.NotNull(snap.LastEventAt);
        Assert.True(snap.LastEventAt >= before);
    }

    [Fact]
    public void RecordLastEvent_CalledTwice_UpdatesToLatestTable()
    {
        var provider = new StatusProvider();
        provider.RecordLastEvent("orders");
        provider.RecordLastEvent("invoices");

        Assert.Equal("invoices", provider.GetSnapshot().LastEventTable);
    }

    // ── Startup metadata ──────────────────────────────────────────────────────

    [Fact]
    public void VersionAndSessionId_SetOnce_ReflectedInSnapshot()
    {
        var provider = new StatusProvider
        {
            Version   = "1.2.3",
            SessionId = "sess-abc",
            Driver    = "postgresql://host/db"
        };

        var snap = provider.GetSnapshot();

        Assert.Equal("1.2.3",    snap.Version);
        Assert.Equal("sess-abc", snap.SessionId);
        Assert.Equal("postgresql://host/db", snap.Driver);
    }

    // ── SanitizeUrl ───────────────────────────────────────────────────────────

    [Theory]
    [InlineData("postgresql://user:pass@host:5432/db",   "postgresql://host:5432/db")]
    [InlineData("mysql://admin:secret@localhost/shop",   "mysql://localhost/shop")]
    [InlineData("postgresql://host:5432/db",             "postgresql://host:5432/db")]
    [InlineData("",                                      "")]
    public void SanitizeUrl_StripsCredentials(string input, string expected)
    {
        Assert.Equal(expected, StatusProvider.SanitizeUrl(input));
    }

    [Fact]
    public void SanitizeUrl_InvalidUrl_ReturnsOriginal()
    {
        const string input = "not-a-url";
        Assert.Equal(input, StatusProvider.SanitizeUrl(input));
    }

    [Fact]
    public void SanitizeUrl_NoTrailingSlash()
    {
        var result = StatusProvider.SanitizeUrl("postgresql://host:5432/db");
        Assert.False(result.EndsWith('/'));
    }
}
