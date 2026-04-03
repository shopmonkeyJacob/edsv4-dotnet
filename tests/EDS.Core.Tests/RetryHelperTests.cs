using EDS.Core.Utilities;
using System.Net.Sockets;

namespace EDS.Core.Tests;

public class RetryHelperTests
{
    // ── IsTransient ───────────────────────────────────────────────────────────

    [Fact]
    public void IsTransient_HttpRequestException_True() =>
        Assert.True(RetryHelper.IsTransient(new HttpRequestException()));

    [Fact]
    public void IsTransient_SocketException_True() =>
        Assert.True(RetryHelper.IsTransient(new SocketException()));

    [Fact]
    public void IsTransient_IOException_True() =>
        Assert.True(RetryHelper.IsTransient(new IOException()));

    [Fact]
    public void IsTransient_TaskCanceledWithTimeoutInner_True()
    {
        var timeout = new TimeoutException();
        var ex = new TaskCanceledException("timeout", timeout);
        Assert.True(RetryHelper.IsTransient(ex));
    }

    [Fact]
    public void IsTransient_PlainTaskCanceled_False()
    {
        // TaskCanceledException without inner TimeoutException is not transient
        var ex = new TaskCanceledException();
        Assert.False(RetryHelper.IsTransient(ex));
    }

    [Fact]
    public void IsTransient_InvalidOperationException_False() =>
        Assert.False(RetryHelper.IsTransient(new InvalidOperationException()));

    [Fact]
    public void IsTransient_ArgumentException_False() =>
        Assert.False(RetryHelper.IsTransient(new ArgumentException()));

    [Fact]
    public void IsTransient_OperationCanceledException_False() =>
        Assert.False(RetryHelper.IsTransient(new OperationCanceledException()));

    // ── ExecuteAsync — success path ───────────────────────────────────────────

    [Fact]
    public async Task ExecuteAsync_SucceedsFirstAttempt_ReturnsValue()
    {
        var result = await RetryHelper.ExecuteAsync<int>(
            _ => Task.FromResult(42));

        Assert.Equal(42, result);
    }

    [Fact]
    public async Task ExecuteAsync_ActionOverload_Completes()
    {
        var ran = false;
        await RetryHelper.ExecuteAsync(_ => { ran = true; return Task.CompletedTask; });
        Assert.True(ran);
    }

    // ── ExecuteAsync — non-transient error ────────────────────────────────────

    [Fact]
    public async Task ExecuteAsync_NonTransientException_ThrowsImmediately()
    {
        var callCount = 0;

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await RetryHelper.ExecuteAsync<int>(_ =>
            {
                callCount++;
                throw new InvalidOperationException("fatal");
            });
        });

        // Must not have retried — non-transient errors throw on the first attempt.
        Assert.Equal(1, callCount);
    }

    // ── ExecuteAsync — maxAttempts=1 means no retry even on transient ─────────

    [Fact]
    public async Task ExecuteAsync_MaxAttempts1_DoesNotRetry()
    {
        var callCount = 0;

        await Assert.ThrowsAsync<HttpRequestException>(async () =>
        {
            await RetryHelper.ExecuteAsync<int>(
                _ =>
                {
                    callCount++;
                    throw new HttpRequestException("network");
                },
                maxAttempts: 1);
        });

        Assert.Equal(1, callCount);
    }

    // ── ExecuteAsync — cancellation ───────────────────────────────────────────

    [Fact]
    public async Task ExecuteAsync_CancelledToken_DoesNotRetry()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<HttpRequestException>(async () =>
        {
            await RetryHelper.ExecuteAsync<int>(
                _ => throw new HttpRequestException("net"),
                maxAttempts: 5,
                ct: cts.Token);
        });
    }
}
