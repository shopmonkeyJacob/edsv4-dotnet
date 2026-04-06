namespace EDS.Core;

/// <summary>
/// Well-known process exit codes for EDS, mirroring the Go implementation.
/// The process manager (systemd, Docker, etc.) uses these to decide restart behavior.
/// </summary>
public static class EdsExitCodes
{
    /// <summary>Normal, clean shutdown.</summary>
    public const int Success = 0;

    /// <summary>Fatal, unrecoverable error (e.g. repeated flush failures).</summary>
    public const int FatalError = 1;

    /// <summary>Intentional restart requested by HQ (upgrade, renew-interval, restart notification).
    /// The process manager should restart immediately with fresh session credentials.</summary>
    public const int IntentionalRestart = 4;

    /// <summary>NATS connectivity lost and could not be re-established.
    /// The process manager should restart after a brief delay.</summary>
    public const int NatsDisconnected = 5;
}
