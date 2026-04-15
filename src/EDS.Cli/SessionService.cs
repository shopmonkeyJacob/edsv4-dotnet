using EDS.Core.Abstractions;
using EDS.Core.Registry;
using EDS.Core.Utilities;
using Serilog;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace EDS.Cli;

/// <summary>
/// Manages the EDS session lifecycle with the Shopmonkey API.
/// Mirrors the Go sendStart / sendEnd / writeCredsToFile logic in server.go.
/// </summary>
public static class SessionService
{
    private const string DefaultNatsUrl     = "nats://connect.nats.shopmonkey.pub";
    internal  const string LastCredsFileKey = "session:last_creds_file";
    private static readonly HttpClient _sharedHttp = new() { Timeout = TimeSpan.FromMinutes(2) };

    /// <summary>
    /// Fast-path: if a valid (non-expired) NATS credential from a previous session exists
    /// on disk, return its session ID and file path so the caller can skip the slow
    /// <see cref="SendStartAsync"/> API call entirely.
    /// Returns <c>null</c> when no usable credential is found.
    /// </summary>
    public static async Task<(string sessionId, string credsFile)?> TryResumeAsync(
        ITracker tracker,
        CancellationToken ct = default)
    {
        var lastCredsFile = await tracker.GetKeyAsync(LastCredsFileKey, ct);
        if (lastCredsFile is null || !File.Exists(lastCredsFile))
            return null;

        try
        {
            var content  = File.ReadAllText(lastCredsFile);
            var jwtMatch = Regex.Match(content,
                @"-----BEGIN NATS USER JWT-----\s*(.+?)\s*-+END NATS USER JWT-+",
                RegexOptions.Singleline);
            if (!jwtMatch.Success) return null;

            var parts = jwtMatch.Groups[1].Value.Trim().Split('.');
            if (parts.Length != 3) return null;

            using var doc  = JsonDocument.Parse(Base64UrlDecode(parts[1]));
            var root       = doc.RootElement;

            // NATS-format JWT stores expiry at nats.exp (not top-level exp)
            if (!root.TryGetProperty("nats", out var natsProp)
                || !natsProp.TryGetProperty("exp", out var expProp))
                return null;

            var expTime = DateTimeOffset.FromUnixTimeSeconds(expProp.GetInt64());
            if (DateTimeOffset.UtcNow >= expTime)
            {
                Log.Information("[server] Cached NATS credential expired at {Exp} — requesting new session.", expTime);
                return null;
            }

            // Extract the sessionId from the subscription allow-list
            if (!natsProp.TryGetProperty("sub", out var subProp)
                || !subProp.TryGetProperty("allow", out var allowProp))
                return null;

            var sessionId = "";
            foreach (var item in allowProp.EnumerateArray())
            {
                var m = Regex.Match(item.GetString() ?? "", @"^eds\.notify\.([a-f0-9-]+)\.");
                if (m.Success) { sessionId = m.Groups[1].Value; break; }
            }
            if (string.IsNullOrEmpty(sessionId)) return null;

            var remaining = expTime - DateTimeOffset.UtcNow;
            Log.Information("[server] Resuming session {SessionId} (credential valid for {Days}d {Hours}h).",
                sessionId, (int)remaining.TotalDays, remaining.Hours);
            return (sessionId, lastCredsFile);
        }
        catch (Exception ex)
        {
            Log.Debug("[server] Could not parse cached credential: {Message}", ex.Message);
            return null;
        }
    }

    /// <summary>
    /// POSTs session start info to the Shopmonkey API, receives a session ID and
    /// NATS credentials, writes the credentials to disk, and returns both.
    /// </summary>
    public static async Task<(string sessionId, string credsFile)> SendStartAsync(
        string apiUrl,
        string apiKey,
        string serverId,
        string? driverUrl,
        string dataDir,
        DriverRegistry registry,
        CancellationToken ct)
    {
        // Gather machine info concurrently — GetMachineId spawns a subprocess on macOS
        // and GetLocalIp enumerates network interfaces; both can be slow sequentially.
        var machineIdTask = Task.Run(GetMachineId, ct);
        var ipAddress     = GetLocalIp();
        var machineId     = await machineIdTask;

        var body = new Dictionary<string, object?>
        {
            ["version"]       = EdsVersion.Current,
            ["hostname"]      = Dns.GetHostName(),
            ["ipAddress"]     = ipAddress,
            ["machineId"]     = machineId,
            ["osinfo"]        = BuildOsInfo(machineId),
            ["serverId"]      = serverId,
            ["supportsImport"] = true,  // Advertise that HQ can trigger data import via the import notification
        };

        if (!string.IsNullOrEmpty(driverUrl))
        {
            try
            {
                var scheme = new Uri(driverUrl).Scheme;
                var meta   = registry.GetAllMetadata().FirstOrDefault(m =>
                    m.Scheme.Equals(scheme, StringComparison.OrdinalIgnoreCase));
                if (meta is not null)
                {
                    body["driver"] = new
                    {
                        id          = meta.Scheme,
                        name        = meta.Name,
                        description = meta.Description,
                        url         = MaskUrl(driverUrl),
                    };
                }
            }
            catch { /* malformed URL — omit driver metadata */ }
        }

        var json     = JsonSerializer.Serialize(body);
        var response = await RetryHelper.ExecuteAsync(
            innerCt =>
            {
                var req = NewRequest(HttpMethod.Post, $"{apiUrl.TrimEnd('/')}/v3/eds/internal", apiKey);
                req.Content = new StringContent(json, Encoding.UTF8, "application/json");
                return _sharedHttp.SendAsync(req, innerCt);
            },
            operationName: "session start", ct: ct);

        if (response.StatusCode == HttpStatusCode.Conflict)
            throw new InvalidOperationException(
                "Another EDS server is already running for this server ID. Retrying in 5 seconds.");

        response.EnsureSuccessStatusCode();

        var respJson = await response.Content.ReadAsStringAsync(ct);
        using var doc = JsonDocument.Parse(respJson);
        var root = doc.RootElement;

        if (!root.GetProperty("success").GetBoolean())
        {
            var msg = root.TryGetProperty("message", out var m) ? m.GetString() : null;
            throw new InvalidOperationException(msg ?? "Session start failed.");
        }

        var data       = root.GetProperty("data");
        var sessionId  = data.GetProperty("sessionId").GetString()
            ?? throw new InvalidOperationException("No sessionId in session start response.");
        var credential = data.GetProperty("credential").GetString()
            ?? throw new InvalidOperationException("No credential in session start response.");

        // Validate sessionId format before using in path construction to prevent traversal.
        if (!System.Text.RegularExpressions.Regex.IsMatch(sessionId, @"^[a-f0-9\-]+$"))
            throw new InvalidOperationException($"Received invalid session ID format from API: '{sessionId}'.");

        // Decode base64 NATS credential and write to disk
        var sessionDir = Path.Combine(dataDir, sessionId);
        // Confirm the resolved path is still inside dataDir (defense in depth).
        var canonicalSession = Path.GetFullPath(sessionDir);
        var canonicalData    = Path.GetFullPath(dataDir);
        if (!canonicalSession.StartsWith(canonicalData + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            throw new InvalidOperationException("Session directory path escaped data directory.");
        Directory.CreateDirectory(sessionDir);
        var credsFile  = Path.Combine(sessionDir, "nats.creds");
        await File.WriteAllBytesAsync(credsFile, Convert.FromBase64String(credential), ct);
        if (!OperatingSystem.IsWindows())
            File.SetUnixFileMode(credsFile, UnixFileMode.UserRead | UnixFileMode.UserWrite); // chmod 600

        return (sessionId, credsFile);
    }

    /// <summary>
    /// POSTs session end to the Shopmonkey API.
    /// </summary>
    public static async Task SendEndAsync(
        string apiUrl,
        string apiKey,
        string sessionId,
        bool errored,
        CancellationToken ct)
    {
        try
        {
            var body = JsonSerializer.Serialize(new { errored });
            var req = NewRequest(HttpMethod.Post,
                $"{apiUrl.TrimEnd('/')}/v3/eds/internal/{Uri.EscapeDataString(sessionId)}", apiKey);
            req.Content = new StringContent(body, Encoding.UTF8, "application/json");
            var response = await _sharedHttp.SendAsync(req, ct);
            response.EnsureSuccessStatusCode();
        }
        catch
        {
            /* best-effort — don't fail shutdown because of this */
        }
    }

    /// <summary>
    /// Resolves the Shopmonkey API URL from the JWT token's issuer claim.
    /// Falls back to production if the token cannot be decoded.
    /// </summary>
    public static string GetApiUrlFromJwt(string token)
    {
        try
        {
            var parts = token.Split('.');
            if (parts.Length != 3)
                throw new FormatException("Not a valid JWT.");

            var payload = Base64UrlDecode(parts[1]);
            using var doc = JsonDocument.Parse(payload);

            if (doc.RootElement.TryGetProperty("iss", out var iss))
            {
                var issStr = iss.GetString()?.TrimEnd('/');
                if (!string.IsNullOrEmpty(issStr))
                    return issStr;
            }
        }
        catch { /* fall through to default */ }

        return "https://api.shopmonkey.cloud";
    }

    /// <summary>Returns the NATS server URL, adjusted for local API environments.</summary>
    public static string GetNatsUrl(string apiUrl, string? configuredNatsUrl)
    {
        if (!string.IsNullOrEmpty(configuredNatsUrl))
            return configuredNatsUrl;
        if (apiUrl.Contains("localhost", StringComparison.OrdinalIgnoreCase))
            return "nats://localhost:4222";
        return DefaultNatsUrl;
    }

    // ── Private helpers ────────────────────────────────────────────────────────

    private static string GetLocalIp()
    {
        try
        {
            // Enumerate network interfaces directly — avoids a DNS/mDNS round-trip
            // that can block for 1–3 s on macOS.
            return System.Net.NetworkInformation.NetworkInterface
                .GetAllNetworkInterfaces()
                .Where(ni =>
                    ni.OperationalStatus == System.Net.NetworkInformation.OperationalStatus.Up &&
                    ni.NetworkInterfaceType != System.Net.NetworkInformation.NetworkInterfaceType.Loopback)
                .SelectMany(ni => ni.GetIPProperties().UnicastAddresses)
                .Where(a => a.Address.AddressFamily == AddressFamily.InterNetwork)
                .Select(a => a.Address.ToString())
                .FirstOrDefault() ?? "unknown";
        }
        catch { return "unknown"; }
    }

    private static string GetMachineId()
    {
        try
        {
            if (OperatingSystem.IsLinux() && File.Exists("/etc/machine-id"))
                return File.ReadAllText("/etc/machine-id").Trim();

            if (OperatingSystem.IsMacOS())
            {
                var psi = new ProcessStartInfo("ioreg", "-rd1 -c IOPlatformExpertDevice")
                    { RedirectStandardOutput = true, UseShellExecute = false };
                using var proc = Process.Start(psi);
                if (proc is not null)
                {
                    var output = proc.StandardOutput.ReadToEnd();
                    if (!proc.WaitForExit(5_000))
                    {
                        try { proc.Kill(); } catch { /* best-effort */ }
                    }
                    var match = Regex.Match(output,
                        @"IOPlatformUUID""\s*=\s*""([^""]+)""");
                    if (match.Success) return match.Groups[1].Value;
                }
            }
        }
        catch { /* fall through */ }

        // Fallback: hostname-based deterministic ID
        return Convert.ToHexString(
            System.Security.Cryptography.SHA256.HashData(
                Encoding.UTF8.GetBytes(Dns.GetHostName())))[..16];
    }

    /// <summary>
    /// Uploads recent log files from <paramref name="dataDir"/> to the Shopmonkey API
    /// so that support can retrieve them remotely without SSH access to the EDS host.
    /// Mirrors the Go sendLogs / getLogUploadURL / uploadLogFile flow:
    ///   1. POST /v3/eds/internal/{sessionId}/log  → pre-signed upload URL
    ///   2. Gzip the most-recent log file
    ///   3. PUT the .gz to the pre-signed URL (Content-Type: application/x-tgz)
    /// </summary>
    public static async Task<(bool success, string? error)> SendLogsAsync(
        string apiUrl,
        string apiKey,
        string sessionId,
        string dataDir,
        CancellationToken ct)
    {
        try
        {
            // Pick the most-recently-modified .log file (Go uploads one at a time).
            var logFile = Directory.GetFiles(dataDir, "*.log", SearchOption.TopDirectoryOnly)
                .Select(f => new FileInfo(f))
                .OrderByDescending(f => f.LastWriteTimeUtc)
                .FirstOrDefault();

            if (logFile is null)
            {
                Log.Debug("[sendlogs] No log files found in {DataDir}.", dataDir);
                return (true, null);
            }

            // Step 1: request a pre-signed upload URL from HQ.
            var urlReq = NewRequest(HttpMethod.Post,
                $"{apiUrl.TrimEnd('/')}/v3/eds/internal/{Uri.EscapeDataString(sessionId)}/log", apiKey);
            urlReq.Content = new StringContent("{}", Encoding.UTF8, "application/json");
            var urlResp = await _sharedHttp.SendAsync(urlReq, ct);
            urlResp.EnsureSuccessStatusCode();

            var urlJson = await urlResp.Content.ReadAsStringAsync(ct);
            using var urlDoc  = JsonDocument.Parse(urlJson);
            var uploadUrl = urlDoc.RootElement
                .GetProperty("data").GetProperty("url").GetString()
                ?? throw new InvalidOperationException("No upload URL in response.");

            // Step 2: gzip the log file to a temp path.
            var gzPath = logFile.FullName + ".gz";
            try
            {
                await using (var src  = new FileStream(logFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                await using (var dst  = File.Create(gzPath))
                await using (var gz   = new System.IO.Compression.GZipStream(dst, System.IO.Compression.CompressionLevel.Optimal))
                    await src.CopyToAsync(gz, ct);

                // Step 3: PUT the gzipped file to the pre-signed URL.
                await using var gzStream = File.OpenRead(gzPath);
                var putContent = new StreamContent(gzStream);
                putContent.Headers.ContentType =
                    new System.Net.Http.Headers.MediaTypeHeaderValue("application/x-tgz");

                using var putReq = new HttpRequestMessage(HttpMethod.Put, uploadUrl) { Content = putContent };
                var putResp = await _sharedHttp.SendAsync(putReq, ct);
                putResp.EnsureSuccessStatusCode();
            }
            finally
            {
                if (File.Exists(gzPath)) File.Delete(gzPath);
            }

            Log.Information("[sendlogs] Uploaded {File} to HQ.", logFile.Name);
            return (true, null);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "[sendlogs] Failed to upload logs to HQ.");
            return (false, ex.Message);
        }
    }

    /// <summary>
    /// Sends a lightweight heartbeat to the Shopmonkey API to confirm the session
    /// is still alive and to surface current runtime stats (version, paused state).
    /// </summary>
    public static async Task SendHeartbeatAsync(
        string apiUrl,
        string apiKey,
        string sessionId,
        CancellationToken ct)
    {
        try
        {
            var body = JsonSerializer.Serialize(new { version = EdsVersion.Current });
            var req = NewRequest(HttpMethod.Post,
                $"{apiUrl.TrimEnd('/')}/v3/eds/internal/{Uri.EscapeDataString(sessionId)}/heartbeat", apiKey);
            req.Content = new StringContent(body, Encoding.UTF8, "application/json");
            await _sharedHttp.SendAsync(req, ct);
            // Heartbeat is best-effort — ignore non-success responses silently.
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Log.Debug("[heartbeat] Failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Reads the NATS credential file and returns its expiry time,
    /// or <see cref="DateTimeOffset.MaxValue"/> if the expiry cannot be determined.
    /// </summary>
    public static DateTimeOffset GetCredentialExpiry(string credsFile)
    {
        try
        {
            var content  = File.ReadAllText(credsFile);
            var jwtMatch = Regex.Match(content,
                @"-----BEGIN NATS USER JWT-----\s*(.+?)\s*-+END NATS USER JWT-+",
                RegexOptions.Singleline);
            if (!jwtMatch.Success) return DateTimeOffset.MaxValue;

            var parts = jwtMatch.Groups[1].Value.Trim().Split('.');
            if (parts.Length != 3) return DateTimeOffset.MaxValue;

            using var doc = JsonDocument.Parse(Base64UrlDecode(parts[1]));
            if (!doc.RootElement.TryGetProperty("nats", out var natsProp)
                || !natsProp.TryGetProperty("exp", out var expProp))
                return DateTimeOffset.MaxValue;

            return DateTimeOffset.FromUnixTimeSeconds(expProp.GetInt64());
        }
        catch { return DateTimeOffset.MaxValue; }
    }

    /// <summary>
    /// Builds the osinfo payload to match the Go binary's structure:
    /// { host: { hostname, uptime, bootTime, os, platform, platformVersion,
    ///           kernelVersion, kernelArch, hostId, ... }, num_cpu, go_version }
    /// </summary>
    private static object BuildOsInfo(string hostId)
    {
        var uptimeSeconds = Environment.TickCount64 / 1000L;
        var bootTimeUnix  = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - uptimeSeconds;
        var platform      = GetPlatformString();
        var kernelVersion = GetKernelVersion();

        return new
        {
            host = new
            {
                hostname             = Dns.GetHostName(),
                uptime               = uptimeSeconds,
                bootTime             = bootTimeUnix,
                procs                = 0,
                os                   = platform,
                platform             = platform,
                platformFamily       = "Standalone Workstation",
                platformVersion      = Environment.OSVersion.Version.ToString(),
                kernelVersion        = kernelVersion,
                kernelArch           = GetArchString(),
                virtualizationSystem = "",
                virtualizationRole   = "",
                hostId               = hostId,
            },
            num_cpu    = Environment.ProcessorCount,
            go_version = Environment.Version.ToString(),  // .NET runtime version
        };
    }

    private static string GetPlatformString() =>
        RuntimeInformation.IsOSPlatform(OSPlatform.OSX)     ? "darwin"  :
        RuntimeInformation.IsOSPlatform(OSPlatform.Linux)   ? "linux"   :
        RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "windows" : "unknown";

    private static string GetArchString() => RuntimeInformation.OSArchitecture switch
    {
        Architecture.Arm64 => "arm64",
        Architecture.X64   => "x86_64",
        Architecture.X86   => "i386",
        Architecture.Arm   => "arm",
        _                  => RuntimeInformation.OSArchitecture.ToString().ToLowerInvariant()
    };

    private static string GetKernelVersion()
    {
        // On macOS, OSDescription is "Darwin 24.6.0 Darwin Kernel Version..."
        // Extract the kernel version number (second token).
        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            var parts = RuntimeInformation.OSDescription.Split(' ');
            if (parts.Length > 1) return parts[1];
        }
        return Environment.OSVersion.Version.ToString();
    }

    private static HttpRequestMessage NewRequest(HttpMethod method, string url, string apiKey)
    {
        var req = new HttpRequestMessage(method, url);
        req.Headers.Authorization =
            new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", apiKey);
        req.Headers.UserAgent.ParseAdd(
            $"Shopmonkey EDS Server/{EdsVersion.Current}");
        return req;
    }

    internal static string MaskUrl(string url)
    {
        try
        {
            var b = new UriBuilder(url);
            if (!string.IsNullOrEmpty(b.Password)) b.Password = "***";
            return b.Uri.ToString();
        }
        catch { return url; }
    }

    private static byte[] Base64UrlDecode(string input)
    {
        var padded = input.Replace('-', '+').Replace('_', '/');
        padded += (padded.Length % 4) switch { 2 => "==", 3 => "=", _ => "" };
        return Convert.FromBase64String(padded);
    }
}
