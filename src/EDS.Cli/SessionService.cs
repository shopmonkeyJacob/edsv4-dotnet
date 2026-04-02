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
            ["osinfo"]        = new { os = RuntimeInformation.OSDescription, arch = RuntimeInformation.OSArchitecture.ToString() },
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

        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
        http.DefaultRequestHeaders.Authorization =
            new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", apiKey);
        http.DefaultRequestHeaders.UserAgent.ParseAdd(
            $"Shopmonkey EDS Server/{EdsVersion.Current}");

        var json     = JsonSerializer.Serialize(body);
        var response = await RetryHelper.ExecuteAsync(
            innerCt => http.PostAsync(
                $"{apiUrl.TrimEnd('/')}/v3/eds/internal",
                new StringContent(json, Encoding.UTF8, "application/json"),
                innerCt),
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

        // Decode base64 NATS credential and write to disk
        var sessionDir = Path.Combine(dataDir, sessionId);
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
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
            http.DefaultRequestHeaders.Authorization =
                new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", apiKey);
            http.DefaultRequestHeaders.UserAgent.ParseAdd(
                $"Shopmonkey EDS Server/{EdsVersion.Current}");

            var body     = JsonSerializer.Serialize(new { errored });
            var response = await http.PostAsync(
                $"{apiUrl.TrimEnd('/')}/v3/eds/internal/{Uri.EscapeDataString(sessionId)}",
                new StringContent(body, Encoding.UTF8, "application/json"),
                ct);

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
                    proc.WaitForExit();
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
    /// Sends at most the 5 most-recently-modified <c>*.log</c> files.
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
            var logFiles = Directory.GetFiles(dataDir, "*.log", SearchOption.TopDirectoryOnly)
                .Select(f => new FileInfo(f))
                .OrderByDescending(f => f.LastWriteTimeUtc)
                .Take(5)
                .ToList();

            if (logFiles.Count == 0)
            {
                Log.Debug("[sendlogs] No log files found in {DataDir}.", dataDir);
                return (true, null);
            }

            using var http = new HttpClient { Timeout = TimeSpan.FromMinutes(2) };
            http.DefaultRequestHeaders.Authorization =
                new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", apiKey);
            http.DefaultRequestHeaders.UserAgent.ParseAdd(
                $"Shopmonkey EDS Server/{EdsVersion.Current}");

            using var form    = new MultipartFormDataContent();
            var openedStreams = new List<Stream>();
            try
            {
                foreach (var file in logFiles)
                {
                    var stream = new FileStream(
                        file.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                    openedStreams.Add(stream);
                    form.Add(new StreamContent(stream), "logs", file.Name);
                }

                var response = await http.PostAsync(
                    $"{apiUrl.TrimEnd('/')}/v3/eds/internal/{Uri.EscapeDataString(sessionId)}/logs",
                    form,
                    ct);

                response.EnsureSuccessStatusCode();
                Log.Information("[sendlogs] Uploaded {Count} log file(s) to HQ.", logFiles.Count);
                return (true, null);
            }
            finally
            {
                foreach (var s in openedStreams) s.Dispose();
            }
        }
        catch (Exception ex)
        {
            Log.Error(ex, "[sendlogs] Failed to upload logs to HQ.");
            return (false, ex.Message);
        }
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
