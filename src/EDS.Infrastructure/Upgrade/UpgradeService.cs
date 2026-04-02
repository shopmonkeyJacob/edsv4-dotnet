using EDS.Core.Utilities;
using Microsoft.Extensions.Logging;
using PgpCore;
using System.IO.Compression;
using System.Security;

namespace EDS.Infrastructure.Upgrade;

public sealed class UpgradeConfig
{
    public required string BinaryUrl { get; init; }
    public required string SignatureUrl { get; init; }
    public required string PublicKey { get; init; }
    public required string DestinationPath { get; init; }
}

/// <summary>
/// Securely downloads, PGP-verifies, and installs a new EDS binary.
/// Mirrors the Go upgrade.go implementation.
/// </summary>
public sealed class UpgradeService
{
    private readonly ILogger<UpgradeService> _logger;
    private readonly HttpClient _http;

    public UpgradeService(ILogger<UpgradeService> logger, HttpClient http)
    {
        _logger = logger;
        _http = http;
    }

    public async Task UpgradeAsync(UpgradeConfig config, CancellationToken ct = default)
    {
        var tmpBinary = Path.GetTempFileName();
        var tmpSig = Path.GetTempFileName();
        try
        {
            _logger.LogInformation("Downloading new binary from {Url}", config.BinaryUrl);
            await DownloadFileAsync(config.BinaryUrl, tmpBinary, ct);

            _logger.LogInformation("Downloading signature from {Url}", config.SignatureUrl);
            await DownloadFileAsync(config.SignatureUrl, tmpSig, ct);

            _logger.LogInformation("Verifying PGP signature...");
            await VerifySignatureAsync(tmpBinary, tmpSig, config.PublicKey, ct);

            _logger.LogInformation("Extracting binary to {Dest}", config.DestinationPath);
            ExtractBinary(tmpBinary, config.DestinationPath);

            // Make executable on Unix
            if (!OperatingSystem.IsWindows())
            {
                File.SetUnixFileMode(config.DestinationPath,
                    UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute |
                    UnixFileMode.GroupRead | UnixFileMode.GroupExecute |
                    UnixFileMode.OtherRead | UnixFileMode.OtherExecute);
            }

            _logger.LogInformation("Upgrade complete. Restart to use the new version.");
        }
        catch
        {
            // Clean up temp files on failure
            TryDelete(tmpBinary);
            TryDelete(tmpSig);
            throw;
        }
    }

    private async Task DownloadFileAsync(string url, string destination, CancellationToken ct)
    {
        using var response = await RetryHelper.ExecuteAsync(
            innerCt => _http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, innerCt),
            _logger, operationName: $"download {Path.GetFileName(url)}", ct: ct);
        response.EnsureSuccessStatusCode();

        await using var stream = await response.Content.ReadAsStreamAsync(ct);
        await using var file = File.Create(destination);
        await stream.CopyToAsync(file, ct);
    }

    private static async Task VerifySignatureAsync(
        string filePath,
        string sigPath,
        string armoredPublicKey,
        CancellationToken ct)
    {
        var encryptionKeys = new EncryptionKeys(armoredPublicKey);
        using var pgp = new PGP(encryptionKeys);
        await using var sigStream = File.OpenRead(sigPath);
        bool valid = await pgp.VerifyStreamAsync(sigStream, throwIfEncrypted: false);
        if (!valid)
            throw new SecurityException("PGP signature verification failed. The binary may have been tampered with.");
    }

    private static void ExtractBinary(string archivePath, string destination)
    {
        // Detect archive type by magic bytes
        using var fs = File.OpenRead(archivePath);
        Span<byte> magic = stackalloc byte[4];
        fs.Read(magic);
        fs.Seek(0, SeekOrigin.Begin);

        if (magic[0] == 0x50 && magic[1] == 0x4B) // PK → ZIP
        {
            ExtractFromZip(fs, destination);
        }
        else if (magic[0] == 0x1F && magic[1] == 0x8B) // gzip magic
        {
            ExtractFromTarGz(fs, destination);
        }
        else
        {
            // Plain binary
            fs.Seek(0, SeekOrigin.Begin);
            using var dest = File.Create(destination);
            fs.CopyTo(dest);
        }
    }

    private static void ExtractFromZip(Stream source, string destination)
    {
        using var zip = new ZipArchive(source, ZipArchiveMode.Read);
        var entry = zip.Entries.FirstOrDefault(e =>
            e.Name.EndsWith(".exe", StringComparison.OrdinalIgnoreCase) ||
            e.Name.Equals("eds", StringComparison.OrdinalIgnoreCase))
            ?? zip.Entries.First();

        using var entryStream = entry.Open();
        using var dest = File.Create(destination);
        entryStream.CopyTo(dest);
    }

    private static void ExtractFromTarGz(Stream source, string destination)
    {
        using var gz = new GZipStream(source, CompressionMode.Decompress);
        // Minimal TAR reader — skip 512-byte header blocks
        var header = new byte[512];
        using var dest = File.Create(destination);

        while (gz.Read(header, 0, 512) == 512)
        {
            if (header[0] == 0) break; // end of archive

            var nameBytes = header.AsSpan(0, 100);
            var name = System.Text.Encoding.ASCII.GetString(nameBytes).TrimEnd('\0');
            var sizeStr = System.Text.Encoding.ASCII.GetString(header, 124, 12).TrimEnd('\0').Trim();
            long size = sizeStr.Length > 0 ? Convert.ToInt64(sizeStr, 8) : 0;

            bool isTarget = Path.GetFileName(name).Equals("eds", StringComparison.OrdinalIgnoreCase)
                || Path.GetFileName(name).EndsWith(".exe", StringComparison.OrdinalIgnoreCase);

            long remaining = size;
            var buf = new byte[4096];
            while (remaining > 0)
            {
                int toRead = (int)Math.Min(buf.Length, remaining);
                int read = gz.Read(buf, 0, toRead);
                if (read == 0) break;
                if (isTarget) dest.Write(buf, 0, read);
                remaining -= read;
            }

            // Skip to next 512-byte boundary
            long padding = (512 - (size % 512)) % 512;
            if (padding > 0)
            {
                var skip = new byte[padding];
                gz.Read(skip, 0, (int)padding);
            }

            if (isTarget) return; // found it
        }
    }

    private static void TryDelete(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); } catch { }
    }
}
