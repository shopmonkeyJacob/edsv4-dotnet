using EDS.Core.Utilities;
using TarFormats = System.Formats.Tar;
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
///
/// Security properties:
/// - PGP signature is verified before any bytes are written to the destination path.
/// - The verified archive bytes are extracted directly from the in-memory buffer that
///   was verified, eliminating the TOCTOU window between verify and extract.
/// - TAR extraction uses System.Formats.Tar (replaces hand-rolled parser) to handle
///   path traversal and symlink entries safely.
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
        _logger.LogInformation("Downloading new binary from {Url}", config.BinaryUrl);
        var archiveBytes = await DownloadBytesAsync(config.BinaryUrl, ct);

        _logger.LogInformation("Downloading signature from {Url}", config.SignatureUrl);
        var sigBytes = await DownloadBytesAsync(config.SignatureUrl, ct);

        // Verify the PGP signature against the downloaded archive bytes.
        // Both buffers are in memory — no temp files, no TOCTOU window.
        _logger.LogInformation("Verifying PGP signature...");
        await VerifySignatureAsync(archiveBytes, sigBytes, config.PublicKey);

        // Extract from the verified in-memory buffer — same bytes that were verified.
        _logger.LogInformation("Extracting binary to {Dest}", config.DestinationPath);
        ExtractBinary(archiveBytes, config.DestinationPath);

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

    private async Task<byte[]> DownloadBytesAsync(string url, CancellationToken ct)
    {
        using var response = await RetryHelper.ExecuteAsync(
            innerCt => _http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, innerCt),
            _logger, operationName: $"download {Path.GetFileName(url)}", ct: ct);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadAsByteArrayAsync(ct);
    }

    private static async Task VerifySignatureAsync(
        byte[] archiveBytes,
        byte[] sigBytes,
        string armoredPublicKey)
    {
        var encryptionKeys = new EncryptionKeys(armoredPublicKey);
        using var pgp = new PGP(encryptionKeys);

        using var dataStream = new MemoryStream(archiveBytes);
        using var sigStream  = new MemoryStream(sigBytes);

        // VerifyAsync with two streams verifies a detached PGP signature:
        // confirms that sigBytes is a valid signature of archiveBytes by the public key.
        bool valid = await pgp.VerifyAsync(dataStream, sigStream);
        if (!valid)
            throw new SecurityException("PGP signature verification failed. The binary may have been tampered with.");
    }

    private static void ExtractBinary(byte[] archiveBytes, string destination)
    {
        // Detect archive type by magic bytes
        if (archiveBytes.Length < 2)
            throw new InvalidDataException("Archive is too small to be a valid binary.");

        using var ms = new MemoryStream(archiveBytes);

        if (archiveBytes[0] == 0x50 && archiveBytes[1] == 0x4B) // PK → ZIP
        {
            ExtractFromZip(ms, destination);
        }
        else if (archiveBytes[0] == 0x1F && archiveBytes[1] == 0x8B) // gzip → tar.gz
        {
            ExtractFromTarGz(ms, destination);
        }
        else
        {
            // Plain binary — write directly
            using var dest = File.Create(destination);
            ms.CopyTo(dest);
        }
    }

    private static void ExtractFromZip(Stream source, string destination)
    {
        using var zip = new ZipArchive(source, ZipArchiveMode.Read);
        var entry = zip.Entries.FirstOrDefault(e =>
            e.Name.EndsWith(".exe", StringComparison.OrdinalIgnoreCase) ||
            e.Name.Equals("eds", StringComparison.OrdinalIgnoreCase))
            ?? zip.Entries.First();

        // Guard against zip-slip: entry.FullName must not escape destination directory.
        var destDir    = Path.GetDirectoryName(destination)!;
        var entryPath  = Path.GetFullPath(Path.Combine(destDir, entry.FullName));
        if (!entryPath.StartsWith(Path.GetFullPath(destDir) + Path.DirectorySeparatorChar, StringComparison.Ordinal)
            && entryPath != Path.GetFullPath(destination))
            throw new InvalidOperationException($"Zip entry '{entry.FullName}' would escape destination directory.");

        using var entryStream = entry.Open();
        using var dest = File.Create(destination);
        entryStream.CopyTo(dest);
    }

    private static void ExtractFromTarGz(Stream source, string destination)
    {
        using var gz     = new GZipStream(source, CompressionMode.Decompress);
        using var reader = new TarFormats.TarReader(gz, leaveOpen: false);

        while (reader.GetNextEntry() is { } entry)
        {
            // Only extract regular files — skip symlinks, hardlinks, directories.
            if (entry.EntryType != TarFormats.TarEntryType.RegularFile
                && entry.EntryType != TarFormats.TarEntryType.V7RegularFile)
                continue;

            var name = Path.GetFileName(entry.Name);
            if (!name.Equals("eds", StringComparison.OrdinalIgnoreCase)
                && !name.EndsWith(".exe", StringComparison.OrdinalIgnoreCase))
                continue;

            // Write to a temp file alongside the destination, then atomic-rename.
            var tmp = destination + ".upgrade.tmp";
            try
            {
                using var dest = File.Create(tmp);
                entry.DataStream!.CopyTo(dest);
                File.Move(tmp, destination, overwrite: true);
            }
            catch
            {
                try { File.Delete(tmp); } catch { }
                throw;
            }
            return;
        }

        throw new InvalidDataException("No EDS binary found in the upgrade archive.");
    }

    private static void TryDelete(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); } catch { }
    }
}
