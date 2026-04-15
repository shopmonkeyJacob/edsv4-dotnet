using Azure.Identity;
using Azure.Storage;
using Azure.Storage.Blobs;
using EDS.Core.Abstractions;
using EDS.Core.Helpers;
using EDS.Core.Models;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace EDS.Drivers.AzureBlob;

/// <summary>
/// Stores CDC events as JSON blobs in an Azure Blob Storage container.
/// Path pattern: {container}/{prefix}/{table}/{timestamp}-{id}.json
///
/// Supports three auth modes (determined by URL query parameters):
///   key=&lt;base64-account-key&gt;              — storage account key
///   connection-string=&lt;uri-encoded-string&gt; — full connection string (e.g. Azurite)
///   (neither)                              — DefaultAzureCredential (managed identity, env, CLI)
/// </summary>
public sealed class AzureBlobDriver : IDriver, IDriverLifecycle, IDriverHelp, IDriverDirectImport
{
    private BlobContainerClient? _container;
    private string _prefix = string.Empty;
    private readonly List<(DbChangeEvent evt, string blobName)> _pending = new();
    private readonly SemaphoreSlim _semaphore = new(8, 8); // max concurrent uploads

    // ── IDriverHelp ───────────────────────────────────────────────────────────

    public string Name        => "Azure Blob Storage";
    public string Description => "Streams EDS messages as JSON blobs to an Azure Blob Storage container.";
    public string ExampleUrl  => "azureblob://accountname/containername?key=<base64-account-key>";
    public string Help        => "Each event is written as {prefix}/{table}/{timestamp}-{id}.json. " +
                                 "Use 'key' for account-key auth, 'connection-string' for a full " +
                                 "connection string (useful for Azurite and other emulators), or " +
                                 "omit both to use DefaultAzureCredential (managed identity, " +
                                 "AZURE_* environment variables, Azure CLI, etc.).";

    public int MaxBatchSize => -1;

    // ── IDriverLifecycle ──────────────────────────────────────────────────────

    public Task StartAsync(DriverConfig config, CancellationToken ct = default)
    {
        (_container, _prefix) = BuildContainerClient(config.Url);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken ct = default)
    {
        _pending.Clear();
        return Task.CompletedTask;
    }

    // ── IDriver ───────────────────────────────────────────────────────────────

    public Task<bool> ProcessAsync(ILogger logger, DbChangeEvent evt, CancellationToken ct = default)
    {
        _pending.Add((evt, BlobPath(evt.Table, $"{evt.Timestamp}-{evt.Id}.json")));
        return Task.FromResult(false);
    }

    public async Task FlushAsync(ILogger logger, CancellationToken ct = default)
    {
        await Task.WhenAll(_pending.Select(p => UploadEventAsync(p.evt, p.blobName, ct)));
        logger.LogDebug("Flushed {Count} blob(s) to container '{Container}'.",
            _pending.Count, _container!.Name);
        _pending.Clear();
    }

    public async Task TestAsync(ILogger logger, string url, CancellationToken ct = default)
    {
        var (container, _) = BuildContainerClient(url);
        // GetProperties throws if the container doesn't exist or credentials are wrong.
        await container.GetPropertiesAsync(cancellationToken: ct);
    }

    // ── IDriverDirectImport ───────────────────────────────────────────────────

    public Task InitForDirectImportAsync(ILogger logger, string url, CancellationToken ct = default)
    {
        (_container, _prefix) = BuildContainerClient(url);
        return Task.CompletedTask;
    }

    public async Task ImportFilesAsync(
        ILogger logger,
        IReadOnlyList<(string Table, string FilePath)> files,
        CancellationToken ct = default)
    {
        long bytes = 0;
        int  done  = 0;

        await Parallel.ForEachAsync(files, ct, async (entry, innerCt) =>
        {
            await _semaphore.WaitAsync(innerCt);
            try
            {
                var (table, filePath) = entry;
                var blobName   = BlobPath(table, Path.GetFileName(filePath));
                var blobClient = _container!.GetBlobClient(blobName);

                await using var stream = File.OpenRead(filePath);
                await blobClient.UploadAsync(stream, overwrite: true, innerCt);

                Interlocked.Add(ref bytes, new FileInfo(filePath).Length);
                var n = Interlocked.Increment(ref done);
                logger.LogDebug("[import] Uploaded {N}/{Total} — {Blob}", n, files.Count, blobName);
            }
            finally
            {
                _semaphore.Release();
            }
        });

        logger.LogInformation(
            "[import] Uploaded {Count} file(s) ({Bytes:N0} bytes) to container '{Container}'.",
            done, bytes, _container!.Name);
    }

    // ── IDriverHelp: configuration form + validation ──────────────────────────

    public IReadOnlyList<DriverField> Configuration() =>
    [
        DriverFieldHelpers.RequiredString("Account",          "Azure storage account name"),
        DriverFieldHelpers.RequiredString("Container",        "Blob container name"),
        DriverFieldHelpers.OptionalString("Prefix",           "Blob path prefix (folder)"),
        DriverFieldHelpers.OptionalPassword("AccountKey",     "Base64-encoded storage account key"),
        DriverFieldHelpers.OptionalString("ConnectionString", "Full connection string (overrides Account + AccountKey)"),
        DriverFieldHelpers.OptionalString("Endpoint",         "Custom service endpoint (e.g. http://127.0.0.1:10000/devstoreaccount1 for Azurite)")
    ];

    public (string url, IReadOnlyList<FieldError> errors) Validate(Dictionary<string, object?> values)
    {
        try
        {
            var account   = DriverFieldHelpers.GetRequiredString("Account",    values);
            var container = DriverFieldHelpers.GetRequiredString("Container",  values);
            var prefix    = DriverFieldHelpers.GetOptionalString("Prefix",           string.Empty, values);
            var key       = DriverFieldHelpers.GetOptionalString("AccountKey",       string.Empty, values);
            var connStr   = DriverFieldHelpers.GetOptionalString("ConnectionString", string.Empty, values);
            var endpoint  = DriverFieldHelpers.GetOptionalString("Endpoint",         string.Empty, values);

            var path = string.IsNullOrEmpty(prefix)
                ? $"{account}/{container}"
                : $"{account}/{container}/{prefix.TrimStart('/')}";

            var qs = new List<string>();
            if (connStr.Length > 0) qs.Add($"connection-string={Uri.EscapeDataString(connStr)}");
            else if (key.Length > 0) qs.Add($"key={Uri.EscapeDataString(key)}");
            if (endpoint.Length > 0) qs.Add($"endpoint={Uri.EscapeDataString(endpoint)}");

            return ($"azureblob://{path}" + (qs.Count > 0 ? $"?{string.Join("&", qs)}" : ""), []);
        }
        catch (Exception ex)
        {
            return (string.Empty, [new FieldError { Field = "Account", Message = ex.Message }]);
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private string BlobPath(string table, string filename) =>
        string.IsNullOrEmpty(_prefix)
            ? $"{table}/{filename}"
            : $"{_prefix.TrimEnd('/')}/{table}/{filename}";

    private async Task UploadEventAsync(DbChangeEvent evt, string blobName, CancellationToken ct)
    {
        await _semaphore.WaitAsync(ct);
        try
        {
            var blobClient = _container!.GetBlobClient(blobName);
            var json = JsonSerializer.Serialize(evt);
            await blobClient.UploadAsync(BinaryData.FromString(json), overwrite: true, ct);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private static (BlobContainerClient container, string prefix) BuildContainerClient(string url)
    {
        var uri  = new Uri(url);
        var qs   = System.Web.HttpUtility.ParseQueryString(uri.Query);

        // Extract container and optional prefix from the URL path
        var pathParts     = uri.AbsolutePath.TrimStart('/').Split('/', 2);
        var containerName = pathParts.Length > 0 ? pathParts[0] : string.Empty;
        var prefix        = pathParts.Length > 1 ? pathParts[1] : string.Empty;

        var connStr  = qs["connection-string"];
        var key      = qs["key"];
        var endpoint = qs["endpoint"];

        BlobServiceClient service;

        if (!string.IsNullOrEmpty(connStr))
        {
            // Full connection string — works for Azurite and standard Azure
            service = new BlobServiceClient(Uri.UnescapeDataString(connStr));
        }
        else
        {
            var accountName = uri.Host;
            var serviceUri  = !string.IsNullOrEmpty(endpoint)
                ? new Uri(Uri.UnescapeDataString(endpoint))
                : new Uri($"https://{accountName}.blob.core.windows.net");

            if (!string.IsNullOrEmpty(key))
            {
                service = new BlobServiceClient(serviceUri,
                    new StorageSharedKeyCredential(accountName, Uri.UnescapeDataString(key)));
            }
            else
            {
                // No explicit credentials — fall back to DefaultAzureCredential.
                // This supports managed identity (Azure VMs, AKS, App Service),
                // AZURE_* environment variables, Visual Studio / Azure CLI auth, etc.
                service = new BlobServiceClient(serviceUri, new DefaultAzureCredential());
            }
        }

        return (service.GetBlobContainerClient(containerName), prefix);
    }
}
