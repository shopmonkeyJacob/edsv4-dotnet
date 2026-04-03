using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using EDS.Core.Abstractions;
using EDS.Core.Helpers;
using EDS.Core.Models;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace EDS.Drivers.S3;

/// <summary>
/// Stores CDC events as JSON files in an S3-compatible bucket.
/// Path: s3://{bucket}/{prefix}/{table}/{timestamp}-{id}.json
/// </summary>
public sealed class S3Driver : IDriver, IDriverLifecycle, IDriverHelp, IDriverDirectImport
{
    private IAmazonS3? _client;
    private string _bucket = string.Empty;
    private string _prefix = string.Empty;
    private readonly List<(DbChangeEvent evt, string key)> _pending = new();
    private readonly SemaphoreSlim _semaphore = new(8, 8); // max concurrent uploads

    public string Name => "Amazon S3";
    public string Description => "Streams EDS messages as JSON files to an S3-compatible bucket.";
    public string ExampleUrl => "s3://my-bucket/prefix?region=us-east-1";
    public string Help => "Each event is written as {bucket}/{prefix}/{table}/{timestamp}-{id}.json. " +
                          "Supports AWS S3, Google Cloud Storage (via custom endpoint), and MinIO.";

    public int MaxBatchSize => -1;

    public Task StartAsync(DriverConfig config, CancellationToken ct = default)
    {
        (_client, _bucket, _prefix) = CreateClient(config.Url);
        return Task.CompletedTask;
    }

    public Task<bool> ProcessAsync(ILogger logger, DbChangeEvent evt, CancellationToken ct = default)
    {
        var tablePath = string.IsNullOrEmpty(_prefix)
            ? evt.Table
            : $"{_prefix.TrimEnd('/')}/{evt.Table}";
        var key = $"{tablePath}/{evt.Timestamp}-{evt.Id}.json";
        _pending.Add((evt, key));
        return Task.FromResult(false);
    }

    public async Task FlushAsync(ILogger logger, CancellationToken ct = default)
    {
        var tasks = _pending.Select(p => UploadWithThrottleAsync(p.evt, p.key, ct)).ToList();
        await Task.WhenAll(tasks);
        logger.LogDebug("Flushed {Count} objects to S3 bucket '{Bucket}'.", _pending.Count, _bucket);
        _pending.Clear();
    }

    private async Task UploadWithThrottleAsync(DbChangeEvent evt, string key, CancellationToken ct)
    {
        await _semaphore.WaitAsync(ct);
        try
        {
            var json = JsonSerializer.Serialize(evt);
            var request = new PutObjectRequest
            {
                BucketName = _bucket,
                Key = key,
                ContentType = "application/json",
                ContentBody = json
            };
            await _client!.PutObjectAsync(request, ct);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public async Task TestAsync(ILogger logger, string url, CancellationToken ct = default)
    {
        var (client, bucket, _) = CreateClient(url);
        await client.GetBucketLocationAsync(bucket, ct);
    }

    public Task StopAsync(CancellationToken ct = default)
    {
        _client?.Dispose();
        _client = null;
        return Task.CompletedTask;
    }

    // ── IDriverDirectImport ───────────────────────────────────────────────────

    public Task InitForDirectImportAsync(ILogger logger, string url, CancellationToken ct = default)
    {
        (_client, _bucket, _prefix) = CreateClient(url);
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
                var tablePath = string.IsNullOrEmpty(_prefix)
                    ? table
                    : $"{_prefix.TrimEnd('/')}/{table}";
                var key = $"{tablePath}/{Path.GetFileName(filePath)}";

                await using var stream = System.IO.File.OpenRead(filePath);
                var request = new PutObjectRequest
                {
                    BucketName  = _bucket,
                    Key         = key,
                    InputStream = stream,
                    ContentType = "application/gzip",
                };
                await _client!.PutObjectAsync(request, innerCt);

                Interlocked.Add(ref bytes, new FileInfo(filePath).Length);
                var n = Interlocked.Increment(ref done);
                logger.LogDebug("[import] Uploaded {N}/{Total} — s3://{Bucket}/{Key}", n, files.Count, _bucket, key);
            }
            finally
            {
                _semaphore.Release();
            }
        });

        logger.LogInformation("[import] Uploaded {Count} file(s) ({Bytes:N0} bytes) to s3://{Bucket}/{Prefix}.",
            done, bytes, _bucket, _prefix);
    }

    public IReadOnlyList<DriverField> Configuration() =>
    [
        DriverFieldHelpers.RequiredString("Bucket", "S3 bucket name"),
        DriverFieldHelpers.OptionalString("Prefix", "Key prefix (folder path)"),
        DriverFieldHelpers.OptionalString("Region", "AWS region (e.g. us-east-1)", "us-east-1"),
        DriverFieldHelpers.OptionalString("AccessKeyId", "AWS access key ID"),
        DriverFieldHelpers.OptionalPassword("SecretAccessKey", "AWS secret access key"),
        DriverFieldHelpers.OptionalString("Endpoint", "Custom endpoint URL (for MinIO, GCS, etc.)")
    ];

    public (string url, IReadOnlyList<FieldError> errors) Validate(Dictionary<string, object?> values)
    {
        try
        {
            var bucket = DriverFieldHelpers.GetRequiredString("Bucket", values);
            var prefix = DriverFieldHelpers.GetOptionalString("Prefix", string.Empty, values);
            var region = DriverFieldHelpers.GetOptionalString("Region", "us-east-1", values);
            var accessKey = DriverFieldHelpers.GetOptionalString("AccessKeyId", string.Empty, values);
            var secretKey = DriverFieldHelpers.GetOptionalString("SecretAccessKey", string.Empty, values);
            var endpoint = DriverFieldHelpers.GetOptionalString("Endpoint", string.Empty, values);

            var qs = new List<string> { $"region={region}" };
            if (accessKey.Length > 0) qs.Add($"access-key-id={Uri.EscapeDataString(accessKey)}");
            if (secretKey.Length > 0) qs.Add($"secret-access-key={Uri.EscapeDataString(secretKey)}");
            if (endpoint.Length > 0) qs.Add($"endpoint={Uri.EscapeDataString(endpoint)}");

            var path = string.IsNullOrEmpty(prefix) ? bucket : $"{bucket}/{prefix.TrimStart('/')}";
            return ($"s3://{path}?{string.Join("&", qs)}", []);
        }
        catch (Exception ex)
        {
            return (string.Empty, [new FieldError { Field = "Bucket", Message = ex.Message }]);
        }
    }

    private static (IAmazonS3 client, string bucket, string prefix) CreateClient(string url)
    {
        var uri = new Uri(url);
        var bucket = uri.Host;
        var prefix = uri.AbsolutePath.TrimStart('/');
        var qs = System.Web.HttpUtility.ParseQueryString(uri.Query);

        var region = qs["region"] ?? "us-east-1";
        var accessKeyId = qs["access-key-id"];
        var secretKey = qs["secret-access-key"];
        var endpoint = qs["endpoint"];

        AmazonS3Config config = new()
        {
            RegionEndpoint = RegionEndpoint.GetBySystemName(region)
        };

        if (!string.IsNullOrEmpty(endpoint))
        {
            config.ServiceURL = endpoint;
            config.ForcePathStyle = true;
        }

        IAmazonS3 client = (accessKeyId is not null && secretKey is not null)
            ? new AmazonS3Client(accessKeyId, secretKey, config)
            : new AmazonS3Client(config);

        return (client, bucket, prefix);
    }
}
