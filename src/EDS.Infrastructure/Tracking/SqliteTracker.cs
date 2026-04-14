using EDS.Core.Abstractions;
using EDS.Core.Models;
using Microsoft.Data.Sqlite;

namespace EDS.Infrastructure.Tracking;

/// <summary>
/// Persistent key-value store backed by SQLite in WAL mode.
/// Also implements <see cref="IDlqWriter"/> so the same database file holds
/// both operational state and the dead-letter queue.
/// </summary>
public sealed class SqliteTracker : ITracker, IDlqWriter
{
    private readonly SqliteConnection _conn;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private volatile bool _disposed;

    public SqliteTracker(string dbPath)
    {
        var connStr = new SqliteConnectionStringBuilder
        {
            DataSource = dbPath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Shared
        }.ToString();

        _conn = new SqliteConnection(connStr);
        _conn.Open();
        InitSchema();
    }

    private void InitSchema()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            CREATE TABLE IF NOT EXISTS kv (
                key   TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL
            ) STRICT;
            CREATE TABLE IF NOT EXISTS dlq (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                failed_at   TEXT    NOT NULL,
                event_id    TEXT    NOT NULL,
                table_name  TEXT    NOT NULL,
                operation   TEXT    NOT NULL,
                company_id  TEXT,
                location_id TEXT,
                retry_count INTEGER NOT NULL DEFAULT 0,
                error       TEXT    NOT NULL,
                payload     TEXT
            ) STRICT;
            """;
        cmd.ExecuteNonQuery();
    }

    public async Task<string?> GetKeyAsync(string key, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _lock.WaitAsync(ct);
        try
        {
            using var cmd = _conn.CreateCommand();
            cmd.CommandText = "SELECT value FROM kv WHERE key = @key";
            cmd.Parameters.AddWithValue("@key", key);
            var result = await cmd.ExecuteScalarAsync(ct);
            return result is DBNull or null ? null : (string)result;
        }
        finally { _lock.Release(); }
    }

    public async Task SetKeyAsync(string key, string value, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _lock.WaitAsync(ct);
        try
        {
            using var cmd = _conn.CreateCommand();
            cmd.CommandText = "INSERT OR REPLACE INTO kv(key, value) VALUES(@key, @value)";
            cmd.Parameters.AddWithValue("@key", key);
            cmd.Parameters.AddWithValue("@value", value);
            await cmd.ExecuteNonQueryAsync(ct);
        }
        finally { _lock.Release(); }
    }

    public async Task SetKeysAsync(IReadOnlyDictionary<string, string> pairs, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _lock.WaitAsync(ct);
        try
        {
            using var tx = _conn.BeginTransaction();
            using var cmd = _conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "INSERT OR REPLACE INTO kv(key, value) VALUES(@key, @value)";
            var kp = cmd.Parameters.Add("@key", SqliteType.Text);
            var vp = cmd.Parameters.Add("@value", SqliteType.Text);
            foreach (var (k, v) in pairs)
            {
                kp.Value = k;
                vp.Value = v;
                await cmd.ExecuteNonQueryAsync(ct);
            }
            tx.Commit();
        }
        finally { _lock.Release(); }
    }

    public async Task DeleteKeyAsync(string key, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _lock.WaitAsync(ct);
        try
        {
            using var cmd = _conn.CreateCommand();
            cmd.CommandText = "DELETE FROM kv WHERE key = @key";
            cmd.Parameters.AddWithValue("@key", key);
            await cmd.ExecuteNonQueryAsync(ct);
        }
        finally { _lock.Release(); }
    }

    public async Task<int> DeleteKeysWithPrefixAsync(string prefix, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _lock.WaitAsync(ct);
        try
        {
            using var cmd = _conn.CreateCommand();
            cmd.CommandText = "DELETE FROM kv WHERE key LIKE @prefix || '%'";
            cmd.Parameters.AddWithValue("@prefix", prefix);
            return await cmd.ExecuteNonQueryAsync(ct);
        }
        finally { _lock.Release(); }
    }

    // ── IDlqWriter ────────────────────────────────────────────────────────────

    /// <summary>
    /// Persists all events in <paramref name="events"/> to the <c>dlq</c> table in a
    /// single transaction. Payload is the raw After (or Before for deletes) JSON text.
    /// </summary>
    public async Task PushAsync(
        IEnumerable<DbChangeEvent> events,
        string error,
        int retryCount,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _lock.WaitAsync(ct);
        try
        {
            var failedAt   = DateTimeOffset.UtcNow.ToString("O");
            var errorTrunc = error.Length > 2000 ? error[..2000] : error;

            using var tx  = _conn.BeginTransaction();
            using var cmd = _conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = """
                INSERT INTO dlq
                    (failed_at, event_id, table_name, operation,
                     company_id, location_id, retry_count, error, payload)
                VALUES
                    (@failed_at, @event_id, @table_name, @operation,
                     @company_id, @location_id, @retry_count, @error, @payload)
                """;

            var pFailedAt   = cmd.Parameters.Add("@failed_at",   SqliteType.Text);
            var pEventId    = cmd.Parameters.Add("@event_id",    SqliteType.Text);
            var pTableName  = cmd.Parameters.Add("@table_name",  SqliteType.Text);
            var pOperation  = cmd.Parameters.Add("@operation",   SqliteType.Text);
            var pCompanyId  = cmd.Parameters.Add("@company_id",  SqliteType.Text);
            var pLocationId = cmd.Parameters.Add("@location_id", SqliteType.Text);
            var pRetryCount = cmd.Parameters.Add("@retry_count", SqliteType.Integer);
            var pError      = cmd.Parameters.Add("@error",       SqliteType.Text);
            var pPayload    = cmd.Parameters.Add("@payload",     SqliteType.Text);

            pFailedAt.Value   = failedAt;
            pError.Value      = errorTrunc;
            pRetryCount.Value = retryCount;

            foreach (var evt in events)
            {
                pEventId.Value    = evt.Id;
                pTableName.Value  = evt.Table;
                pOperation.Value  = evt.Operation;
                pCompanyId.Value  = (object?)evt.CompanyId  ?? DBNull.Value;
                pLocationId.Value = (object?)evt.LocationId ?? DBNull.Value;

                string? payload = evt.After?.GetRawText() ?? evt.Before?.GetRawText();
                pPayload.Value = (object?)payload ?? DBNull.Value;

                await cmd.ExecuteNonQueryAsync(ct);
            }

            tx.Commit();
        }
        finally { _lock.Release(); }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        await _lock.WaitAsync();
        try { _conn.Dispose(); }
        finally
        {
            _lock.Release();
            // Do NOT dispose _lock: SemaphoreSlim holds no unmanaged resources, and
            // disposing it while a racing caller is mid-WaitAsync throws ObjectDisposedException.
            // The volatile _disposed flag is the correct guard; GC handles reclamation.
        }
    }
}
