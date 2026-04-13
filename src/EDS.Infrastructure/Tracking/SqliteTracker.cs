using EDS.Core.Abstractions;
using Microsoft.Data.Sqlite;

namespace EDS.Infrastructure.Tracking;

/// <summary>
/// Persistent key-value store backed by SQLite in WAL mode.
/// Replaces BuntDB from the Go implementation.
/// </summary>
public sealed class SqliteTracker : ITracker
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
