# EDS — Enterprise Data Streaming (.NET)

A .NET port of Shopmonkey's Enterprise Data Streaming server. Connects to Shopmonkey HQ via NATS JetStream and streams change data capture (CDC) events to your own data infrastructure through a configurable driver.

## Supported Drivers

| Scheme       | Destination              | Import |
|--------------|--------------------------|:------:|
| `postgres`   | PostgreSQL               | ✓      |
| `mysql`      | MySQL / MariaDB          | ✓      |
| `sqlserver`  | SQL Server               | ✓      |
| `snowflake`  | Snowflake                | ✓      |
| `s3`         | Amazon S3                | ✓ ²    |
| `azureblob`  | Azure Blob Storage       | ✓ ²    |
| `file`       | Local NDJSON files       | ✓ ²    |
| `kafka`      | Apache Kafka             |        |
| `eventhub`   | Azure Event Hubs         |        |

> ² S3, Azure Blob Storage, and File drivers transfer raw `.ndjson.gz` export files directly
> to the destination, preserving the filename and per-table directory structure. No row-level
> parsing is performed — the export format is already the natural storage format for these drivers.

## Requirements

- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0) (build from source)
- A Shopmonkey account with EDS access

## Quick Start

1. Download the binary for your platform from [Releases](../../releases).
2. Run `eds server` — on first launch you will be prompted for a one-time enrollment
   code from the [Shopmonkey HQ web interface](https://app.shopmonkey.io).
3. Configure your destination driver in the web interface. EDS will begin streaming
   events as soon as a driver URL is saved.

### macOS: running a downloaded binary

macOS Gatekeeper will block unsigned binaries downloaded from the internet. To allow EDS to run, execute the following command once after extracting the archive:

```sh
xattr -d com.apple.quarantine ./EDS.Cli
```

Then make it executable if needed:

```sh
chmod +x ./EDS.Cli
```

Alternatively, open **System Settings → Privacy & Security → Security** and click **Open Anyway** after the first blocked launch attempt.

> **Note for production deployments:** This step can be avoided entirely by deploying via a package manager, configuration management tool (Ansible, Chef, etc.), or a container — none of which trigger the Gatekeeper quarantine flag.

## Commands

| Command                          | Description                                                  |
|----------------------------------|--------------------------------------------------------------|
| `eds server`                     | Start the streaming server                                   |
| `eds import`                     | Run a one-time bulk data import then start the server        |
| `eds enroll --api-key <token>`   | Save API credentials directly without an enrollment code     |
| `eds driver list`                | List all available drivers                                   |
| `eds driver help <scheme>`       | Show driver-specific connection string help                  |
| `eds version`                    | Print the current EDS version                                |
| `eds publickey`                  | Print the Shopmonkey PGP public key used to verify upgrades  |

### `eds server` options

| Flag              | Description                                                                   |
|-------------------|-------------------------------------------------------------------------------|
| `--config`        | Path to config file (default: `data/config.toml`)                            |
| `--data-dir`      | Directory for state, logs, and credentials                                    |
| `--verbose`       | Enable debug-level console output                                             |
| `--driver-mode`   | `upsert` (default) or `timeseries` — see [Time-Series Mode](#time-series-mode)|

### `eds import` options

| Flag              | Description                                                       |
|-------------------|-------------------------------------------------------------------|
| `--url`           | Destination driver URL (falls back to `url` in config.toml)      |
| `--api-key`       | Shopmonkey API key (falls back to `token` in config.toml)         |
| `--only`          | Comma-separated list of table names to import                     |
| `--company-ids`   | Filter export to specific company IDs                             |
| `--location-ids`  | Filter export to specific location IDs                            |
| `--job-id`        | Reuse an existing export job ID                                   |
| `--dir`           | Path to already-downloaded export files (skips API export)        |
| `--parallel`      | Max parallel table workers (default: 4)                           |
| `--dry-run`       | Parse and validate without writing any rows                       |
| `--no-confirm`    | Skip the interactive delete confirmation prompt                   |
| `--no-delete`     | Insert rows only; do not drop and recreate tables                 |
| `--schema-only`   | Create tables without importing any rows                          |
| `--no-cleanup`    | Keep the temporary download directory after import                |
| `--resume`        | Resume the last interrupted import — re-polls the export if still in progress, skips already-downloaded files, and continues from the first unfinished row file (implies `--no-delete --no-cleanup`) |
| `--driver-mode`   | `upsert` (default) or `timeseries` — see [Time-Series Mode](#time-series-mode) |

#### Import resumability

EDS saves a checkpoint to `data/state.db` as soon as an export job is created. This means `--resume` can recover from an interruption at **any stage** of the import pipeline:

| Interrupted during | `--resume` behaviour |
|--------------------|----------------------|
| Export polling (HQ still generating files) | Re-polls the same export job until it completes, then downloads |
| File download | Skips files already on disk, downloads the remainder |
| Row import | Skips fully-processed files, re-applies the first unfinished file from the start |

```sh
eds import --resume
```

Because rows are written via MERGE/upsert, re-applying a partially-processed file is safe — duplicate rows are simply overwritten. The checkpoint is automatically cleared after a successful full import.

You can also specify an export job ID explicitly to attach to a known job without needing a saved checkpoint:

```sh
eds import --job-id <export-job-id>
```

## Time-Series Mode

By default, SQL drivers mirror the source tables using **upsert** semantics — each row reflects the latest known state of the corresponding record in Shopmonkey.

Passing `--driver-mode timeseries` switches all SQL drivers to an **append-only event log** model. Every CDC event is inserted as a new row; no rows are ever updated or deleted. This enables full audit history and point-in-time queries at the cost of needing to join against the latest event per entity when you want current state.

### Events table schema

For each Shopmonkey table `{table}`, a fixed-schema table is created:

| Dialect         | Table location                     |
|-----------------|------------------------------------|
| PostgreSQL      | `eds_events.{table}_events`        |
| MySQL / MariaDB | `{table}__events` (same database)  |
| SQL Server      | `eds_events.{table}_events`        |
| Snowflake       | `eds_events.{table}_events`        |

All events tables share the same columns regardless of the source table structure:

| Column        | Type   | Description                                              |
|---------------|--------|----------------------------------------------------------|
| `_seq`        | BIGINT | Auto-increment primary key (insertion order)             |
| `_event_id`   | TEXT   | Unique event ID from Shopmonkey                          |
| `_operation`  | TEXT   | `CREATE`, `UPDATE`, or `DELETE`                          |
| `_entity_id`  | TEXT   | Primary key of the affected record                       |
| `_timestamp`  | BIGINT | Event timestamp in milliseconds since epoch              |
| `_mvcc_ts`    | TEXT   | MVCC timestamp from the source database                  |
| `_company_id` | TEXT   | Shopmonkey company ID                                    |
| `_location_id`| TEXT   | Shopmonkey location ID                                   |
| `_model_ver`  | TEXT   | Shopmonkey schema model version                          |
| `_diff`       | TEXT   | JSON array of changed field names (UPDATE only)          |
| `_before`     | JSON   | Full record state before the change (JSONB on PostgreSQL)|
| `_after`      | JSON   | Full record state after the change (JSONB on PostgreSQL) |

### Auto-maintained views

Two views are automatically created and refreshed whenever the schema changes:

**`current_{table}`** — latest state of each entity (equivalent to the upsert mirror):

```sql
-- Example: latest state of all work orders
SELECT * FROM eds_events.current_work_orders;
```

**`{table}_history`** — full audit trail with each schema column extracted from the JSON payloads:

```sql
-- Example: full change history for a specific work order
SELECT _seq, _operation, _timestamp, id, status, total
FROM eds_events.work_orders_history
WHERE id = 'wo-abc123'
ORDER BY _timestamp;
```

### Point-in-time queries

To reconstruct the state of all records at a specific moment, query the `current_{table}` view filtered by timestamp — or use a window function directly:

```sql
-- State of all work orders as of a specific Unix millisecond timestamp
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY _entity_id ORDER BY _timestamp DESC, _seq DESC) AS rn
  FROM eds_events.work_orders_events
  WHERE _timestamp <= 1743897600000   -- 2025-02-06T00:00:00Z
) t
WHERE rn = 1 AND _operation <> 'DELETE';
```

### Joining across tables in time-series mode

When joining time-series tables, use the auto-maintained views to get current state on both sides:

```sql
-- Current work orders joined to current customer
SELECT wo.id, wo.status, c.name AS customer_name
FROM eds_events.current_work_orders wo
JOIN eds_events.current_customers c ON c.id = wo.customer_id;
```

### Usage

```sh
# Start the server in time-series mode
eds server --driver-mode timeseries

# Run a bulk import then start the server in time-series mode
eds import --driver-mode timeseries --url postgres://user:password@localhost/mydb
```

The selected mode is automatically persisted to `config.toml` (`driver_mode = "timeseries"`). On subsequent restarts you can omit the flag and the stored value will be used. If you pass `--driver-mode` with a value that differs from what's in `config.toml`, EDS will prompt you to confirm before changing it. Pass `--no-confirm` to accept the change non-interactively (useful in scripts).

> **Note on bulk import:** Regardless of `--driver-mode`, the `eds import` command always writes the snapshot data into the **standard mirror tables** (e.g. `order`, `customer`). The events tables (`eds_events.order_events` etc.) are populated only by the live CDC stream once `eds server` is running. This means you can safely run `eds import --driver-mode timeseries` to set up the mode and load the initial snapshot — the server will then append new change events to the events tables on top of that baseline.

> **Note:** Upsert and time-series data can coexist in the same database. The events tables live in the `eds_events` schema (or use a `__events` suffix in MySQL), keeping them separate from the standard mirror tables.

## Driver Connection Strings

### PostgreSQL (`postgres`)

```
postgres://user:password@host:5432/dbname
postgres://user:password@host:5432/dbname?sslmode=require
```

### MySQL / MariaDB (`mysql`)

```
mysql://user:password@host:3306/dbname
mysql://user:password@host:3306/dbname?tls=true
```

### SQL Server (`sqlserver`)

```
sqlserver://user:password@host:1433?database=dbname
sqlserver://user:password@host:1433?database=dbname&trust-server-certificate=false
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `trust-server-certificate` | `true` | Set to `false` to enforce TLS certificate validation (recommended for production) |

### Snowflake (`snowflake`)

```
snowflake://user:password@account/dbname/schema?warehouse=WH&role=ROLE
```

### Amazon S3 (`s3`)

```
s3://bucket-name/optional-prefix?region=us-east-1
s3://bucket-name/optional-prefix?region=us-east-1&access-key=KEY&secret-key=SECRET
```

Credentials fall back to the AWS credential chain (environment variables, `~/.aws/credentials`, instance metadata) when `access-key` and `secret-key` are not provided.

### Azure Blob Storage (`azureblob`)

| Auth method       | URL format |
|-------------------|------------|
| Account key       | `azureblob://accountname/containername?key=<base64-key>` |
| Account key + prefix | `azureblob://accountname/containername/myprefix?key=<base64-key>` |
| Connection string | `azureblob://accountname/containername?connection-string=<uri-encoded-string>` |
| Azurite emulator  | `azureblob://devstoreaccount1/containername?connection-string=<uri-encoded-string>&endpoint=http%3A%2F%2F127.0.0.1%3A10000%2Fdevstoreaccount1` |

Each CDC event is stored as `{prefix}/{table}/{timestamp}-{id}.json`. During import, raw
`.ndjson.gz` export files are uploaded directly under `{prefix}/{table}/{filename}`.

## Building from Source

```sh
dotnet build

# Publish self-contained binaries (always pass -p:Version so the version
# string is stamped into the binary and reported to Shopmonkey HQ)
dotnet publish src/EDS.Cli -r linux-x64  -o publish/linux-x64  -p:Version=0.9.2-rc
dotnet publish src/EDS.Cli -r osx-arm64  -o publish/osx-arm64  -p:Version=0.9.2-rc
dotnet publish src/EDS.Cli -r win-x64    -o publish/win-x64    -p:Version=0.9.2-rc
```

Without `-p:Version`, the binary reports version `dev` to HQ. The CI/CD pipeline sets this
automatically from the git tag (see [Releases](#releases)).

## Running Tests

The test suite is split into two categories: **unit tests** (no external dependencies) and **integration tests** (require Docker).

### Unit tests

Run all unit tests across the three test projects:

```sh
dotnet test tests/EDS.Core.Tests
dotnet test tests/EDS.Infrastructure.Tests
dotnet test tests/EDS.Integration.Tests --filter "Category!=Integration"
```

Or run everything at once (integration tests are skipped if Docker is not running):

```sh
dotnet test --filter "Category!=Integration"
```

| Project | What is tested |
|---------|----------------|
| `EDS.Core.Tests` | SQL helpers, schema column ordering, `DbChangeEvent` logic, `ValidationResult`, `RetryHelper`, `DriverRegistry`, and the `QuoteJsonElement` / `QuoteString` escaping helpers in `SqlDriverBase` |
| `EDS.Infrastructure.Tests` | `StatusProvider` state management and URL sanitization |
| `EDS.Integration.Tests` (unit) | `BuildSql` output for PostgreSQL, MySQL, and SQL Server — INSERT, UPDATE (with and without diff), DELETE; special characters, SQL injection strings, Unicode, missing columns → NULL, and cross-driver balanced-quote invariant — all without a database connection |

### Integration tests (Docker required)

Integration tests spin up real database containers via [TestContainers](https://dotnet.testcontainers.org/) and push events through the full `ProcessAsync → FlushAsync` pipeline, verifying that values are stored and retrieved correctly.

**Prerequisites:** Docker Desktop (or any Docker-compatible daemon) must be running.

```sh
dotnet test tests/EDS.Integration.Tests --filter "Category=Integration"
```

| Container | What is tested |
|-----------|----------------|
| `postgres:16-alpine` | Insert, upsert dedup, update, update-with-diff (partial column update), delete, single-quote strings, SQL injection stored literally, Unicode + emoji, null values, numerics, booleans, newlines in values, multi-row batch commit |
| `mysql:8.0` | Same core scenarios, plus ISO 8601 → MySQL `TIMESTAMP` reformatting |

## Configuration

At runtime EDS creates a `data/` directory (or the path set by `--data-dir`) containing:

| File / Directory        | Description                                              |
|-------------------------|----------------------------------------------------------|
| `config.toml`           | Server settings — API token, driver URL, server ID       |
| `state.db`              | SQLite database for change-tracking and import state     |
| `<session-id>.log`      | Per-session log file (always captured at Debug level)    |
| `import-<timestamp>.log`| Log file for each import run                             |
| `<session-id>/`         | NATS credentials for the current session                 |

> **Keep `data/` out of source control.** It contains your API token and NATS credentials.

`config.toml` is automatically created with `600` permissions (owner read/write only) on macOS and Linux to protect the API token it contains.

### Example `config.toml`

```toml
token     = "your-shopmonkey-jwt"
server_id = "your-server-id"
url       = "postgres://user:password@localhost:5432/mydb"
```

Environment variables prefixed with `EDS_` override any value in `config.toml` (e.g. `EDS_TOKEN`, `EDS_URL`).

## Metrics & Status

EDS exposes an HTTP server on port **8080** (configurable via `metrics.port` in `config.toml` or the `[metrics]` section). By default the server binds to `localhost` only. Two endpoints are available:

| Endpoint   | Format     | Description                              |
|------------|------------|------------------------------------------|
| `/metrics` | Prometheus | Counters, histograms, and gauges for scraping by Prometheus/Grafana |
| `/status`  | JSON       | Human-readable runtime snapshot          |

### `/status` response

```json
{
  "version": "1.2.3",
  "session_id": "abc123def456",
  "driver": "postgres://localhost:5432/mydb",
  "uptime_seconds": 3847,
  "paused": false,
  "last_event_at": "2026-04-03T14:22:10+00:00",
  "last_event_table": "work_orders",
  "events_processed": 184920,
  "pending_flush": 3
}
```

### Configuring the metrics port

By default the metrics server binds to `localhost` only. Set `host = "+"` to expose on all interfaces when running inside Docker or Kubernetes where a Prometheus scraper reaches the container from outside loopback.

```toml
[metrics]
port = 9090   # default: 8080
host = "+"    # "+" = all interfaces (needed for Docker/k8s); default is "localhost"
```

## Session Renewal

EDS automatically restarts every 24 hours to obtain a fresh session and NATS credentials from Shopmonkey HQ. The restart is clean — all in-flight events are acknowledged before shutdown. If you are running EDS under a process supervisor (systemd, Docker, etc.), configure it to restart on any exit code.

## Exit Codes

| Code | Meaning | Recommended supervisor action |
|------|---------|-------------------------------|
| `0` | Clean shutdown | Do not restart |
| `1` | Fatal error | Restart with backoff |
| `4` | Intentional restart (session renewal, HQ-initiated restart, successful upgrade) | Restart immediately |
| `5` | NATS connectivity lost | Restart with backoff |

### systemd example

```ini
[Service]
ExecStart=/usr/local/bin/eds server
Restart=always
RestartSec=5
# Restart immediately (no delay) for intentional restart and NATS disconnect
RestartForceExitStatus=4 5
```

## Architecture

```
Shopmonkey HQ
     │  NATS JetStream (CDC events)
     ▼
NatsConsumerService  ──▶  IDriver  ──▶  Destination (SQL, S3, Kafka, …)
     │
     │  NATS notifications (configure, import, pause, upgrade, sendlogs, …)
     ▼
NotificationService
```

- **CDC events** arrive via NATS JetStream, are buffered in a channel, and flushed
  to the driver in batches with exponential-backoff retry on failure.
- **Notifications** from HQ allow the web interface to configure the driver, trigger
  a backfill import, pause/unpause streaming, request log uploads, and initiate
  in-place binary upgrades.
- **Schema registry** tracks table model versions and triggers DDL migrations when
  the Shopmonkey data model changes.
- **Pause handling** NAKs messages with a 30-second server-side delay so paused
  sessions do not create a tight redelivery loop.

## Project Structure

```
src/
  EDS.Cli/              Entry point — CLI commands, host setup
  EDS.Core/             Interfaces, models, shared helpers
  EDS.Infrastructure/   NATS consumer, schema registry, metrics, upgrade
  EDS.Importer/         Bulk import pipeline (NDJSON/gz)
  EDS.Drivers.PostgreSQL/
  EDS.Drivers.MySQL/
  EDS.Drivers.SqlServer/
  EDS.Drivers.Snowflake/
  EDS.Drivers.S3/
  EDS.Drivers.AzureBlob/
  EDS.Drivers.Kafka/
  EDS.Drivers.EventHub/
  EDS.Drivers.File/
tests/
  EDS.Core.Tests/
  EDS.Infrastructure.Tests/
  EDS.Integration.Tests/
Directory.Packages.props  Centralized NuGet package versions (all projects)
.github/workflows/        CI/CD — build on push, publish on version tag
```

## License

Copyright (c) 2022-2026 Shopmonkey, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
