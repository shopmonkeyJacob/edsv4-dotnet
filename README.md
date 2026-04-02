# EDS — Enterprise Data Streaming (.NET)

A .NET port of Shopmonkey's Enterprise Data Streaming server. Connects to Shopmonkey HQ via NATS JetStream and streams change data capture (CDC) events to your own data infrastructure through a configurable driver.

## Supported Drivers

| Scheme       | Destination          | Import |
|--------------|----------------------|:------:|
| `postgres`   | PostgreSQL           | ✓      |
| `mysql`      | MySQL / MariaDB      | ✓      |
| `sqlserver`  | SQL Server           | ✓      |
| `snowflake`  | Snowflake            | ✓      |
| `s3`         | Amazon S3            |        |
| `kafka`      | Apache Kafka         |        |
| `eventhub`   | Azure Event Hubs     |        |
| `file`       | Local NDJSON files   |        |

## Requirements

- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0) (build from source)
- A Shopmonkey account with EDS access

## Quick Start

1. Download the binary for your platform from [Releases](../../releases).
2. Run `eds server` — on first launch you will be prompted for a one-time enrollment
   code from the [Shopmonkey HQ web interface](https://app.shopmonkey.io).
3. Configure your destination driver in the web interface. EDS will begin streaming
   events as soon as a driver URL is saved.

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

| Flag            | Description                                          |
|-----------------|------------------------------------------------------|
| `--config`      | Path to config file (default: `data/config.toml`)   |
| `--data-dir`    | Directory for state, logs, and credentials           |
| `--verbose`     | Enable debug-level console output                    |

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

## Building from Source

```sh
dotnet build

# Publish self-contained binaries for all platforms
dotnet publish src/EDS.Cli -r linux-x64  -o publish/linux-x64
dotnet publish src/EDS.Cli -r osx-arm64  -o publish/osx-arm64
dotnet publish src/EDS.Cli -r win-x64    -o publish/win-x64
```

## Running Tests

```sh
dotnet test
```

## Configuration

At runtime EDS creates a `data/` directory (or the path set by `--data-dir`) containing:

| File / Directory        | Description                                              |
|-------------------------|----------------------------------------------------------|
| `config.toml`           | Server settings — API token, driver URL, server ID       |
| `state.db`              | SQLite database for change-tracking state                |
| `<session-id>.log`      | Per-session log file (always captured at Debug level)    |
| `import-<timestamp>.log`| Log file for each import run                             |
| `<session-id>/`         | NATS credentials for the current session                 |

> **Keep `data/` out of source control.** It contains your API token and NATS credentials.

### Example `config.toml`

```toml
token     = "your-shopmonkey-jwt"
server_id = "your-server-id"
url       = "postgres://user:password@localhost:5432/mydb"
```

Environment variables prefixed with `EDS_` override any value in `config.toml`
(e.g. `EDS_TOKEN`, `EDS_URL`).

## Architecture

```
Shopmonkey HQ
     │  NATS JetStream (CDC events)
     ▼
NatsConsumerService  ──▶  IDriver  ──▶  Destination (SQL, S3, Kafka, …)
     │
     │  NATS notifications (configure, import, pause, upgrade, …)
     ▼
NotificationService
```

- **CDC events** arrive via NATS JetStream, are buffered in a channel, and flushed
  to the driver in batches with exponential-backoff retry on failure.
- **Notifications** from HQ allow the web interface to configure the driver, trigger
  a backfill import, pause/unpause streaming, and initiate in-place binary upgrades.
- **Schema registry** tracks table model versions and triggers DDL migrations when
  the Shopmonkey data model changes.

## Project Structure

```
src/
  EDS.Cli/              Entry point — CLI commands, host setup
  EDS.Core/             Interfaces, models, shared helpers
  EDS.Infrastructure/   NATS consumer, schema registry, metrics, upgrade
  EDS.Importer/         Bulk import pipeline (NDJSON/gz)
  EDS.Drivers.*/        One project per driver
tests/
  EDS.Core.Tests/
  EDS.Infrastructure.Tests/
  EDS.Integration.Tests/
```

## License

Copyright (c) 2022-2026 Shopmonkey, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
