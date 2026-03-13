# Environment Variables Reference

Used by MockEtlFrameworkPython and Proofmark.

## Variables

### `ETL_ROOT`

**Purpose:** Solution root for the ETL framework. Used for:
- `{ETL_ROOT}` token expansion in `control.jobs.job_conf_path`
- Base path for resolving relative paths (output directories, etc.) via `path_helper.resolve()`

| Environment | Value |
|---|---|
| Host (Hobson) | `/media/dan/fdrive/codeprojects/MockEtlFrameworkPython` |
| Basement (BD) | `/workspace/MockEtlFrameworkPython` |

### `ETL_DB_PASSWORD`

**Purpose:** PostgreSQL password. Read once at startup, never stored in config files. Required for any mode that touches the database.

| Environment | Value |
|---|---|
| Host (Hobson) | `claude` (the `claude` DB role) |
| Basement (BD) | `claude` |

## Proofmark Comparison Pattern

When Proofmark compares original vs rewrite output:
- **LHS (original):** paths use `{ETL_ROOT}` — resolves to the OG framework's output
- **RHS (rewrite/challenger):** paths also use `{ETL_ROOT}` — host flips `ETL_ROOT` to point at the RE workspace when validating RE output

## Non-Environment Configuration

These are set in `appsettings.json` (ETL FW) or YAML settings (Proofmark), not env vars:

| Setting | Default | Notes |
|---|---|---|
| `Database.Host` | `localhost` | |
| `Database.Username` | `claude` | |
| `Database.DatabaseName` | `atc` | |
| `Database.Timeout` | `15` | seconds |
| `Database.CommandTimeout` | `300` | seconds |
| `TaskQueue.ThreadCount` | `5` | |
| `TaskQueue.PollIntervalMs` | `5000` | |
| `TaskQueue.IdleShutdownSeconds` | `28800` | 8 hours |
