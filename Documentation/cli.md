# CLI

Entry point for running jobs. Defined in `cli.py` (project root).

## Usage

```
python cli.py --service                        # long-running queue executor (polls control.task_queue)
python cli.py --show-config                    # dump resolved config and exit
python cli.py <effective_date>                 # run all active jobs for that date
python cli.py <effective_date> <job_name>      # run one job for that date
```

- `effective_date` format: `YYYY-MM-DD`. **Required** unless `--service` or `--show-config` is given.
- `--service` mode imports and delegates to `TaskQueueService`.
- `--show-config` prints all resolved configuration values (paths, database, task queue) and exits. Does **not** require a database password.
- All other modes delegate to `job_executor_service.run()`.
- `run_date` is always today internally and is never a CLI argument.

## Startup Sequence

1. Configure logging (`INFO` level, timestamped format).
2. Parse arguments via `argparse`.
3. Locate `appsettings.json` adjacent to `cli.py`. If absent, `AppConfig` defaults are used.
4. Call `load_config(appsettings_path)` to build the frozen `AppConfig`.
5. If `--show-config`, print config and exit (no password check).
6. Fail fast if `ETL_DB_PASSWORD` env var is not set (all remaining modes need it).
7. Call `connection_helper.initialize(config)` and `path_helper.initialize(config)`.
8. Dispatch to the appropriate service based on arguments.

## Build & Run

```bash
export ETL_DB_PASSWORD='your_password'                        # required
pip install -e .                                              # install in editable mode
python cli.py --show-config                                   # verify config
python cli.py 2024-10-15                                      # all jobs for date
python cli.py 2024-10-15 JobName                              # one job for date
python cli.py --service                                       # queue executor
```

## Tests

```bash
pytest                                                        # run all tests
```
