"""CLI entry point for the ETL framework.

Usage:
    python cli.py --service                        # long-running queue executor
    python cli.py --show-config                    # dump config and exit
    python cli.py <effective_date>                 # run all active jobs for date
    python cli.py <effective_date> <job_name>      # run one job for date

effective_date format: YYYY-MM-DD
"""

from __future__ import annotations

import argparse
import datetime
import logging
import sys
import time
from pathlib import Path

from etl import connection_helper, path_helper
from etl.app_config import load_config

logger = logging.getLogger("etl")


def _find_appsettings() -> Path | None:
    """Locate appsettings.json next to this script."""
    candidate = Path(__file__).resolve().parent / "appsettings.json"
    return candidate if candidate.exists() else None


def _parse_date(value: str) -> datetime.date:
    """Parse a YYYY-MM-DD date string or raise argparse error."""
    try:
        return datetime.date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Expected format: YYYY-MM-DD"
        )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cli.py",
        description="MockEtlFramework — ETL job executor.",
    )
    parser.add_argument(
        "--service",
        action="store_true",
        help="Run as a long-running queue executor (polls control.task_queue).",
    )
    parser.add_argument(
        "--show-config",
        action="store_true",
        help="Dump resolved configuration and exit.",
    )
    parser.add_argument(
        "effective_date",
        nargs="?",
        type=_parse_date,
        help="Effective date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "job_name",
        nargs="?",
        help="Optional job name — run only this job for the given date.",
    )
    return parser


def _show_config(config) -> None:
    """Print resolved config values to stdout."""
    print("[Config] Resolved values:")
    print(f'  Paths.EtlRoot       = "{config.paths.etl_root}"')
    print(f'  Paths.EtlReOutput   = "{config.paths.etl_re_output}"')
    print(f'  Paths.EtlReRoot     = "{config.paths.etl_re_root}"')
    print(f'  Paths.EtlLogPath    = "{config.paths.etl_log_path}"')
    print(f'  Database.Host       = "{config.database.host}"')
    print(f'  Database.Username   = "{config.database.username}"')
    print(f'  Database.DatabaseName = "{config.database.database_name}"')
    print(
        f"  Database.Password   = "
        f"{'(set)' if config.database.password else '(empty)'}"
    )
    print(f"  Database.Timeout    = {config.database.timeout}")
    print(f"  Database.CmdTimeout = {config.database.command_timeout}")
    print(f"  TaskQueue.Threads   = {config.task_queue.thread_count}")
    print(f"  TaskQueue.PollMs    = {config.task_queue.poll_interval_ms}")
    print(f"  TaskQueue.IdleShut  = {config.task_queue.idle_shutdown_seconds}s")


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    parser = _build_parser()
    args = parser.parse_args(argv)

    # Load config
    appsettings = _find_appsettings()
    config = load_config(appsettings)
    if appsettings:
        logger.info("Loaded config from %s", appsettings)
    else:
        logger.info("No appsettings.json found — using defaults.")

    # --show-config: dump and exit
    if args.show_config:
        _show_config(config)
        return

    # All other modes require a database password
    if not config.database.password:
        print("Error: No database password configured.", file=sys.stderr)
        print("Set the ETL_DB_PASSWORD environment variable.", file=sys.stderr)
        sys.exit(1)

    # Initialise global helpers
    connection_helper.initialize(config)
    path_helper.initialize(config)

    # --service mode
    if args.service:
        from etl.control.task_queue_service import TaskQueueService

        logger.info("Starting queue executor (service mode).")
        queue_service = TaskQueueService(config)
        t0 = time.monotonic()
        queue_service.run()
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        logger.info("Queue execution completed in %dms.", elapsed_ms)
        return

    # Date-based execution requires an effective_date
    if args.effective_date is None:
        parser.error(
            "An effective_date is required unless --service or --show-config "
            "is specified."
        )

    # Single-date executor
    from etl.control import job_executor_service

    t0 = time.monotonic()
    job_executor_service.run(args.effective_date, args.job_name)
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    logger.info("Job execution completed in %dms.", elapsed_ms)


if __name__ == "__main__":
    main()
