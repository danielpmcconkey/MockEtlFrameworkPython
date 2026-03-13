"""Pipeline executor — loads a job conf and runs its modules in sequence."""

from __future__ import annotations

import datetime
import logging
import time
from pathlib import Path

from etl import module_factory, path_helper
from etl.app_config import get_config
from etl.job_conf import JobConf

logger = logging.getLogger(__name__)


def _attach_file_handler(
    job_name: str,
    effective_date: datetime.date | None,
) -> logging.FileHandler | None:
    """If ETL_LOG_PATH is configured, attach a per-job file handler to the
    root 'etl' logger and return it.  Returns None if logging is not configured.
    """
    config = get_config()
    if not config or not config.paths.etl_log_path:
        return None

    log_dir = Path(config.paths.etl_log_path)
    log_dir.mkdir(parents=True, exist_ok=True)

    date_str = effective_date.isoformat() if effective_date else "no-date"
    safe_name = job_name.replace(" ", "_").replace("/", "_")
    log_file = log_dir / f"{safe_name}_{date_str}.log"

    handler = logging.FileHandler(log_file, encoding="utf-8")
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
    )
    logging.getLogger("etl").addHandler(handler)
    return handler


def run(
    job_conf_path: str | Path,
    initial_state: dict[str, object] | None = None,
) -> dict[str, object]:
    """Load a job conf, build the module pipeline, and execute it.

    Each module receives the shared state dict from the previous module.
    Returns the final shared state after all modules have executed.
    """
    resolved = path_helper.resolve(str(job_conf_path))
    job_conf = JobConf.from_file(resolved)

    # Determine effective date for logging (may be None for dry runs).
    eff_date = None
    if initial_state:
        val = initial_state.get("__etlEffectiveDate")
        if isinstance(val, datetime.date):
            eff_date = val

    file_handler = _attach_file_handler(job_conf.job_name, eff_date)

    logger.info("Starting job: %s", job_conf.job_name)
    t0 = time.monotonic()

    shared_state: dict[str, object] = (
        dict(initial_state) if initial_state is not None else {}
    )

    try:
        for module_config in job_conf.modules:
            module_type = module_config.get("type", "?")
            module = module_factory.create(module_config)
            shared_state = module.execute(shared_state)
            logger.info("  %s", module_type)

        elapsed_ms = int((time.monotonic() - t0) * 1000)
        logger.info("Job complete: %s (%dms)", job_conf.job_name, elapsed_ms)
    finally:
        if file_handler:
            logging.getLogger("etl").removeHandler(file_handler)
            file_handler.close()

    return shared_state
