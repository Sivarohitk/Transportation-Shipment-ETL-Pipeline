"""Backfill batch job orchestration."""

from __future__ import annotations

import copy
from typing import Any, Mapping

from transport_etl.common.config import load_config
from transport_etl.common.dates import enumerate_dates, resolve_backfill_window
from transport_etl.common.logging import configure_logging, get_logger
from transport_etl.jobs.run_daily_batch import run_daily_batch


def _set_nested_value(payload: dict[str, Any], dotted_key: str, value: Any) -> None:
    """Set a nested dictionary value using a dotted-path key."""
    keys = [segment for segment in dotted_key.split(".") if segment]
    if not keys:
        return

    current = payload
    for key in keys[:-1]:
        if key not in current or not isinstance(current[key], dict):
            current[key] = {}
        current = current[key]
    current[keys[-1]] = value


def _apply_overrides(config: dict[str, Any], overrides: Mapping[str, Any] | None) -> dict[str, Any]:
    """Apply runtime override values onto a loaded config dictionary."""
    merged = copy.deepcopy(config)
    if not overrides:
        return merged

    for key, value in overrides.items():
        if value is None:
            continue
        _set_nested_value(merged, str(key), value)

    return merged


def run_backfill_batch(
    config_path: str,
    start_date: str | None,
    end_date: str | None,
    overrides: Mapping[str, Any] | None = None,
) -> int:
    """Run backfill ETL workflow over an inclusive date window."""
    loaded_config = load_config(config_path)
    config = _apply_overrides(loaded_config, overrides)

    logging_config = config.get("logging", {}) if isinstance(config.get("logging"), Mapping) else {}
    configure_logging(
        level=str(logging_config.get("level", "INFO")),
        json_logs=bool(logging_config.get("json", False)),
    )

    logger = get_logger(
        "transport_etl.jobs.backfill",
        job="backfill",
        env=str(config.get("app", {}).get("env", "unknown")),
    )

    try:
        start, end = resolve_backfill_window(start_date=start_date, end_date=end_date)
    except ValueError as exc:
        logger.error("Invalid backfill date window: %s", exc)
        return 1

    run_dates = enumerate_dates(start=start, end=end)
    runtime_config = config.get("runtime", {}) if isinstance(config.get("runtime"), Mapping) else {}
    fail_fast = bool(runtime_config.get("fail_fast", True))

    logger.info(
        "Backfill batch started start_date=%s end_date=%s total_dates=%s",
        start_date,
        end_date,
        len(run_dates),
    )

    failures: list[str] = []
    for run_date in run_dates:
        logger.info("Backfill executing date=%s", run_date)
        status = run_daily_batch(
            config_path=config_path,
            run_date=run_date,
            overrides=overrides,
        )

        if status != 0:
            failures.append(run_date)
            logger.error("Backfill date failed run_date=%s status=%s", run_date, status)
            if fail_fast:
                break

    if failures:
        logger.error("Backfill completed with failures failed_dates=%s", failures)
        return 1

    logger.info("Backfill completed successfully")
    return 0
