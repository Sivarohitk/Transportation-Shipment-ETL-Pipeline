"""Logging utilities including structured logger adapters."""

from __future__ import annotations

import json
import logging
import logging.config
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from transport_etl.common.constants import DEFAULT_LOG_LEVEL, DEFAULT_LOGGER_NAME


class JsonFormatter(logging.Formatter):
    """JSON formatter for structured application logs."""

    def format(self, record: logging.LogRecord) -> str:
        """Render a log record as a JSON object string."""
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        for key in ("run_id", "job", "env", "batch_date"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=True)


class ContextLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that carries structured context fields."""

    def process(self, msg: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        """Inject context fields into log record extras."""
        extra = kwargs.get("extra", {})
        kwargs["extra"] = {**self.extra, **extra}
        return msg, kwargs


def _configure_fallback_logging(level: str = DEFAULT_LOG_LEVEL, json_logs: bool = False) -> None:
    """Configure an in-process fallback logger when config file is absent."""
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(level.upper())

    handler = logging.StreamHandler()
    if json_logs:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
        )

    root_logger.addHandler(handler)


def configure_logging(
    config_path: str = "config/logging.yaml",
    level: str | None = None,
    json_logs: bool | None = None,
) -> None:
    """Configure application logging from YAML, with fallback defaults.

    Args:
        config_path: Path to YAML logging config using `logging.config.dictConfig` schema.
        level: Optional override for log level.
        json_logs: Optional fallback to JSON formatter when no config file exists.
    """
    path = Path(config_path)

    if path.exists():
        with path.open("r", encoding="utf-8") as handle:
            config = yaml.safe_load(handle) or {}

        if level and "root" in config:
            config["root"]["level"] = level.upper()

        logging.config.dictConfig(config)
        return

    _configure_fallback_logging(
        level=level or DEFAULT_LOG_LEVEL,
        json_logs=bool(json_logs),
    )


def get_logger(name: str = DEFAULT_LOGGER_NAME, **context: Any) -> ContextLoggerAdapter:
    """Return a context-aware logger adapter for structured logs."""
    logger = logging.getLogger(name)
    return ContextLoggerAdapter(logger, context)
