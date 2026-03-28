"""Audit logging helpers for ETL runs."""

from __future__ import annotations

from typing import Any


def build_audit_record(
    run_id: str, status: str, details: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Create an audit payload for a pipeline run.

    Persistence is intentionally left to the caller so the helper stays usable
    for local development, tests, and future publish targets.
    """
    return {
        "run_id": run_id,
        "status": status,
        "details": details or {},
    }
