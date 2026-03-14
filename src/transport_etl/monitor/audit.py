"""Audit logging helpers for ETL runs."""

from __future__ import annotations

from typing import Any


def build_audit_record(run_id: str, status: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    """Create audit payload for a pipeline run.

    TODO: Persist audit records to storage table/path.
    """
    return {
        "run_id": run_id,
        "status": status,
        "details": details or {},
    }
