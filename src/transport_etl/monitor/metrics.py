"""Operational metrics emission helpers."""

from __future__ import annotations

from typing import Any


def emit_batch_metrics(metrics: dict[str, Any]) -> None:
    """Accept operational metrics for a batch run.

    The project keeps metrics emission as a no-op helper so local execution and
    tests stay dependency-free while preserving a clear integration point.
    """
    _ = metrics
