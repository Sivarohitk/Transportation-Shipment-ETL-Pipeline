"""Date utilities for batch run windows and validation."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

DATE_FMT = "%Y-%m-%d"


def parse_date(value: str, field_name: str = "date") -> date:
    """Parse a string date in `YYYY-MM-DD` format."""
    try:
        return datetime.strptime(value, DATE_FMT).date()
    except ValueError as exc:
        raise ValueError(f"Invalid {field_name}: {value}. Expected format YYYY-MM-DD") from exc


def resolve_run_date(run_date: str | None) -> str:
    """Resolve the run date as `YYYY-MM-DD`.

    If no run date is provided, the current UTC date is used.
    """
    if run_date:
        return parse_date(run_date, field_name="run_date").strftime(DATE_FMT)
    return datetime.now(timezone.utc).strftime(DATE_FMT)


def resolve_backfill_window(start_date: str | None, end_date: str | None) -> tuple[date, date]:
    """Validate and resolve a backfill date window."""
    if not start_date or not end_date:
        raise ValueError("Both start_date and end_date are required for backfill runs")

    start = parse_date(start_date, field_name="start_date")
    end = parse_date(end_date, field_name="end_date")

    if start > end:
        raise ValueError("start_date must be less than or equal to end_date")

    return start, end


def enumerate_dates(start: date, end: date) -> list[str]:
    """Enumerate inclusive run dates between start and end."""
    total_days = (end - start).days
    return [(start + timedelta(days=offset)).strftime(DATE_FMT) for offset in range(total_days + 1)]


def iso_utc_now() -> str:
    """Return current UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
