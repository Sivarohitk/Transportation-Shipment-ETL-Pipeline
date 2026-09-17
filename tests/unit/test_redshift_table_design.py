"""Unbenchmarked Redshift tables leave physical tuning to Redshift AUTO."""

from __future__ import annotations

from pathlib import Path


def test_redshift_ddl_uses_automatic_physical_design():
    root = Path(__file__).resolve().parents[2] / "sql" / "redshift"
    for filename in ("002_create_staging_tables.sql", "003_create_final_tables.sql"):
        sql = (root / filename).read_text(encoding="utf-8").upper()
        assert sql.count("CREATE TABLE IF NOT EXISTS") == sql.count("DISTSTYLE AUTO")
        assert sql.count("CREATE TABLE IF NOT EXISTS") == sql.count("SORTKEY AUTO")
        assert sql.count("CREATE TABLE IF NOT EXISTS") == sql.count("ENCODE AUTO")
