"""SQL-focused tests for KPI model output semantics."""

from __future__ import annotations

from pathlib import Path

from transport_etl.transform.build_kpi_delivery_daily import build_kpi_delivery_daily


def _first_sql_statement(sql_text: str) -> str:
    """Return first SQL statement from a multi-statement SQL script."""
    # Handle UTF-8 BOM-prefixed SQL files produced by some editors.
    sql_text = sql_text.lstrip("\ufeff")
    statements = [statement.strip() for statement in sql_text.split(";") if statement.strip()]
    if not statements:
        raise ValueError("No SQL statement found")
    return statements[0]


def test_kpi_sql_matches_python_builder(
    project_root: Path,
    curated_frames: dict[str, object],
    spark,
) -> None:
    """Spark SQL KPI model should match Python KPI builder output."""
    from pyspark.sql import functions as F

    agg_df = curated_frames["agg_shipment_daily"]
    fct_event_df = curated_frames["fct_delivery_event"]

    agg_df.createOrReplaceTempView("agg_shipment_daily")
    fct_event_df.createOrReplaceTempView("fct_delivery_event")

    sql_file = project_root / "sql" / "kpi" / "kpi_delivery_daily.sql"
    sql_query = _first_sql_statement(sql_file.read_text(encoding="utf-8"))
    sql_kpi_df = spark.sql(sql_query)

    python_kpi_df = build_kpi_delivery_daily(
        agg_shipment_daily_df=agg_df,
        fct_delivery_event_df=fct_event_df,
    )

    keys = ["p_date", "region_code", "carrier_id"]
    metrics = [
        "on_time_delivery_rate",
        "avg_transit_hours",
        "late_delivery_rate",
        "first_attempt_success_rate",
        "exception_rate",
        "avg_cost_per_mile",
        "volume_by_carrier",
        "delivery_event_density",
    ]

    assert sql_kpi_df.count() == python_kpi_df.count()

    joined = sql_kpi_df.alias("s").join(
        python_kpi_df.alias("p"),
        on=keys,
        how="inner",
    )
    assert joined.count() == python_kpi_df.count()

    for metric in metrics:
        max_abs_diff = (
            joined.select(F.max(F.abs(F.col(f"s.{metric}") - F.col(f"p.{metric}"))).alias("d"))
            .collect()[0]["d"]
        )
        assert (max_abs_diff or 0.0) < 1e-9
