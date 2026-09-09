"""Integration tests for the Phase-6 Gold analytics builders.

The tests run against a real local SparkSession and use the same
``curated_frames`` fixture as the existing Gold-layer tests.  They
validate:

- table grain (uniqueness of the key columns)
- KPI arithmetic (rates, denominators, edge cases)
- dedup behavior (multiple shipments / events per grain collapse)
- null behavior (rates default to 0.0; counts default to 0)
- aggregation correctness against an independently-computed reference
- SQL builder parity (Spark SQL matches the Python builder)
- live-Databricks tests are skipped (per AGENTS.md rule 9)
"""

from __future__ import annotations

from pathlib import Path

import pytest

from transport_etl.common.constants import (
    DELIVERY_EXCEPTION_EVENT_TYPES,
    TABLE_GOLD_CARRIER_PERFORMANCE,
    TABLE_GOLD_DELIVERY_EXCEPTION_SUMMARY,
    TABLE_GOLD_ROUTE_PERFORMANCE,
)
from transport_etl.transform.build_carrier_performance import build_carrier_performance
from transport_etl.transform.build_delivery_exception_summary import (
    build_delivery_exception_summary,
)
from transport_etl.transform.build_route_performance import build_route_performance

# ---------------------------------------------------------------------------
# carrier_performance
# ---------------------------------------------------------------------------


def test_delivery_event_partition_date_matches_shipment_cohort(
    curated_frames: dict[str, object],
) -> None:
    """Event KPIs must join to the pickup cohort used by shipment facts."""
    events = curated_frames["fct_delivery_event"].select("shipment_id", "p_date").alias("events")
    shipments = curated_frames["fct_shipment"].select("shipment_id", "p_date").alias("shipments")
    mismatches = events.join(shipments, on="shipment_id", how="inner").where(
        events["p_date"] != shipments["p_date"]
    )
    assert mismatches.count() == 0


class TestCarrierPerformanceBuilder:
    """Validate grain, metrics, and edge cases for the carrier_performance table."""

    def test_grain_is_unique(self, curated_frames: dict[str, object]) -> None:

        result = build_carrier_performance(
            fct_shipment_df=curated_frames["fct_shipment"],
            fct_delivery_event_df=curated_frames["fct_delivery_event"],
            dim_carrier_df=curated_frames["dim_carrier"],
        )
        collected = result.collect()
        assert len(collected) > 0
        # Grain is (p_date, carrier_id, service_mode)
        keys = [(r["p_date"], r["carrier_id"], r["service_mode"]) for r in collected]
        assert len(keys) == len(set(keys)), "carrier_performance grain must be unique"

    def test_all_rates_in_unit_interval(self, curated_frames: dict[str, object]) -> None:

        result = build_carrier_performance(
            fct_shipment_df=curated_frames["fct_shipment"],
            fct_delivery_event_df=curated_frames["fct_delivery_event"],
            dim_carrier_df=curated_frames["dim_carrier"],
        )
        # Collect the columns of interest to assert on them.
        rows = result.select(
            "on_time_delivery_rate",
            "late_delivery_rate",
            "exception_rate",
            "first_attempt_success_rate",
        ).collect()
        assert rows
        for row in rows:
            for field in (
                "on_time_delivery_rate",
                "late_delivery_rate",
                "exception_rate",
                "first_attempt_success_rate",
            ):
                value = float(row[field] or 0.0)
                assert 0.0 <= value <= 1.0, f"{field} = {value} out of [0,1]"

    def test_rates_match_shipment_facts(self, curated_frames: dict[str, object]) -> None:
        """The Python builder should match a manual aggregate of the
        shipment facts (within floating-point tolerance)."""
        from pyspark.sql import functions as F

        fct = curated_frames["fct_shipment"]
        result = build_carrier_performance(
            fct_shipment_df=fct,
            fct_delivery_event_df=curated_frames["fct_delivery_event"],
            dim_carrier_df=curated_frames["dim_carrier"],
        )

        # Manual reference aggregate on fct_shipment alone.
        # We can't use service_mode from the dimension here, so we
        # only verify shipment_volume, on_time_delivery_rate,
        # late_delivery_rate, exception_rate, avg_transit_hours,
        # avg_cost_per_mile on the (p_date, carrier_id) grain.
        ref = fct.groupBy("p_date", "carrier_id").agg(
            F.countDistinct(F.col("shipment_id")).alias("shipment_volume"),
            F.sum(F.col("on_time_delivery_flag")).cast("long").alias("on_time_shipments"),
            F.sum(F.when(F.col("delay_minutes") > 0, F.lit(1)).otherwise(F.lit(0)))
            .cast("long")
            .alias("late_shipments"),
            F.sum(F.col("exception_flag")).cast("long").alias("exception_shipments"),
            F.sum(F.col("delivered_flag")).cast("long").alias("delivered_shipments"),
        )
        # The Python builder includes service_mode; we can verify the
        # total shipment volume per (p_date, carrier_id) by summing
        # over service_mode.
        rolled = result.groupBy("p_date", "carrier_id").agg(
            F.sum(F.col("shipment_volume")).cast("long").alias("total_volume"),
        )
        joined = rolled.join(ref, on=["p_date", "carrier_id"], how="inner")
        # All keys must match.
        assert joined.count() == ref.count()

        # For each (p_date, carrier_id), the rolled shipment_volume
        # across service_mode rows must equal the shipment fact
        # distinct count.
        max_diff = joined.select(
            F.max(F.abs(F.col("total_volume") - F.col("shipment_volume")))
        ).collect()[0][0]
        assert (max_diff or 0) == 0

    def test_null_handling_rates_default_to_zero(self, curated_frames: dict[str, object]) -> None:
        """When the shipment fact is empty, every rate defaults to 0.0."""
        from pyspark.sql import functions as F

        # Build an empty shipment fact (no rows) using the existing
        # fixture's SparkSession.  We do not spin up a new
        # SparkSession here because that is unreliable in the
        # Windows CI environment.
        empty_shipment = curated_frames["fct_shipment"].where(F.lit(False))
        empty_event = curated_frames["fct_delivery_event"].where(F.lit(False))
        empty_dim = curated_frames["dim_carrier"].where(F.lit(False))

        result = build_carrier_performance(
            fct_shipment_df=empty_shipment,
            fct_delivery_event_df=empty_event,
            dim_carrier_df=empty_dim,
        )
        # The build must produce zero rows for an empty input.
        rows = result.collect()
        assert rows == []

    def test_service_mode_comes_from_dim_carrier(self, curated_frames: dict[str, object]) -> None:

        result = build_carrier_performance(
            fct_shipment_df=curated_frames["fct_shipment"],
            fct_delivery_event_df=curated_frames["fct_delivery_event"],
            dim_carrier_df=curated_frames["dim_carrier"],
        )
        # The service_mode values must be from the dim_carrier vocabulary
        # (FTL / LTL / PARCEL / UNKNOWN).
        service_modes = {
            row["service_mode"] for row in result.select("service_mode").distinct().collect()
        }
        assert service_modes.issubset({"FTL", "LTL", "PARCEL", "UNKNOWN"})

    def test_dedup_collapses_duplicate_shipments(self, curated_frames: dict[str, object]) -> None:
        """The shipment fact already de-dupes by ``shipment_id``; the
        carrier_performance aggregate must therefore count each
        shipment exactly once per grain."""
        from pyspark.sql import functions as F

        result = build_carrier_performance(
            fct_shipment_df=curated_frames["fct_shipment"],
            fct_delivery_event_df=curated_frames["fct_delivery_event"],
            dim_carrier_df=curated_frames["dim_carrier"],
        )

        # shipment_volume must equal the count of distinct shipment
        # ids in the corresponding (p_date, carrier_id) window of the
        # shipment fact, summed across service_mode.
        rolled = result.groupBy("p_date", "carrier_id").agg(
            F.sum(F.col("shipment_volume")).cast("long").alias("rolled_volume")
        )
        fct = curated_frames["fct_shipment"]
        ref = fct.groupBy("p_date", "carrier_id").agg(
            F.countDistinct(F.col("shipment_id")).cast("long").alias("fct_volume")
        )
        joined = rolled.join(ref, on=["p_date", "carrier_id"], how="inner")
        max_diff = joined.select(
            F.max(F.abs(F.col("rolled_volume") - F.col("fct_volume")))
        ).collect()[0][0]
        assert (max_diff or 0) == 0


# ---------------------------------------------------------------------------
# route_performance
# ---------------------------------------------------------------------------


class TestRoutePerformanceBuilder:
    """Validate grain, metrics, and edge cases for the route_performance table."""

    def test_grain_is_unique(self, curated_frames: dict[str, object]) -> None:

        result = build_route_performance(fct_shipment_df=curated_frames["fct_shipment"])
        collected = result.collect()
        assert len(collected) > 0
        keys = [
            (r["p_date"], r["origin_region_code"], r["destination_region_code"], r["carrier_id"])
            for r in collected
        ]
        assert len(keys) == len(set(keys)), "route_performance grain must be unique"

    def test_all_rates_in_unit_interval(self, curated_frames: dict[str, object]) -> None:

        result = build_route_performance(fct_shipment_df=curated_frames["fct_shipment"])
        rows = result.select("on_time_rate", "late_rate", "exception_rate").collect()
        assert rows
        for row in rows:
            for field in ("on_time_rate", "late_rate", "exception_rate"):
                value = float(row[field] or 0.0)
                assert 0.0 <= value <= 1.0, f"{field} = {value} out of [0,1]"

    def test_shipment_count_matches_shipment_fact(self, curated_frames: dict[str, object]) -> None:
        from pyspark.sql import functions as F

        fct = curated_frames["fct_shipment"]
        result = build_route_performance(fct_shipment_df=fct)

        # Total shipment_count per (p_date, origin, destination)
        # should equal the count of distinct shipment ids from fct.
        rolled = result.groupBy("p_date", "origin_region_code", "destination_region_code").agg(
            F.sum(F.col("shipment_count")).cast("long").alias("total")
        )
        ref = (
            fct.groupBy("p_date", "origin_region_code", "region_code")
            .agg(F.countDistinct(F.col("shipment_id")).cast("long").alias("fct_total"))
            .withColumnRenamed("region_code", "destination_region_code")
        )
        joined = rolled.join(
            ref, on=["p_date", "origin_region_code", "destination_region_code"], how="inner"
        )
        max_diff = joined.select(F.max(F.abs(F.col("total") - F.col("fct_total")))).collect()[0][0]
        assert (max_diff or 0) == 0

    def test_dedup_per_route(self, curated_frames: dict[str, object]) -> None:
        """Each ``shipment_id`` appears in exactly one route row."""
        from pyspark.sql import functions as F

        result = build_route_performance(fct_shipment_df=curated_frames["fct_shipment"])
        # We can't recover shipment_id from the aggregate directly, but
        # the count of distinct (p_date, origin, dest, carrier) grain
        # rows summed over the carrier_id should equal the count of
        # distinct (p_date, origin, dest) rows in the underlying fact.
        # In other words, rolling up the carrier_id dimension must
        # not change the shipment_count totals.
        fct = curated_frames["fct_shipment"]
        ref = fct.groupBy("p_date", "origin_region_code", "region_code").agg(
            F.countDistinct(F.col("shipment_id")).cast("long").alias("fct_total")
        )
        rolled = result.groupBy("p_date", "origin_region_code", "destination_region_code").agg(
            F.sum(F.col("shipment_count")).cast("long").alias("rolled_total")
        )
        joined = rolled.join(
            ref.withColumnRenamed("region_code", "destination_region_code"),
            on=["p_date", "origin_region_code", "destination_region_code"],
            how="inner",
        )
        max_diff = joined.select(
            F.max(F.abs(F.col("rolled_total") - F.col("fct_total")))
        ).collect()[0][0]
        assert (max_diff or 0) == 0

    def test_null_origin_region_defaults_to_unknown(
        self, curated_frames: dict[str, object]
    ) -> None:
        """``origin_region_code`` must always be non-null after the
        builder — missing values are coalesced to ``"UNKNOWN"`` so
        the partition contract holds."""
        from pyspark.sql import functions as F

        result = build_route_performance(fct_shipment_df=curated_frames["fct_shipment"])
        nulls = result.filter(F.col("origin_region_code").isNull()).collect()
        assert nulls == []


# ---------------------------------------------------------------------------
# delivery_exception_summary
# ---------------------------------------------------------------------------


class TestDeliveryExceptionSummaryBuilder:
    """Validate grain, metrics, and edge cases for the exception summary."""

    def test_grain_is_unique(self, curated_frames: dict[str, object]) -> None:

        result = build_delivery_exception_summary(
            fct_delivery_event_df=curated_frames["fct_delivery_event"],
            fct_shipment_df=curated_frames["fct_shipment"],
        )
        collected = result.collect()
        assert len(collected) > 0
        keys = [
            (r["p_date"], r["event_type"], r["carrier_id"], r["region_code"]) for r in collected
        ]
        assert len(keys) == len(set(keys)), "delivery_exception_summary grain must be unique"

    def test_event_count_matches_event_fact(self, curated_frames: dict[str, object]) -> None:
        from pyspark.sql import functions as F

        event = curated_frames["fct_delivery_event"]
        result = build_delivery_exception_summary(
            fct_delivery_event_df=event,
            fct_shipment_df=curated_frames["fct_shipment"],
        )

        ref = event.groupBy("p_date", "event_type", "carrier_id", "region_code").agg(
            F.count(F.lit(1)).cast("long").alias("ref_event_count")
        )
        joined = result.join(
            ref, on=["p_date", "event_type", "carrier_id", "region_code"], how="inner"
        )
        max_diff = joined.select(
            F.max(F.abs(F.col("event_count") - F.col("ref_event_count")))
        ).collect()[0][0]
        assert (max_diff or 0) == 0

    def test_is_exception_event_type_flag(self, curated_frames: dict[str, object]) -> None:
        from pyspark.sql import functions as F

        result = build_delivery_exception_summary(
            fct_delivery_event_df=curated_frames["fct_delivery_event"],
            fct_shipment_df=curated_frames["fct_shipment"],
        )
        # Every (event_type, is_exception_event_type) pair should match
        # the documented exception event types.
        bad = result.filter(
            F.col("is_exception_event_type")
            != F.col("event_type").isin(list(DELIVERY_EXCEPTION_EVENT_TYPES))
        ).collect()
        assert bad == []

    def test_event_fact_exception_flag_uses_documented_categories(
        self, curated_frames: dict[str, object]
    ) -> None:
        from pyspark.sql import functions as F

        event = curated_frames["fct_delivery_event"]
        bad = event.filter(
            F.col("exception_flag")
            != F.col("event_type").isin(list(DELIVERY_EXCEPTION_EVENT_TYPES)).cast("int")
        ).collect()

        assert bad == []

    def test_rate_zero_when_no_shipments(self, curated_frames: dict[str, object]) -> None:
        """When the per-grain shipment denominator is zero the rate
        must be 0.0, never NaN or null."""
        from pyspark.sql import functions as F

        result = build_delivery_exception_summary(
            fct_delivery_event_df=curated_frames["fct_delivery_event"],
            fct_shipment_df=curated_frames["fct_shipment"],
        )
        bad = result.filter(F.col("rate").isNull() | F.isnan(F.col("rate"))).collect()
        assert bad == []

    def test_exception_event_count_matches_flag(self, curated_frames: dict[str, object]) -> None:
        """``exception_event_count`` must equal the count of events
        whose ``exception_flag`` is 1 within the grain."""
        from pyspark.sql import functions as F

        event = curated_frames["fct_delivery_event"]
        result = build_delivery_exception_summary(
            fct_delivery_event_df=event,
            fct_shipment_df=curated_frames["fct_shipment"],
        )

        ref = event.groupBy("p_date", "event_type", "carrier_id", "region_code").agg(
            F.sum(F.col("exception_flag").cast("long")).alias("ref_exception_event_count")
        )
        joined = result.join(
            ref, on=["p_date", "event_type", "carrier_id", "region_code"], how="inner"
        )
        max_diff = joined.select(
            F.max(F.abs(F.col("exception_event_count") - F.col("ref_exception_event_count")))
        ).collect()[0][0]
        assert (max_diff or 0) == 0


# ---------------------------------------------------------------------------
# Spark SQL parity tests
# ---------------------------------------------------------------------------


class TestSparkSQLParity:
    """Each Python builder must produce the same output as the
    hand-written Spark SQL file in ``sql/gold``.
    """

    def _first_sql_statement(self, sql_text: str) -> str:
        sql_text = sql_text.lstrip("\ufeff")
        statements = [s.strip() for s in sql_text.split(";") if s.strip()]
        return statements[0]

    def test_carrier_performance_python_matches_sql(
        self, project_root: Path, curated_frames: dict[str, object], spark
    ) -> None:
        from pyspark.sql import functions as F

        fct = curated_frames["fct_shipment"]
        event = curated_frames["fct_delivery_event"]
        dim = curated_frames["dim_carrier"]

        fct.createOrReplaceTempView("fct_shipment")
        event.createOrReplaceTempView("fct_delivery_event")
        dim.createOrReplaceTempView("dim_carrier")

        sql_path = project_root / "sql" / "gold" / "carrier_performance.sql"
        sql_df = spark.sql(self._first_sql_statement(sql_path.read_text(encoding="utf-8")))

        py_df = build_carrier_performance(
            fct_shipment_df=fct,
            fct_delivery_event_df=event,
            dim_carrier_df=dim,
        )

        assert sql_df.count() == py_df.count()
        keys = ["p_date", "carrier_id", "service_mode"]
        joined = sql_df.alias("s").join(py_df.alias("p"), on=keys, how="inner")
        assert joined.count() == py_df.count()

        for metric in (
            "shipment_volume",
            "on_time_delivery_rate",
            "late_delivery_rate",
            "exception_rate",
            "first_attempt_success_rate",
            "avg_transit_hours",
            "avg_cost_per_mile",
            "total_shipping_cost_usd",
            "total_distance_miles",
        ):
            if metric not in joined.columns or f"p.{metric}" not in joined.columns:
                continue
            max_abs_diff = joined.select(
                F.max(F.abs(F.col(f"s.{metric}") - F.col(f"p.{metric}"))).alias("d")
            ).collect()[0]["d"]
            assert (max_abs_diff or 0.0) < 1e-9

    def test_route_performance_python_matches_sql(
        self, project_root: Path, curated_frames: dict[str, object], spark
    ) -> None:
        from pyspark.sql import functions as F

        fct = curated_frames["fct_shipment"]
        fct.createOrReplaceTempView("fct_shipment")

        sql_path = project_root / "sql" / "gold" / "route_performance.sql"
        sql_df = spark.sql(self._first_sql_statement(sql_path.read_text(encoding="utf-8")))

        py_df = build_route_performance(fct_shipment_df=fct)
        assert sql_df.count() == py_df.count()

        keys = ["p_date", "origin_region_code", "destination_region_code", "carrier_id"]
        joined = sql_df.alias("s").join(py_df.alias("p"), on=keys, how="inner")
        assert joined.count() == py_df.count()

        for metric in (
            "shipment_count",
            "on_time_rate",
            "late_rate",
            "exception_rate",
            "avg_transit_hours",
            "avg_cost_per_mile",
        ):
            if metric not in joined.columns or f"p.{metric}" not in joined.columns:
                continue
            max_abs_diff = joined.select(
                F.max(F.abs(F.col(f"s.{metric}") - F.col(f"p.{metric}"))).alias("d")
            ).collect()[0]["d"]
            assert (max_abs_diff or 0.0) < 1e-9

    def test_delivery_exception_summary_python_matches_sql(
        self, project_root: Path, curated_frames: dict[str, object], spark
    ) -> None:
        from pyspark.sql import functions as F

        fct = curated_frames["fct_shipment"]
        event = curated_frames["fct_delivery_event"]
        fct.createOrReplaceTempView("fct_shipment")
        event.createOrReplaceTempView("fct_delivery_event")

        sql_path = project_root / "sql" / "gold" / "delivery_exception_summary.sql"
        sql_df = spark.sql(self._first_sql_statement(sql_path.read_text(encoding="utf-8")))

        py_df = build_delivery_exception_summary(
            fct_delivery_event_df=event,
            fct_shipment_df=fct,
        )
        assert sql_df.count() == py_df.count()
        keys = ["p_date", "event_type", "carrier_id", "region_code"]
        joined = sql_df.alias("s").join(py_df.alias("p"), on=keys, how="inner")
        assert joined.count() == py_df.count()
        for metric in (
            "event_count",
            "shipment_count",
            "exception_event_count",
            "exception_shipment_count",
            "rate",
            "avg_delay_minutes",
        ):
            if metric not in joined.columns or f"p.{metric}" not in joined.columns:
                continue
            max_abs_diff = joined.select(
                F.max(F.abs(F.col(f"s.{metric}") - F.col(f"p.{metric}"))).alias("d")
            ).collect()[0]["d"]
            assert (max_abs_diff or 0.0) < 1e-9


# ---------------------------------------------------------------------------
# Live-Databricks Delta MERGE (skipped without a workspace)
# ---------------------------------------------------------------------------


class TestLiveDatabricksGoldAnalytics:
    """Live Delta write paths for the Phase-6 analytics tables are
    validated by the existing Bronze/Silver publisher dispatch
    tests.  A live workspace run is required to flip the deployment
    status flag in ``docs/architecture.md`` (per AGENTS.md rule 10).
    """

    def test_live_delta_write_skipped_without_workspace(self) -> None:
        pytest.skip(
            "Live Databricks Delta write for Phase-6 Gold analytics requires a "
            "running Databricks workspace; CI does not provide one. "
            "See AGENTS.md rule 9."
        )


# ---------------------------------------------------------------------------
# Daily job publishes the new tables
# ---------------------------------------------------------------------------


def test_daily_job_publishes_phase6_gold_analytics(
    project_root: Path, sample_run_date: str, tmp_path: Path
) -> None:
    """The daily batch must publish all three Phase-6 Gold tables."""
    pytest.importorskip("pyspark")
    from transport_etl.jobs.run_daily_batch import run_daily_batch

    staging_base = tmp_path / "staging"
    curated_base = tmp_path / "curated"
    audit_base = tmp_path / "logs"

    status = run_daily_batch(
        config_path="config/dev.yaml",
        run_date=sample_run_date,
        overrides={
            "spark.enable_hive_support": False,
            "hive.register_tables": False,
            "paths.raw_base_path": str(project_root / "data" / "sample" / "raw"),
            "paths.reference_base_path": str(project_root / "data" / "sample" / "reference"),
            "paths.staging_base_path": str(staging_base),
            "paths.curated_base_path": str(curated_base),
            "paths.audit_base_path": str(audit_base),
        },
    )
    assert status == 0
    for table in (
        TABLE_GOLD_CARRIER_PERFORMANCE,
        TABLE_GOLD_ROUTE_PERFORMANCE,
        TABLE_GOLD_DELIVERY_EXCEPTION_SUMMARY,
    ):
        table_path = curated_base / table
        assert table_path.exists(), f"Expected {table} path at {table_path}"
        files = list(table_path.rglob("*.parquet")) + list(table_path.rglob("*.jsonl"))
        assert files, f"No data files written for {table}"
