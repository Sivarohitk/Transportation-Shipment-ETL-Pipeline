"""Target-aware Gold publication and daily quality-policy contracts."""

import pytest

from transport_etl.jobs.run_daily_batch import (
    _blocking_quality_failures,
    _resolve_gold_write_target,
)


def test_databricks_gold_uses_qualified_delta_table() -> None:
    config = {
        "spark": {"profile": "databricks"},
        "unity_catalog": {
            "catalog": "supply_chain",
            "gold_schema": "gold_dev",
        },
    }

    assert _resolve_gold_write_target(config, "fct_shipment") == (
        "supply_chain.gold_dev.fct_shipment",
        "delta",
    )


def test_local_gold_preserves_parquet_table_name() -> None:
    config = {
        "spark": {"profile": "local"},
        "hive": {"database": "curated"},
    }

    assert _resolve_gold_write_target(config, "fct_shipment") == (
        "fct_shipment",
        "parquet",
    )


@pytest.mark.parametrize(
    ("config_key", "rule_name"),
    [
        ("fail_on_schema_drift", "schema_drift"),
        ("fail_on_required_nulls", "required_nulls"),
        ("fail_on_duplicate_primary_keys", "duplicate_keys"),
    ],
)
def test_quality_failure_is_blocking_only_when_configured(
    config_key: str,
    rule_name: str,
) -> None:
    assert _blocking_quality_failures([rule_name], {config_key: True}) == [rule_name]
    assert _blocking_quality_failures([rule_name], {config_key: False}) == []


def test_quality_policy_returns_all_configured_blockers() -> None:
    failed = ["schema_drift", "required_nulls", "duplicate_keys"]
    config = {
        "fail_on_schema_drift": True,
        "fail_on_required_nulls": True,
        "fail_on_duplicate_primary_keys": False,
    }

    assert _blocking_quality_failures(failed, config) == [
        "schema_drift",
        "required_nulls",
    ]
