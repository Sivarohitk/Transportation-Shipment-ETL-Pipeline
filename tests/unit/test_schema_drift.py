"""Unit tests for schema drift checks."""

from __future__ import annotations

from transport_etl.quality.schema_drift import detect_df_schema_drift, detect_schema_drift


def test_detect_schema_drift_no_findings_for_equal_schema() -> None:
    """Schema drift checker should return no findings for equivalent schemas."""
    expected = {
        "columns": [
            {"name": "shipment_id", "type": "string", "nullable": False},
            {"name": "carrier_id", "type": "string", "nullable": False},
        ]
    }
    actual = {
        "columns": [
            {"name": "shipment_id", "type": "string", "nullable": False},
            {"name": "carrier_id", "type": "string", "nullable": False},
        ]
    }

    assert detect_schema_drift(actual_schema=actual, expected_schema=expected) == []


def test_detect_schema_drift_finds_missing_unexpected_and_type_mismatch() -> None:
    """Schema drift checker should emit findings for key schema mismatches."""
    expected = {
        "columns": [
            {"name": "shipment_id", "type": "string", "nullable": False},
            {"name": "carrier_id", "type": "int", "nullable": False},
            {"name": "pickup_ts", "type": "timestamp", "nullable": True},
        ]
    }
    actual = {
        "columns": [
            {"name": "shipment_id", "type": "string", "nullable": False},
            {"name": "carrier_id", "type": "string", "nullable": False},
            {"name": "extra_col", "type": "string", "nullable": True},
        ]
    }

    findings = detect_schema_drift(actual_schema=actual, expected_schema=expected)

    assert "missing_column:pickup_ts" in findings
    assert "unexpected_column:extra_col" in findings
    assert "type_mismatch:carrier_id:expected=integer:actual=string" in findings


def test_detect_df_schema_drift_on_spark_dataframe(spark) -> None:
    """DataFrame schema drift helper should inspect DataFrame schema correctly."""
    df = spark.createDataFrame(
        [("SHP1", "CAR1")],
        schema="shipment_id string, carrier_id string",
    )
    expected = {
        "columns": [
            {"name": "shipment_id", "type": "string", "nullable": True},
            {"name": "carrier_id", "type": "string", "nullable": True},
            {"name": "pickup_ts", "type": "timestamp", "nullable": True},
        ]
    }

    findings = detect_df_schema_drift(df=df, expected_schema=expected)

    assert "missing_column:pickup_ts" in findings
