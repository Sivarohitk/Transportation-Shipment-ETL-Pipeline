"""Unit tests for the Silver quarantine writer.

Pure-Python tests where possible.  The writer delegates to
``transport_etl.common.io.write_invalid_records_with_fallback`` so the
heavy lifting is already covered by the IO tests; here we focus on
the Silver-specific labelling and the rule-name validation.
"""

from __future__ import annotations

import pytest

from transport_etl.silver.quarantine import (
    ALL_SILVER_RULE_NAMES,
    RULE_DROPPED_BY_DEDUP,
    RULE_DUPLICATE_KEYS,
    RULE_INVALID_ALLOWED_VALUE,
    RULE_NON_NEGATIVE,
    RULE_REQUIRED_NULLS,
    RULE_SCHEMA_DRIFT,
    RULE_TIMESTAMP_ORDER,
    quarantine_silver_records,
)


class TestSilverRuleNames:
    def test_all_rule_names_present(self) -> None:
        for rule in (
            RULE_REQUIRED_NULLS,
            RULE_DUPLICATE_KEYS,
            RULE_INVALID_ALLOWED_VALUE,
            RULE_NON_NEGATIVE,
            RULE_TIMESTAMP_ORDER,
            RULE_SCHEMA_DRIFT,
            RULE_DROPPED_BY_DEDUP,
        ):
            assert rule in ALL_SILVER_RULE_NAMES

    def test_rule_names_are_unique(self) -> None:
        assert len(ALL_SILVER_RULE_NAMES) == len(set(ALL_SILVER_RULE_NAMES))

    def test_rule_names_have_silver_prefix(self) -> None:
        """Every Silver rule name must start with ``silver_`` so the
        quarantine layout is easy to identify downstream."""
        for rule in ALL_SILVER_RULE_NAMES:
            assert rule.startswith("silver_")


class TestQuarantineSilverRecordsValidation:
    def test_empty_path_raises(self) -> None:
        with pytest.raises(ValueError, match="quarantine_path must be configured"):
            quarantine_silver_records(
                invalid_df=None,  # type: ignore[arg-type]
                quarantine_path="",
                table_name="stg_shipments",
                rule_name=RULE_REQUIRED_NULLS,
            )

    def test_unknown_rule_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown Silver rule name"):
            quarantine_silver_records(
                invalid_df=None,  # type: ignore[arg-type]
                quarantine_path="/tmp/q",
                table_name="stg_shipments",
                rule_name="not_a_real_rule",
            )

    def test_known_rule_names_accepted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The validator must accept every documented rule name."""
        # Provide a stub DataFrame so the call reaches the rule-name
        # check.  We never actually call ``withColumn`` because we
        # stub both the enrichment helper and the writer.
        from transport_etl.silver import quarantine as q_mod

        class _StubDf:
            columns: list[str] = ["shipment_id", "__rule_name", "__rule_reason"]

            def withColumn(self, name, expr):  # noqa: ARG002
                return self

            def count(self) -> int:
                return 0

        monkeypatch.setattr(q_mod, "_enrich_with_source_identity", lambda df, **_: df)

        def fake_writer(df, destination, mode, write_config, logger):  # noqa: ARG001
            return "parquet"

        monkeypatch.setattr(
            "transport_etl.silver.quarantine.write_invalid_records_with_fallback",
            fake_writer,
        )

        for rule in ALL_SILVER_RULE_NAMES:
            quarantine_silver_records(
                invalid_df=_StubDf(),  # type: ignore[arg-type]
                quarantine_path="/tmp/q",
                table_name="stg_shipments",
                rule_name=rule,
                batch_id="b1",
                run_date="2026-01-01",
            )
