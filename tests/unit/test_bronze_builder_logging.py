"""Bronze metadata logging must not launch a separate Spark count job."""

from __future__ import annotations

from transport_etl.bronze.builder import _log_summary


def test_bronze_summary_does_not_trigger_dataframe_action():
    class LazyFrame:
        def count(self):
            raise AssertionError("logging should not count a large DataFrame")

    _log_summary(LazyFrame(), entity="shipments", source_file="synthetic.csv")
