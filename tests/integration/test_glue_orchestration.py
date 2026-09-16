"""Gold-write to Glue registration order, without AWS credentials."""

from __future__ import annotations

from transport_etl.jobs import run_daily_batch as daily_module
from transport_etl.publish.glue_catalog import GLUE_TABLE_ORDER


class _NotFound(Exception):
    response = {"Error": {"Code": "EntityNotFoundException"}}


class _Field:
    def __init__(self, name, data_type):
        self.name = name
        self.dataType = data_type


class _Frame:
    def __init__(self):
        self.schema = type(
            "Schema",
            (),
            {
                "fields": [
                    _Field("value", "string"),
                    _Field("p_date", "date"),
                    _Field("region_code", "string"),
                    _Field("carrier_id", "string"),
                ]
            },
        )()


class _FakeGlue:
    def __init__(self, events):
        self.events = events

    def get_database(self, **kwargs):
        self.events.append("glue:get_database")
        raise _NotFound()

    def create_database(self, **kwargs):
        self.events.append("glue:create_database")

    def get_table(self, **kwargs):
        self.events.append(f"glue:get_table:{kwargs['Name']}")
        raise _NotFound()

    def create_table(self, **kwargs):
        self.events.append(f"glue:create_table:{kwargs['TableInput']['Name']}")


def test_glue_registration_occurs_only_after_all_gold_writes(monkeypatch) -> None:
    events: list[str] = []

    def fake_write_partitioned_table(**kwargs):
        name = kwargs["table_name"]
        events.append(f"write:{name}")
        return f"s3://test-bucket/curated/{name}"

    monkeypatch.setattr(daily_module, "write_partitioned_table", fake_write_partitioned_table)
    frames = [(name, _Frame()) for name in GLUE_TABLE_ORDER]
    config = {
        "glue": {
            "enabled": True,
            "region": "us-east-1",
            "database": "transport_curated",
            "register_partitions": False,
            "failure_policy": "fail",
        }
    }

    outputs, glue_results = daily_module._write_gold_and_register_glue(
        table_writes=frames,
        config=config,
        curated_base_path="s3://test-bucket/curated",
        writer_kwargs={"partitions": ["p_date", "region_code", "carrier_id"]},
        logger=None,
        glue_client=_FakeGlue(events),
    )

    assert list(outputs) == list(GLUE_TABLE_ORDER)
    assert [result["table"] for result in glue_results] == list(GLUE_TABLE_ORDER)
    assert all(result["status"] == "created" for result in glue_results)
    assert events[:5] == [f"write:{name}" for name in GLUE_TABLE_ORDER]
    assert events[5] == "glue:get_database"
