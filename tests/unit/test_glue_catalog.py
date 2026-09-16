"""AWS Glue Data Catalog adapter tests with an in-memory client."""

from __future__ import annotations

import copy

import pytest

from transport_etl.publish.glue_catalog import (
    GLUE_TABLE_ORDER,
    GlueCatalogAdapter,
    GlueCatalogConfig,
    GlueCatalogError,
    build_table_input,
    publish_curated_to_glue,
    spark_type_to_glue,
)


class _Field:
    def __init__(self, name: str, data_type: str) -> None:
        self.name = name
        self.dataType = data_type


class _Schema:
    def __init__(self, *fields: _Field) -> None:
        self.fields = list(fields)


SCHEMA = _Schema(
    _Field("shipment_id", "string"),
    _Field("shipping_cost_usd", "double"),
    _Field("p_date", "date"),
    _Field("region_code", "string"),
    _Field("carrier_id", "string"),
)
PARTITIONS = ["p_date", "region_code", "carrier_id"]


class _NotFound(Exception):
    def __init__(self) -> None:
        super().__init__("not found")
        self.response = {"Error": {"Code": "EntityNotFoundException"}}


class _FakeGlueClient:
    def __init__(self) -> None:
        self.databases: dict[str, dict] = {}
        self.tables: dict[tuple[str, str], dict] = {}
        self.created_tables: list[dict] = []
        self.updated_tables: list[dict] = []
        self.created_partitions: list[dict] = []
        self.updated_partitions: list[dict] = []
        self.partition_values: set[tuple[str, ...]] = set()
        self.fail_get_database: Exception | None = None

    def get_database(self, **kwargs):
        if self.fail_get_database:
            raise self.fail_get_database
        name = kwargs["Name"]
        if name not in self.databases:
            raise _NotFound()
        return {"Database": self.databases[name]}

    def create_database(self, **kwargs):
        payload = kwargs["DatabaseInput"]
        self.databases[payload["Name"]] = copy.deepcopy(payload)
        return {}

    def get_table(self, **kwargs):
        key = (kwargs["DatabaseName"], kwargs["Name"])
        if key not in self.tables:
            raise _NotFound()
        return {"Table": copy.deepcopy(self.tables[key])}

    def create_table(self, **kwargs):
        self.created_tables.append(copy.deepcopy(kwargs))
        table = copy.deepcopy(kwargs["TableInput"])
        self.tables[(kwargs["DatabaseName"], table["Name"])] = table
        return {}

    def update_table(self, **kwargs):
        self.updated_tables.append(copy.deepcopy(kwargs))
        table = copy.deepcopy(kwargs["TableInput"])
        self.tables[(kwargs["DatabaseName"], table["Name"])] = table
        return {}

    def batch_create_partition(self, **kwargs):
        self.created_partitions.append(copy.deepcopy(kwargs))
        errors = []
        for entry in kwargs["PartitionInputList"]:
            values = tuple(entry["Values"])
            if values in self.partition_values:
                errors.append(
                    {
                        "PartitionValues": list(values),
                        "ErrorDetail": {"ErrorCode": "AlreadyExistsException"},
                    }
                )
            else:
                self.partition_values.add(values)
        return {"Errors": errors}

    def update_partition(self, **kwargs):
        self.updated_partitions.append(copy.deepcopy(kwargs))
        return {}


def _config(**changes):
    settings = {
        "enabled": True,
        "region": "us-east-1",
        "database": "transport_curated",
        "catalog_id": "",
        "update_existing_tables": True,
        "register_partitions": True,
        "failure_policy": "fail",
    }
    settings.update(changes)
    return {"glue": settings}


def test_disabled_config_needs_no_aws_values() -> None:
    assert GlueCatalogConfig.from_mapping({"glue": {"enabled": False}}).enabled is False


@pytest.mark.parametrize(
    "changes",
    [
        {"region": ""},
        {"database": "bad-name"},
        {"catalog_id": "123"},
        {"failure_policy": "ignore"},
        {"database": "${GLUE_DATABASE}"},
    ],
)
def test_invalid_config_rejected(changes) -> None:
    with pytest.raises(GlueCatalogError):
        GlueCatalogConfig.from_mapping(_config(**changes))


@pytest.mark.parametrize(
    ("spark_type", "glue_type"),
    [
        ("string", "string"),
        ("integer", "int"),
        ("long", "bigint"),
        ("double", "double"),
        ("decimal(12,2)", "decimal(12,2)"),
        ("array<string>", "array<string>"),
        ("timestamp", "timestamp"),
    ],
)
def test_spark_type_mapping(spark_type: str, glue_type: str) -> None:
    assert spark_type_to_glue(spark_type) == glue_type


def test_table_input_excludes_partition_keys_from_data_columns() -> None:
    table = build_table_input(
        "fct_shipment", "s3://bucket/curated/fct_shipment", SCHEMA, PARTITIONS
    )

    assert table["Name"] == "fct_shipment"
    assert [item["Name"] for item in table["StorageDescriptor"]["Columns"]] == [
        "shipment_id",
        "shipping_cost_usd",
    ]
    assert [item["Name"] for item in table["PartitionKeys"]] == PARTITIONS
    assert [item["Type"] for item in table["PartitionKeys"]] == [
        "date",
        "string",
        "string",
    ]
    assert table["StorageDescriptor"]["SerdeInfo"]["SerializationLibrary"].endswith(
        "ParquetHiveSerDe"
    )


@pytest.mark.parametrize(
    "table,location,schema,keys",
    [
        ("bad-name", "s3://bucket/path", SCHEMA, PARTITIONS),
        ("fct_shipment", "/local/path", SCHEMA, PARTITIONS),
        ("fct_shipment", "s3://bucket/path", _Schema(_Field("p_date", "date")), PARTITIONS),
        ("fct_shipment", "s3://bucket/path", SCHEMA, ["missing"]),
    ],
)
def test_invalid_table_metadata_rejected(table, location, schema, keys) -> None:
    with pytest.raises(GlueCatalogError):
        build_table_input(table, location, schema, keys)


def test_database_absent_is_created_and_existing_database_is_reused() -> None:
    client = _FakeGlueClient()
    adapter = GlueCatalogAdapter(GlueCatalogConfig.from_mapping(_config()), client)

    assert adapter.ensure_database() == "created"
    assert adapter.ensure_database() == "existing"
    assert list(client.databases) == ["transport_curated"]


def test_table_absent_is_created_and_existing_compatible_table_updated() -> None:
    client = _FakeGlueClient()
    adapter = GlueCatalogAdapter(GlueCatalogConfig.from_mapping(_config()), client)
    adapter.ensure_database()

    assert (
        adapter.register_table("fct_shipment", "s3://bucket/first", SCHEMA, PARTITIONS) == "created"
    )
    assert (
        adapter.register_table("fct_shipment", "s3://bucket/second", SCHEMA, PARTITIONS)
        == "updated"
    )
    assert (
        client.tables[("transport_curated", "fct_shipment")]["StorageDescriptor"]["Location"]
        == "s3://bucket/second/"
    )
    assert len(client.updated_tables) == 1


def test_appended_data_column_is_compatible() -> None:
    client = _FakeGlueClient()
    adapter = GlueCatalogAdapter(GlueCatalogConfig.from_mapping(_config()), client)
    adapter.ensure_database()
    adapter.register_table("fct_shipment", "s3://bucket/curated", SCHEMA, PARTITIONS)
    extended = _Schema(*SCHEMA.fields, _Field("new_metric", "double"))

    assert (
        adapter.register_table("fct_shipment", "s3://bucket/curated", extended, PARTITIONS)
        == "updated"
    )


def test_unchanged_table_does_not_call_update() -> None:
    client = _FakeGlueClient()
    adapter = GlueCatalogAdapter(GlueCatalogConfig.from_mapping(_config()), client)
    adapter.ensure_database()
    adapter.register_table("fct_shipment", "s3://bucket/curated", SCHEMA, PARTITIONS)

    assert (
        adapter.register_table("fct_shipment", "s3://bucket/curated", SCHEMA, PARTITIONS)
        == "unchanged"
    )
    assert client.updated_tables == []


def test_incompatible_existing_schema_is_rejected() -> None:
    client = _FakeGlueClient()
    adapter = GlueCatalogAdapter(GlueCatalogConfig.from_mapping(_config()), client)
    adapter.ensure_database()
    adapter.register_table("fct_shipment", "s3://bucket/first", SCHEMA, PARTITIONS)
    changed = _Schema(_Field("shipment_id", "int"), *SCHEMA.fields[1:])

    with pytest.raises(GlueCatalogError, match="incompatible"):
        adapter.register_table("fct_shipment", "s3://bucket/first", changed, PARTITIONS)


def test_update_can_be_disabled() -> None:
    client = _FakeGlueClient()
    adapter = GlueCatalogAdapter(
        GlueCatalogConfig.from_mapping(_config(update_existing_tables=False)), client
    )
    adapter.ensure_database()
    adapter.register_table("fct_shipment", "s3://bucket/first", SCHEMA, PARTITIONS)

    assert (
        adapter.register_table("fct_shipment", "s3://bucket/second", SCHEMA, PARTITIONS)
        == "update_disabled"
    )
    assert client.updated_tables == []


def test_partition_registration_creates_then_updates_existing_partition() -> None:
    client = _FakeGlueClient()
    adapter = GlueCatalogAdapter(GlueCatalogConfig.from_mapping(_config()), client)
    adapter.ensure_database()
    table = build_table_input(
        "fct_shipment", "s3://bucket/curated/fct_shipment", SCHEMA, PARTITIONS
    )
    adapter.register_table("fct_shipment", "s3://bucket/curated/fct_shipment", SCHEMA, PARTITIONS)
    values = [("2026-01-01", "EAST", "C1")]

    assert adapter.register_partitions("fct_shipment", table, values) == 1
    assert adapter.register_partitions("fct_shipment", table, values) == 1
    assert client.created_partitions[0]["PartitionInputList"][0]["StorageDescriptor"][
        "Location"
    ].endswith("/p_date=2026-01-01/region_code=EAST/carrier_id=C1/")
    assert len(client.updated_partitions) == 1


def test_disabled_publish_makes_no_client_calls() -> None:
    client = _FakeGlueClient()

    assert publish_curated_to_glue({"glue": {"enabled": False}}, {}, {}, client=client) == []
    assert client.databases == {}


def test_glue_failure_policy_fail_or_warn() -> None:
    client = _FakeGlueClient()
    client.fail_get_database = RuntimeError("access denied")
    locations = {"fct_shipment": "s3://bucket/curated/fct_shipment"}
    schemas = {"fct_shipment": SCHEMA}

    with pytest.raises(RuntimeError, match="access denied"):
        publish_curated_to_glue(_config(failure_policy="fail"), locations, schemas, client=client)

    results = publish_curated_to_glue(
        _config(failure_policy="warn"), locations, schemas, client=client
    )
    assert results[0].status == "warning"
    assert "access denied" in results[0].error


def test_glue_warning_redacts_credentials() -> None:
    client = _FakeGlueClient()
    client.fail_get_database = RuntimeError("denied password=topsecret")
    results = publish_curated_to_glue(_config(failure_policy="warn"), {}, {}, client=client)
    assert "topsecret" not in results[0].error


def test_warn_policy_continues_after_one_table_failure() -> None:
    class _OneTableFails(_FakeGlueClient):
        def get_table(self, **kwargs):
            if kwargs["Name"] == "dim_carrier":
                raise RuntimeError("table permission denied")
            return super().get_table(**kwargs)

    client = _OneTableFails()
    locations = {table: f"s3://bucket/curated/{table}" for table in GLUE_TABLE_ORDER}
    schemas = {table: SCHEMA for table in GLUE_TABLE_ORDER}
    results = publish_curated_to_glue(
        _config(failure_policy="warn", register_partitions=False),
        locations,
        schemas,
        client=client,
    )

    assert results[0].status == "warning"
    assert results[1].status == "created"
