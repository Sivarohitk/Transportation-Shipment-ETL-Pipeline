"""Optional AWS Glue Data Catalog registration for S3-backed Gold Parquet.

This adapter changes catalog metadata only.  Spark's existing Parquet writer
and optional Hive registration remain independent of Glue.
"""

from __future__ import annotations

import copy
import re
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence
from urllib.parse import urlsplit

from transport_etl.common.aws_retry import RetryPolicy, classify_aws_error, retry_aws_call
from transport_etl.common.constants import (
    TABLE_AGG_SHIPMENT_DAILY,
    TABLE_DIM_CARRIER,
    TABLE_FCT_DELIVERY_EVENT,
    TABLE_FCT_SHIPMENT,
    TABLE_KPI_DELIVERY_DAILY,
)
from transport_etl.monitor.audit import sanitize_error
from transport_etl.publish.partitions import required_partition_columns

GLUE_TABLE_ORDER = (
    TABLE_DIM_CARRIER,
    TABLE_FCT_SHIPMENT,
    TABLE_FCT_DELIVERY_EVENT,
    TABLE_AGG_SHIPMENT_DAILY,
    TABLE_KPI_DELIVERY_DAILY,
)
_NAME = re.compile(r"^[a-z_][a-z0-9_]{0,254}$")
_TYPE = re.compile(r"^[a-z0-9_,<>(): ]+$")
_PARTITION_VALUE = re.compile(r"^[A-Za-z0-9_.-]+$")
_PARQUET_INPUT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
_PARQUET_OUTPUT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
_PARQUET_SERDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"


class GlueCatalogError(ValueError):
    """Raised for invalid or incompatible Glue catalog metadata."""


class GlueClient(Protocol):
    """Injectable subset of the boto3 Glue client."""

    def get_database(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def create_database(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def get_table(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def create_table(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def update_table(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def batch_create_partition(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def update_partition(self, **kwargs: Any) -> Mapping[str, Any]: ...


def _enabled(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "on"}


def _name(value: str, label: str) -> str:
    if not _NAME.fullmatch(value):
        raise GlueCatalogError(f"Invalid Glue {label}: {value!r}")
    return value


def _s3_location(value: str) -> str:
    uri = urlsplit(value)
    if uri.scheme != "s3" or not uri.netloc or not uri.path.strip("/"):
        raise GlueCatalogError(f"Glue table location must be an S3 prefix: {value!r}")
    if uri.query or uri.fragment:
        raise GlueCatalogError("Glue table location cannot have query or fragment")
    return value.rstrip("/") + "/"


def _is_not_found(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if not isinstance(response, Mapping):
        return False
    detail = response.get("Error")
    return isinstance(detail, Mapping) and detail.get("Code") == "EntityNotFoundException"


def _is_already_exists(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    detail = response.get("Error") if isinstance(response, Mapping) else None
    return isinstance(detail, Mapping) and detail.get("Code") == "AlreadyExistsException"


@dataclass(frozen=True)
class GlueCatalogConfig:
    """Validated Glue settings; all AWS fields stay optional while disabled."""

    enabled: bool = False
    region: str = ""
    database: str = ""
    catalog_id: str = ""
    update_existing_tables: bool = True
    register_partitions: bool = True
    failure_policy: str = "fail"

    @classmethod
    def from_mapping(cls, config: Mapping[str, Any]) -> GlueCatalogConfig:
        """Parse the full app configuration or its ``glue`` subsection."""
        raw = config.get("glue", config)
        if not isinstance(raw, Mapping):
            raise GlueCatalogError("glue configuration must be a mapping")
        settings = cls(
            enabled=_enabled(raw.get("enabled", False)),
            region=str(raw.get("region", "")).strip(),
            database=str(raw.get("database", "")).strip(),
            catalog_id=str(raw.get("catalog_id", "")).strip(),
            update_existing_tables=_enabled(raw.get("update_existing_tables", True)),
            register_partitions=_enabled(raw.get("register_partitions", True)),
            failure_policy=str(raw.get("failure_policy", "fail")).strip().lower(),
        )
        if not settings.enabled:
            return settings
        if not settings.region or "${" in settings.region:
            raise GlueCatalogError("glue.region must be configured when Glue is enabled")
        _name(settings.database, "database")
        if settings.catalog_id and not re.fullmatch(r"[0-9]{12}", settings.catalog_id):
            raise GlueCatalogError("glue.catalog_id must be a 12-digit AWS account ID")
        if settings.failure_policy not in {"fail", "warn"}:
            raise GlueCatalogError("glue.failure_policy must be 'fail' or 'warn'")
        return settings


def spark_type_to_glue(data_type: Any) -> str:
    """Convert a Spark type to a Glue/Hive type without importing PySpark."""
    raw = (
        str(data_type.simpleString()).lower()
        if hasattr(data_type, "simpleString")
        else str(data_type).lower()
    )
    mapped = {"integer": "int", "long": "bigint", "bool": "boolean"}.get(raw, raw)
    if not _TYPE.fullmatch(mapped):
        raise GlueCatalogError(f"Invalid Spark schema type for Glue: {raw!r}")
    return mapped


def build_table_input(
    table_name: str, location: str, schema: Any, partition_keys: Sequence[str]
) -> dict[str, Any]:
    """Build a Glue external Parquet table definition from a Spark schema."""
    _name(table_name, "table name")
    s3_location = _s3_location(location)
    fields = getattr(schema, "fields", None)
    if not isinstance(fields, (list, tuple)) or not fields:
        raise GlueCatalogError("Glue registration requires a nonempty Spark schema")

    columns: dict[str, str] = {}
    for field in fields:
        field_name = _name(str(getattr(field, "name", "")), "column name")
        if field_name in columns:
            raise GlueCatalogError(f"Duplicate Glue column: {field_name}")
        columns[field_name] = spark_type_to_glue(getattr(field, "dataType", ""))

    keys = [_name(str(key), "partition key") for key in partition_keys]
    if not keys or len(keys) != len(set(keys)) or any(key not in columns for key in keys):
        raise GlueCatalogError("Glue partition keys must be distinct columns in the Spark schema")
    data_columns = [
        {"Name": name, "Type": dtype} for name, dtype in columns.items() if name not in keys
    ]
    if not data_columns:
        raise GlueCatalogError("Glue table requires at least one non-partition data column")

    return {
        "Name": table_name,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
        "PartitionKeys": [{"Name": name, "Type": columns[name]} for name in keys],
        "StorageDescriptor": {
            "Columns": data_columns,
            "Location": s3_location,
            "InputFormat": _PARQUET_INPUT,
            "OutputFormat": _PARQUET_OUTPUT,
            "SerdeInfo": {"SerializationLibrary": _PARQUET_SERDE},
        },
    }


def _compatible(existing: Mapping[str, Any], desired: Mapping[str, Any]) -> bool:
    """Allow appended data columns and location changes, not breaking schema drift."""
    old_storage = existing.get("StorageDescriptor", {})
    new_storage = desired["StorageDescriptor"]
    if not isinstance(old_storage, Mapping):
        return False
    old_columns = old_storage.get("Columns", [])
    new_columns = new_storage["Columns"]
    return bool(
        existing.get("TableType") == "EXTERNAL_TABLE"
        and existing.get("PartitionKeys") == desired["PartitionKeys"]
        and old_storage.get("InputFormat") == _PARQUET_INPUT
        and old_storage.get("OutputFormat") == _PARQUET_OUTPUT
        and isinstance(old_columns, list)
        and old_columns == new_columns[: len(old_columns)]
    )


@dataclass(frozen=True)
class GlueRegistrationResult:
    """One table's Glue registration outcome."""

    table: str
    location: str
    status: str
    partitions: int = 0
    error: str = ""


class GlueCatalogAdapter:
    """Thin Glue API adapter with an injectable boto3-compatible client."""

    def __init__(
        self,
        config: GlueCatalogConfig,
        client: GlueClient | None = None,
        *,
        retry_policy: RetryPolicy | None = None,
        sleep: Callable[[float], None] = time.sleep,
        logger: Any | None = None,
    ) -> None:
        self.config = config
        self.retry_policy = retry_policy or RetryPolicy()
        self.sleep = sleep
        self.logger = logger
        if client is None:
            try:
                import boto3
            except ModuleNotFoundError as exc:  # pragma: no cover - optional install
                raise ModuleNotFoundError(
                    "Glue registration requires the optional AWS dependency: pip install .[aws]"
                ) from exc
            client = boto3.client("glue", region_name=config.region)
        self.client = client

    def _call(self, operation: str, call: Callable[[], Any]) -> Any:
        return retry_aws_call(
            call,
            operation=f"glue.{operation}",
            policy=self.retry_policy,
            sleep=self.sleep,
            logger=self.logger,
        )

    def _scope(self) -> dict[str, str]:
        return {"CatalogId": self.config.catalog_id} if self.config.catalog_id else {}

    def get_database(self) -> Mapping[str, Any] | None:
        """Fetch the configured database; return ``None`` only when missing."""
        try:
            return self._call(
                "get_database",
                lambda: self.client.get_database(Name=self.config.database, **self._scope()),
            )["Database"]
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise

    def ensure_database(self) -> str:
        """Create the metadata database if it does not already exist."""
        if self.get_database() is not None:
            return "existing"
        try:
            self._call(
                "create_database",
                lambda: self.client.create_database(
                    DatabaseInput={"Name": self.config.database}, **self._scope()
                ),
            )
        except Exception as exc:
            # A timed-out create may have committed; confirm before failing.
            if classify_aws_error(exc) == "permanent" and not _is_already_exists(exc):
                raise exc
            if self.get_database() is None:
                raise exc
        return "created"

    def get_table(self, table_name: str) -> Mapping[str, Any] | None:
        """Fetch one Glue table; return ``None`` only when missing."""
        _name(table_name, "table name")
        try:
            return self._call(
                "get_table",
                lambda: self.client.get_table(
                    DatabaseName=self.config.database, Name=table_name, **self._scope()
                ),
            )["Table"]
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise

    def register_table(
        self, table_name: str, location: str, schema: Any, partition_keys: Sequence[str]
    ) -> str:
        """Create or compatibly update a curated external Parquet table."""
        desired = build_table_input(table_name, location, schema, partition_keys)
        current = self.get_table(table_name)
        args = {"DatabaseName": self.config.database, "TableInput": desired, **self._scope()}
        if current is None:
            try:
                self._call("create_table", lambda: self.client.create_table(**args))
            except Exception as exc:
                # Reconcile a create whose response was lost after commit.
                if classify_aws_error(exc) == "permanent" and not _is_already_exists(exc):
                    raise exc
                recovered = self.get_table(table_name)
                if recovered is None:
                    raise exc
                if not _compatible(recovered, desired):
                    raise GlueCatalogError(
                        f"Existing Glue table {table_name} has incompatible metadata"
                    ) from exc
            return "created"
        if not _compatible(current, desired):
            raise GlueCatalogError(f"Existing Glue table {table_name} has incompatible metadata")
        old_storage = current["StorageDescriptor"]
        new_storage = desired["StorageDescriptor"]
        if (
            old_storage.get("Columns") == new_storage["Columns"]
            and old_storage.get("Location") == new_storage["Location"]
            and old_storage.get("SerdeInfo") == new_storage["SerdeInfo"]
            and current.get("Parameters", {}).get("classification") == "parquet"
        ):
            return "unchanged"
        if not self.config.update_existing_tables:
            return "update_disabled"
        desired["Parameters"] = {
            **dict(current.get("Parameters", {})),
            **desired["Parameters"],
        }
        self._call("update_table", lambda: self.client.update_table(**args))
        return "updated"

    def register_partitions(
        self,
        table_name: str,
        table_input: Mapping[str, Any],
        values: Iterable[Sequence[str]],
    ) -> int:
        """Create listed partitions, updating existing locations on reruns."""
        keys = [item["Name"] for item in table_input["PartitionKeys"]]
        descriptor = table_input["StorageDescriptor"]
        root = descriptor["Location"]
        entries: list[dict[str, Any]] = []
        for raw_values in values:
            partition_values = [str(value) for value in raw_values]
            if len(partition_values) != len(keys) or any(
                not _PARTITION_VALUE.fullmatch(value) for value in partition_values
            ):
                raise GlueCatalogError(
                    f"Invalid partition values for {table_name}: {partition_values}"
                )
            suffix = "/".join(f"{key}={value}" for key, value in zip(keys, partition_values))
            item_descriptor = copy.deepcopy(descriptor)
            item_descriptor["Location"] = root + suffix + "/"
            entries.append({"Values": partition_values, "StorageDescriptor": item_descriptor})

        total = 0
        for start in range(0, len(entries), 100):
            chunk = entries[start : start + 100]
            response = self._call(
                "batch_create_partition",
                lambda: self.client.batch_create_partition(
                    DatabaseName=self.config.database,
                    TableName=table_name,
                    PartitionInputList=chunk,
                    **self._scope(),
                ),
            )
            by_values = {tuple(entry["Values"]): entry for entry in chunk}
            for error in response.get("Errors", []):
                detail = error.get("ErrorDetail", {})
                code = detail.get("ErrorCode")
                partition = by_values.get(tuple(error.get("PartitionValues", [])))
                if code != "AlreadyExistsException" or partition is None:
                    raise GlueCatalogError(f"Glue partition registration failed: {error}")
                self._call(
                    "update_partition",
                    lambda: self.client.update_partition(
                        DatabaseName=self.config.database,
                        TableName=table_name,
                        PartitionValueList=partition["Values"],
                        PartitionInput=partition,
                        **self._scope(),
                    ),
                )
            total += len(chunk)
        return total


def publish_curated_to_glue(
    config: Mapping[str, Any],
    locations: Mapping[str, str],
    schemas: Mapping[str, Any],
    *,
    partition_values: Mapping[str, Iterable[Sequence[str]]] | None = None,
    client: GlueClient | None = None,
    logger: Any | None = None,
) -> list[GlueRegistrationResult]:
    """Register the five core Gold S3 outputs with configured failure policy."""
    settings = GlueCatalogConfig.from_mapping(config)
    if not settings.enabled:
        return []

    try:
        adapter = GlueCatalogAdapter(
            settings,
            client=client,
            retry_policy=RetryPolicy.from_config(config),
            logger=logger,
        )
        adapter.ensure_database()
    except Exception as exc:
        if settings.failure_policy == "fail":
            raise
        safe_error = sanitize_error(exc, config)
        if logger is not None:
            logger.warning("Glue database registration failed: %s", safe_error)
        return [GlueRegistrationResult("", "", "warning", error=safe_error)]

    results: list[GlueRegistrationResult] = []
    keys = required_partition_columns()
    for table in GLUE_TABLE_ORDER:
        try:
            location = locations[table]
            schema = schemas[table]
            status = adapter.register_table(table, location, schema, keys)
            count = 0
            if settings.register_partitions and status != "update_disabled":
                if partition_values is None or table not in partition_values:
                    raise GlueCatalogError(f"Missing partition values for Glue table {table}")
                table_input = build_table_input(table, location, schema, keys)
                count = adapter.register_partitions(table, table_input, partition_values[table])
            result = GlueRegistrationResult(table, location, status, count)
        except Exception as exc:
            if settings.failure_policy == "fail":
                raise
            location = locations.get(table, "")
            safe_error = sanitize_error(exc, config)
            result = GlueRegistrationResult(table, location, "warning", error=safe_error)
            if logger is not None:
                logger.warning("Glue registration failed table=%s error=%s", table, safe_error)
        results.append(result)
    return results
