"""Optional CloudWatch metrics with a credential-free local sink."""

from __future__ import annotations

from typing import Any, Mapping, Protocol


class MetricsSink(Protocol):
    """Emit metrics for one completed run attempt."""

    def emit(self, record: Mapping[str, Any]) -> None: ...


def build_metric_data(record: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Map an audit record to low-cardinality CloudWatch metric data."""
    status = str(record.get("status", ""))
    if status not in {"success", "failed"}:
        return []
    common = [
        {"Name": "Job", "Value": str(record.get("job", "unknown"))},
        {"Name": "Environment", "Value": str(record.get("environment", "unknown"))},
    ]
    data: list[dict[str, Any]] = []

    def add(
        name: str, value: int | float, *, entity: str | None = None, unit: str = "Count"
    ) -> None:
        dimensions = list(common)
        if entity is not None:
            dimensions.append({"Name": "Entity", "Value": entity})
        data.append(
            {
                "MetricName": name,
                "Dimensions": dimensions,
                "Value": float(value),
                "Unit": unit,
            }
        )

    add("PipelineSuccess" if status == "success" else "PipelineFailure", 1)
    add("PipelineDurationSeconds", float(record.get("duration_seconds", 0)), unit="Seconds")
    for field, metric in (
        ("source_rows", "RowsRead"),
        ("curated_rows", "RowsWritten"),
        ("rejected_rows", "RowsRejected"),
    ):
        for entity, count in record.get(field, {}).items():
            if int(count) >= 0:
                add(metric, int(count), entity=str(entity))
    for entity, rules in record.get("quality_failures", {}).items():
        if rules:
            add("DataQualityFailures", len(rules), entity=str(entity))
            drift_count = sum("schema_drift" in str(rule).lower() for rule in rules)
            if drift_count:
                add("SchemaDriftFailures", drift_count, entity=str(entity))
    if record.get("redshift_status") == "failed":
        add("RedshiftLoadFailures", 1)
    if record.get("glue_status") in {"failed", "warning"}:
        add("GlueCatalogFailures", 1)
    return data


class NoOpMetricsSink:
    """Deliberate disabled sink; never constructs an AWS client."""

    def emit(self, record: Mapping[str, Any]) -> None:
        """Ignore the record when CloudWatch is disabled."""


class CloudWatchMetricsSink:
    """Put audit-derived metrics through an injectable boto3 CloudWatch client."""

    def __init__(self, namespace: str, region: str, *, client: Any | None = None) -> None:
        if not namespace.strip() or not region.strip():
            raise ValueError("CloudWatch namespace and region are required")
        self.namespace = namespace
        if client is None:
            try:
                import boto3
            except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
                raise ModuleNotFoundError("CloudWatch metrics require pip install .[aws]") from exc
            client = boto3.client("cloudwatch", region_name=region)
        self.client = client

    def emit(self, record: Mapping[str, Any]) -> None:
        """Publish one small batch, omitting skipped runs."""
        data = build_metric_data(record)
        if data:
            self.client.put_metric_data(Namespace=self.namespace, MetricData=data)


def create_metrics_sink(config: Mapping[str, Any], *, client: Any | None = None) -> MetricsSink:
    """Select the CloudWatch sink only when explicitly enabled."""
    settings = config.get("cloudwatch", {})
    if not isinstance(settings, Mapping):
        raise ValueError("cloudwatch configuration must be a mapping")
    if not bool(settings.get("enabled", False)):
        return NoOpMetricsSink()
    return CloudWatchMetricsSink(
        str(settings.get("namespace", "")), str(settings.get("region", "")), client=client
    )
