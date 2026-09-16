"""Bounded AWS retry behavior; all delays use an injected clock."""

from __future__ import annotations

import pytest

from transport_etl.common.aws_retry import RetryPolicy, classify_aws_error, retry_aws_call


class _AwsError(Exception):
    def __init__(self, code: str, status: int = 400):
        super().__init__("password=do-not-log")
        self.response = {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}


def test_first_attempt_does_not_sleep() -> None:
    delays: list[float] = []
    assert retry_aws_call(lambda: "ok", operation="s3.get_object", sleep=delays.append) == "ok"
    assert delays == []


def test_transient_then_success_and_backoff() -> None:
    calls = 0
    delays: list[float] = []

    def call() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise _AwsError("ThrottlingException", 429)
        return "ok"

    assert (
        retry_aws_call(
            call,
            operation="glue.get_table",
            policy=RetryPolicy(max_attempts=3, initial_delay=1, max_delay=10, jitter=0),
            sleep=delays.append,
        )
        == "ok"
    )
    assert calls == 3
    assert delays == [1, 2]


def test_retry_exhaustion_and_permanent_errors() -> None:
    delays: list[float] = []

    def transient() -> None:
        raise _AwsError("ServiceUnavailableException", 503)

    with pytest.raises(_AwsError):
        retry_aws_call(
            transient,
            operation="s3.put_object",
            policy=RetryPolicy(max_attempts=2, jitter=0),
            sleep=delays.append,
        )
    assert len(delays) == 1
    for code in ("AccessDeniedException", "UnrecognizedClientException", "ValidationException"):
        assert classify_aws_error(_AwsError(code)) == "permanent"
    assert classify_aws_error(ValueError("schema mismatch")) == "permanent"


def test_jitter_bounds_and_safe_structured_logging() -> None:
    events: list[dict] = []

    class _Logger:
        def warning(self, message, *args, **kwargs):
            events.append(kwargs["extra"])

    def transient() -> None:
        raise _AwsError("ThrottlingException", 429)

    with pytest.raises(_AwsError):
        retry_aws_call(
            transient,
            operation="cloudwatch.put_metric_data",
            policy=RetryPolicy(max_attempts=2, initial_delay=2, max_delay=10, jitter=0.25),
            sleep=lambda seconds: events.append({"delay": seconds}),
            random=lambda: 1.0,
            logger=_Logger(),
        )
    assert events[0] == {
        "operation": "cloudwatch.put_metric_data",
        "attempt": 1,
        "max_attempts": 2,
        "reason_category": "throttling",
        "next_delay": 2.5,
    }
    assert events[1]["delay"] == 2.5
    assert "do-not-log" not in str(events)


def test_symmetric_jitter_stays_within_configured_bounds() -> None:
    policy = RetryPolicy(initial_delay=2, max_delay=10, jitter=0.25)
    assert policy.delay(1, lambda: 0.0) == 1.5
    assert policy.delay(1, lambda: 1.0) == 2.5
    assert policy.delay(9, lambda: 1.0) == 10


@pytest.mark.parametrize("value", [0, 11])
def test_unbounded_attempt_count_rejected(value: int) -> None:
    with pytest.raises(ValueError, match="max_attempts"):
        RetryPolicy(max_attempts=value)


def test_nonfinite_delay_rejected() -> None:
    with pytest.raises(ValueError, match="finite"):
        RetryPolicy(initial_delay=float("inf"), max_delay=float("inf"))
