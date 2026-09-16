"""Bounded retries for repeatable AWS adapter calls, not ETL computation."""

from __future__ import annotations

import logging
import math
import random as random_module
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping, TypeVar

T = TypeVar("T")
_THROTTLING = frozenset(
    {
        "Throttling",
        "ThrottlingException",
        "TooManyRequestsException",
        "RequestLimitExceeded",
        "SlowDown",
        "ProvisionedThroughputExceededException",
    }
)
_TRANSIENT = frozenset(
    {
        "InternalError",
        "InternalFailure",
        "InternalServerError",
        "InternalServerException",
        "ServiceUnavailable",
        "ServiceUnavailableException",
        "RequestTimeout",
        "RequestTimeoutException",
    }
)
_PERMANENT = frozenset(
    {
        "AccessDenied",
        "AccessDeniedException",
        "InvalidAccessKeyId",
        "InvalidClientTokenId",
        "UnrecognizedClientException",
        "ExpiredToken",
        "ExpiredTokenException",
        "ValidationException",
        "InvalidParameterException",
        "InvalidParameterValueException",
        "MalformedQueryException",
    }
)
_NETWORK = frozenset(
    {
        "ConnectTimeoutError",
        "ReadTimeoutError",
        "ConnectionClosedError",
        "EndpointConnectionError",
        "ProxyConnectionError",
    }
)


@dataclass(frozen=True)
class RetryPolicy:
    """Maximum total calls and exponential delay with bounded fractional jitter."""

    max_attempts: int = 3
    initial_delay: float = 0.25
    max_delay: float = 2.0
    jitter: float = 0.2

    def __post_init__(self) -> None:
        if not 1 <= self.max_attempts <= 10:
            raise ValueError("retry.max_attempts must be between 1 and 10")
        if not all(
            math.isfinite(value) for value in (self.initial_delay, self.max_delay, self.jitter)
        ):
            raise ValueError("retry delays and jitter must be finite")
        if self.initial_delay < 0 or self.max_delay < self.initial_delay:
            raise ValueError("retry delays must be non-negative and max >= initial")
        if not 0 <= self.jitter <= 1:
            raise ValueError("retry.jitter must be between 0 and 1")

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> RetryPolicy:
        """Read shared AWS retry settings; invalid configuration is not retried."""
        raw = config.get("aws_retry", {})
        if not isinstance(raw, Mapping):
            raise ValueError("aws_retry configuration must be a mapping")
        return cls(
            max_attempts=int(raw.get("max_attempts", 3)),
            initial_delay=float(raw.get("initial_delay_seconds", 0.25)),
            max_delay=float(raw.get("max_delay_seconds", 2.0)),
            jitter=float(raw.get("jitter", 0.2)),
        )

    def delay(self, attempt: int, random: Callable[[], float]) -> float:
        """Return a capped exponential delay with symmetric fractional jitter."""
        base = min(self.max_delay, self.initial_delay * 2 ** (attempt - 1))
        return max(0.0, min(self.max_delay, base * (1 + self.jitter * (2 * random() - 1))))


def classify_aws_error(exc: Exception) -> str:
    """Classify only known transient AWS/network errors; everything else is permanent."""
    response = getattr(exc, "response", None)
    if isinstance(response, Mapping):
        error = response.get("Error", {})
        metadata = response.get("ResponseMetadata", {})
        code = str(error.get("Code", "")) if isinstance(error, Mapping) else ""
        status = metadata.get("HTTPStatusCode") if isinstance(metadata, Mapping) else None
        if code in _PERMANENT or status in {401, 403}:
            return "permanent"
        if code in _THROTTLING or status == 429:
            return "throttling"
        if code in _TRANSIENT or status in {500, 502, 503, 504}:
            return "service_unavailable"
        return "permanent"
    if isinstance(exc, (ConnectionError, TimeoutError)):
        return "network"
    if type(exc).__module__.startswith("botocore.") and type(exc).__name__ in _NETWORK:
        return "network"
    return "permanent"


def retry_aws_call(
    call: Callable[[], T],
    *,
    operation: str,
    policy: RetryPolicy | None = None,
    sleep: Callable[[float], None] = time.sleep,
    random: Callable[[], float] = random_module.random,
    logger: Any | None = None,
) -> T:
    """Retry one idempotent remote operation, preserving its final exception."""
    selected = policy or RetryPolicy()
    retry_logger = logger if logger is not None else logging.getLogger("transport_etl.aws_retry")
    for attempt in range(1, selected.max_attempts + 1):
        try:
            return call()
        except Exception as exc:
            category = classify_aws_error(exc)
            if category == "permanent" or attempt == selected.max_attempts:
                raise
            delay = selected.delay(attempt, random)
            fields = {
                "operation": operation,
                "attempt": attempt,
                "max_attempts": selected.max_attempts,
                "reason_category": category,
                "next_delay": delay,
            }
            retry_logger.warning(
                "AWS retry operation=%s attempt=%s max_attempts=%s "
                "reason_category=%s next_delay=%s",
                operation,
                attempt,
                selected.max_attempts,
                category,
                delay,
                extra=fields,
            )
            sleep(delay)
    raise AssertionError("unreachable")
