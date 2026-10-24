"""Pydantic schemas for test-result and heartbeat messages."""

import json
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints, ValidationError

# Measurement names become a metric label, so keep them short, simple and few.
MeasurementName = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]{0,40}$")]


class TestResult(BaseModel):
    __test__ = False  # not a pytest test class
    model_config = ConfigDict(frozen=True)

    schema_version: Literal[1]
    event_id: UUID
    machine_id: str
    product_id: Annotated[str, StringConstraints(min_length=1, max_length=64)]
    result: Literal["PASS", "FAIL"]
    timestamp: AwareDatetime
    measurements: Annotated[dict[MeasurementName, float], Field(max_length=10)] = {}


class Heartbeat(BaseModel):
    model_config = ConfigDict(frozen=True)

    schema_version: Literal[1]
    machine_id: str
    timestamp: AwareDatetime


class Rejected(Exception):
    """A message that can't be processed. `reason` is one of a fixed set (it's a metric label)."""

    def __init__(self, reason: str, detail: str):
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


SUPPORTED_SCHEMA_VERSIONS = (1,)


def parse(model: type[TestResult] | type[Heartbeat], raw: bytes | None, line: tuple[str, ...]) -> TestResult | Heartbeat:
    """Validate a Kafka message value, or raise Rejected with one of the reasons below."""
    try:
        data = json.loads(raw or b"")
    except ValueError as e:
        raise Rejected("invalid_json", str(e)) from None
    if isinstance(data, dict) and "schema_version" in data and data["schema_version"] not in SUPPORTED_SCHEMA_VERSIONS:
        raise Rejected("unsupported_version", f"schema_version {data['schema_version']!r}")
    try:
        message = model.model_validate(data)
    except ValidationError as e:
        detail = "; ".join(f"{'.'.join(map(str, err['loc'])) or 'message'}: {err['msg']}" for err in e.errors(include_url=False))
        raise Rejected("invalid_schema", detail) from None
    if message.machine_id not in line:
        raise Rejected("unknown_machine", f"{message.machine_id!r} is not in LINE_SEQUENCE")
    return message
