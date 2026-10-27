"""Tests for the @timed decorator that feeds pulseops_mongodb_operation_seconds."""

import asyncio

import pytest
from pymongo.errors import ServerSelectionTimeoutError

from pulseops import metrics
from pulseops.storage import timed


def observations(operation: str) -> float:
    return metrics.MONGODB_OPERATION.labels(operation)._sum.get()


def test_the_duration_of_an_operation_is_observed_and_its_result_returned():
    class Store:
        @timed("test_operation_ok")
        async def read(self, value):
            await asyncio.sleep(0.02)
            return value

    assert asyncio.run(Store().read(42)) == 42
    assert observations("test_operation_ok") >= 0.02


def test_a_failed_operation_is_observed_too_and_its_error_still_raised():
    class Store:
        @timed("test_operation_failing")
        async def write(self):
            await asyncio.sleep(0.02)
            raise ServerSelectionTimeoutError("MongoDB lost")

    with pytest.raises(ServerSelectionTimeoutError):
        asyncio.run(Store().write())
    assert observations("test_operation_failing") >= 0.02


def test_the_wrapped_method_keeps_its_name_and_docstring():
    class Store:
        @timed("test_operation_name")
        async def read(self):
            """Reads."""

    assert Store.read.__name__ == "read"
    assert Store.read.__doc__ == "Reads."
