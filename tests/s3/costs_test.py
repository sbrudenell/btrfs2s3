from __future__ import annotations

from datetime import timezone

import arrow
import pytest

from btrfs2s3._internal.s3 import CostPerByteAndTime
from btrfs2s3._internal.s3 import Costs
from btrfs2s3._internal.s3 import StorageClassCost
from btrfs2s3._internal.s3 import Timespan
from tests.cost_fixtures import AWS_COSTS
from tests.cost_fixtures import STORAGE_COST_DEEP_ARCHIVE
from tests.cost_fixtures import STORAGE_COST_RATE_STANDARD
from tests.cost_fixtures import STORAGE_COST_STANDARD


@pytest.mark.parametrize(
    ("costs", "size", "start", "end", "expected_cost"),
    [
        (STORAGE_COST_RATE_STANDARD, 10**9, "2006-01-01", "2006-02-01", 0.023),
        (STORAGE_COST_RATE_STANDARD, 2 * 10**9, "2006-01-01", "2006-02-01", 0.023 * 2),
        (STORAGE_COST_RATE_STANDARD, 10**9, "2006-04-01", "2006-04-16", 0.023 / 2),
    ],
)
def test_cost_per_byte_and_time(
    costs: CostPerByteAndTime, size: int, start: str, end: str, expected_cost: float
) -> None:
    assert (
        costs.get(
            size=size, timespan=Timespan(start=arrow.get(start), end=arrow.get(end))
        )
        == expected_cost
    )


@pytest.mark.parametrize(
    ("costs", "size", "start", "end", "expected_cost"),
    [
        (STORAGE_COST_STANDARD, 10**9, "2006-01-01", "2006-02-01", 0.023),
        (
            STORAGE_COST_DEEP_ARCHIVE,
            10**9,
            "2006-01-01",
            "2006-02-01",
            0.00099 * (5 + 29 / 30),
        ),
        (STORAGE_COST_DEEP_ARCHIVE, 10**9, "2006-01-01", "2007-01-01", 0.00099 * 12),
    ],
)
def test_storage_class_get_storage_cost(
    costs: StorageClassCost, size: int, start: str, end: str, expected_cost: float
) -> None:
    assert (
        costs.get_storage_cost(
            size=size, timespan=Timespan(start=arrow.get(start), end=arrow.get(end))
        )
        == expected_cost
    )


def test_properties() -> None:
    assert AWS_COSTS.tzinfo == timezone.utc
    assert AWS_COSTS.billing_period == "month"
    assert AWS_COSTS.storage_time_granularity == "hour"
    assert list(AWS_COSTS.storage_classes()) == ["STANDARD", "DEEP_ARCHIVE"]


@pytest.mark.parametrize(
    ("costs", "storage_class", "size", "start", "end", "expected_cost"),
    [
        (AWS_COSTS, "STANDARD", 10**9, "2006-01-01", "2006-02-01", 0.023),
        (
            AWS_COSTS,
            "DEEP_ARCHIVE",
            10**9,
            "2006-01-01",
            "2006-02-01",
            0.00099 * (5 + 29 / 30),
        ),
        (AWS_COSTS, "DEEP_ARCHIVE", 10**9, "2006-01-01", "2007-01-01", 0.00099 * 12),
        (AWS_COSTS, "UNKNOWN", 10**9, "2006-01-01", "2006-02-01", None),
    ],
)
def test_s3_get_storage_cost(
    costs: Costs,
    storage_class: str,
    size: int,
    start: str,
    end: str,
    expected_cost: float | None,
) -> None:
    assert (
        costs.get_storage_cost(
            size=size,
            timespan=Timespan(start=arrow.get(start), end=arrow.get(end)),
            storage_class=storage_class,
        )
    ) == expected_cost
