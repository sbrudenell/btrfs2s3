# btrfs2s3 - maintains a tree of differential backups in object storage.
#
# Copyright (C) 2025 Steven Brudenell and other contributors.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

from datetime import timezone

import arrow
import pytest

from btrfs2s3._internal.durations import Duration
from btrfs2s3._internal.s3 import CostPerByteAndTime
from btrfs2s3._internal.s3 import Costs
from btrfs2s3._internal.s3 import StorageClassCost
from btrfs2s3._internal.s3 import Timespan
from tests.conftest import AWS_COSTS
from tests.conftest import AWS_STORAGE_COST_DEEP_ARCHIVE
from tests.conftest import AWS_STORAGE_COST_RATE_STANDARD
from tests.conftest import AWS_STORAGE_COST_STANDARD
from tests.conftest import B2_COSTS
from tests.conftest import B2_STORAGE_COST_RATE_STANDARD
from tests.conftest import B2_STORAGE_COST_STANDARD


@pytest.mark.parametrize(
    ("costs", "size", "start", "end", "expected_cost"),
    [
        (AWS_STORAGE_COST_RATE_STANDARD, 10**9, "2006-01-01", "2006-02-01", 0.023),
        (
            AWS_STORAGE_COST_RATE_STANDARD,
            2 * 10**9,
            "2006-01-01",
            "2006-02-01",
            0.023 * 2,
        ),
        (AWS_STORAGE_COST_RATE_STANDARD, 10**9, "2006-04-01", "2006-04-16", 0.023 / 2),
        (
            B2_STORAGE_COST_RATE_STANDARD,
            10**9,
            "2006-01-01",
            "2006-02-01",
            0.006 / 30 * 31,
        ),
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
        (AWS_STORAGE_COST_STANDARD, 10**9, "2006-01-01", "2006-02-01", 0.023),
        (
            AWS_STORAGE_COST_DEEP_ARCHIVE,
            10**9,
            "2006-01-01",
            "2006-02-01",
            0.00099 * (5 + 29 / 30),
        ),
        (
            AWS_STORAGE_COST_DEEP_ARCHIVE,
            10**9,
            "2006-01-01",
            "2007-01-01",
            0.00099 * 12,
        ),
        (B2_STORAGE_COST_STANDARD, 10**9, "2006-01-01", "2006-02-01", 0.006 / 30 * 31),
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
    assert AWS_COSTS.billing_period == Duration(months=1)
    assert AWS_COSTS.storage_time_granularity == Duration(hours=1)
    assert list(AWS_COSTS.storage_classes()) == ["STANDARD", "DEEP_ARCHIVE"]

    assert B2_COSTS.tzinfo == timezone.utc
    assert B2_COSTS.billing_period == Duration(months=1)
    assert B2_COSTS.storage_time_granularity == Duration(hours=1)
    assert list(B2_COSTS.storage_classes()) == ["STANDARD"]


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
        (B2_COSTS, "STANDARD", 10**9, "2006-01-01", "2006-02-01", 0.006 / 30 * 31),
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
