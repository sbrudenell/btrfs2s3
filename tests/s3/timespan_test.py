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
from typing import cast
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import arrow
import pytest

from btrfs2s3._internal.durations import Duration
from btrfs2s3._internal.s3 import apply_granularity
from btrfs2s3._internal.s3 import apply_min_duration
from btrfs2s3._internal.s3 import BillingPerTime
from btrfs2s3._internal.s3 import count_periods

if TYPE_CHECKING:
    from datetime import tzinfo


@pytest.fixture(
    params=[timezone.utc, ZoneInfo("America/Los_Angeles")],
    ids=["UTC", "America/Los_Angeles"],
)
def tz(request: pytest.FixtureRequest) -> tzinfo:
    return cast("tzinfo", request.param)


def _t(value: str, *, tz: tzinfo) -> float:
    return arrow.get(value, tzinfo=tz).timestamp()


def test_apply_granularity(tz: tzinfo) -> None:
    timespan = (_t("2006-01-01T00:12:00", tz=tz), _t("2006-01-01T00:30:00", tz=tz))
    timespan = apply_granularity(Duration(hours=1), timespan, tzinfo=tz)
    assert timespan == (
        _t("2006-01-01T00:00:00", tz=tz),
        _t("2006-01-01T01:00:00", tz=tz),
    )


def test_apply_granularity_edge_case(tz: tzinfo) -> None:
    timespan = (_t("2006-01-01T00:00:00", tz=tz), _t("2006-02-01T00:00:00", tz=tz))
    timespan = apply_granularity(Duration(hours=1), timespan, tzinfo=tz)
    assert timespan == (
        _t("2006-01-01T00:00:00", tz=tz),
        _t("2006-02-01T00:00:00", tz=tz),
    )


def test_apply_min_duration(tz: tzinfo) -> None:
    timespan = (_t("2006-01-01T00:12:00", tz=tz), _t("2006-01-01T00:30:00", tz=tz))
    timespan = apply_min_duration(Duration(days=1), timespan, tzinfo=tz)
    assert timespan == (
        _t("2006-01-01T00:12:00", tz=tz),
        _t("2006-01-02T00:12:00", tz=tz),
    )


def test_count_periods(tz: tzinfo) -> None:
    timespan = (_t("2006-01-05", tz=tz), _t("2006-03-05", tz=tz))
    periods = count_periods(Duration(months=1), timespan, tzinfo=tz)
    assert periods == 2.0

    timespan = (_t("2006-01-21", tz=tz), _t("2006-03-09", tz=tz))
    periods = count_periods(Duration(months=1), timespan, tzinfo=tz)
    assert periods == (31 - 20) / 31 + 1 + 8 / 31

    timespan = (_t("2006-01-21", tz=tz), _t("2006-03-09", tz=tz))
    periods = count_periods(Duration(days=30), timespan, tzinfo=tz)
    assert periods == ((31 - 20) + 28 + 8) / 30


@pytest.mark.parametrize(
    ("minimum", "granularity", "timespan", "expected"),
    [
        (
            None,
            None,
            ("2006-01-01T00:12:00", "2006-01-01T00:30:00"),
            ("2006-01-01T00:12:00", "2006-01-01T00:30:00"),
        ),
        (
            Duration(days=1),
            None,
            ("2006-01-01T00:12:00", "2006-01-01T00:30:00"),
            ("2006-01-01T00:12:00", "2006-01-02T00:12:00"),
        ),
        (
            None,
            Duration(hours=1),
            ("2006-01-01T00:12:00", "2006-01-01T00:30:00"),
            ("2006-01-01T00:00:00", "2006-01-01T01:00:00"),
        ),
        (
            Duration(days=1),
            Duration(hours=1),
            ("2006-01-01T00:12:00", "2006-01-01T00:30:00"),
            ("2006-01-01T00:00:00", "2006-01-02T00:00:00"),
        ),
    ],
)
def test_billable_timespan(
    tz: tzinfo,
    minimum: Duration | None,
    granularity: Duration | None,
    timespan: tuple[str, str],
    expected: tuple[str, str],
) -> None:
    bpt = BillingPerTime(
        period=Duration(months=1), tzinfo=tz, minimum=minimum, granularity=granularity
    )
    timespan_floats = (_t(timespan[0], tz=tz), _t(timespan[1], tz=tz))
    expected_floats = (_t(expected[0], tz=tz), _t(expected[1], tz=tz))
    assert bpt.get_billable_timespan(timespan_floats) == expected_floats
