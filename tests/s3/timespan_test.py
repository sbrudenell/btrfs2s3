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

import arrow

from btrfs2s3._internal.durations import Duration
from btrfs2s3._internal.s3 import apply_granularity
from btrfs2s3._internal.s3 import apply_min_duration
from btrfs2s3._internal.s3 import count_periods
from btrfs2s3._internal.s3 import Timespan


def test_timespan_length() -> None:
    timespan = Timespan(
        start=arrow.get("2006-01-01T00:00:00"), end=arrow.get("2006-01-01T01:00:00")
    )
    assert timespan.length() == 3600


def test_apply_granularity() -> None:
    timespan = Timespan(
        start=arrow.get("2006-01-01T00:12:00"), end=arrow.get("2006-01-01T00:30:00")
    )
    timespan = apply_granularity(Duration(hours=1), timespan)
    assert timespan == Timespan(
        start=arrow.get("2006-01-01T00:00:00"), end=arrow.get("2006-01-01T01:00:00")
    )


def test_apply_granularity_edge_case() -> None:
    timespan = Timespan(
        start=arrow.get("2006-01-01T00:00:00"), end=arrow.get("2006-02-01T00:00:00")
    )
    timespan = apply_granularity(Duration(hours=1), timespan)
    assert timespan == Timespan(
        start=arrow.get("2006-01-01T00:00:00"), end=arrow.get("2006-02-01T00:00:00")
    )


def test_apply_min_duration() -> None:
    timespan = Timespan(
        start=arrow.get("2006-01-01T00:12:00"), end=arrow.get("2006-01-01T00:30:00")
    )
    timespan = apply_min_duration(Duration(days=1), timespan)
    assert timespan == Timespan(
        start=arrow.get("2006-01-01T00:12:00"), end=arrow.get("2006-01-02T00:12:00")
    )


def test_count_periods() -> None:
    timespan = Timespan(start=arrow.get("2006-01-05"), end=arrow.get("2006-03-05"))
    periods = count_periods(Duration(months=1), timespan)
    assert periods == 2.0

    timespan = Timespan(start=arrow.get("2006-01-21"), end=arrow.get("2006-03-09"))
    periods = count_periods(Duration(months=1), timespan)
    assert periods == (31 - 20) / 31 + 1 + 8 / 31

    timespan = Timespan(start=arrow.get("2006-01-21"), end=arrow.get("2006-03-09"))
    periods = count_periods(Duration(days=30), timespan)
    assert periods == ((31 - 20) + 28 + 8) / 30
