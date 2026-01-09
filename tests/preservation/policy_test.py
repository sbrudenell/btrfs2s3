# btrfs2s3 - maintains a tree of differential backups in object storage.
#
# Copyright (C) 2024 Steven Brudenell and other contributors.
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

from typing import cast
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import arrow
import pytest

from btrfs2s3._internal.arrowutil import convert_span
from btrfs2s3._internal.cvar import TZINFO
from btrfs2s3._internal.cvar import use_now
from btrfs2s3._internal.cvar import use_tzinfo
from btrfs2s3._internal.preservation import Params
from btrfs2s3._internal.preservation import Policy
from btrfs2s3._internal.preservation import TIMEFRAMES

if TYPE_CHECKING:
    from btrfs2s3._internal.preservation import Timeframe


@pytest.fixture(params=TIMEFRAMES)
def timeframe(request: pytest.FixtureRequest) -> Timeframe:
    return cast("Timeframe", request.param)


def test_empty_iter_time_spans() -> None:
    policy = Policy()
    assert list(policy.iter_time_spans(1234.0)) == []


def test_empty_should_preserve_for_time_span(timeframe: Timeframe) -> None:
    with use_now(1234.0):
        policy = Policy()
        time_span = convert_span(
            arrow.get(1234.0, tzinfo=TZINFO.get()).span(timeframe, bounds="[]")
        )
        assert not policy.should_preserve_for_time_span(time_span)


def test_all(timeframe: Timeframe) -> None:
    # This should be changed when we implement infinite preservation policies
    time_span = convert_span(
        arrow.get(1234.0, tzinfo=TZINFO.get()).span(timeframe, bounds="[]")
    )
    with use_now(1234.0):
        policy = Policy.all()
        assert time_span in list(policy.iter_time_spans(1234.0))
        assert policy.should_preserve_for_time_span(time_span)


def test_some_time_frames() -> None:
    time_span = convert_span(
        arrow.get(1234.0, tzinfo=TZINFO.get()).span("year", bounds="[]")
    )
    with use_now(1234.0):
        policy = Policy(params=Params(years=1))

        assert time_span in policy.iter_time_spans(1234.0)
        assert policy.should_preserve_for_time_span(time_span)


def test_alternate_timezone() -> None:
    with use_now(1234.0), use_tzinfo(ZoneInfo("America/Los_Angeles")):
        policy = Policy(params=Params(years=1))
        time_span = convert_span(
            arrow.get(1234.0, tzinfo=ZoneInfo("America/Los_Angeles")).span(
                "year", bounds="[]"
            )
        )

        assert time_span in policy.iter_time_spans(1234.0)
        assert policy.should_preserve_for_time_span(time_span)
