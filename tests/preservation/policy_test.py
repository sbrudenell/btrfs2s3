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

from random import uniform
import time
from typing import cast
from zoneinfo import ZoneInfo

import arrow
import pytest

from btrfs2s3._internal.arrowutil import convert_span
from btrfs2s3._internal.cvar import TZINFO
from btrfs2s3._internal.cvar import use_tzinfo
from btrfs2s3._internal.preservation import Params
from btrfs2s3._internal.preservation import Policy
from btrfs2s3._internal.preservation import Timeframe
from btrfs2s3._internal.preservation import TIMEFRAMES


def _random_timestamp() -> float:
    return uniform(0.0, arrow.get("9999").timestamp())


@pytest.fixture(params=TIMEFRAMES)
def timeframe(request: pytest.FixtureRequest) -> Timeframe:
    return cast(Timeframe, request.param)


def test_empty_iter_time_spans(timeframe: Timeframe) -> None:
    policy = Policy(now=_random_timestamp())
    assert list(policy.iter_time_spans(_random_timestamp())) == []
    time_span = convert_span(
        arrow.get(_random_timestamp()).span(timeframe, bounds="[]")
    )
    assert not policy.should_preserve_for_time_span(time_span)


def test_empty_should_preserve_for_time_span(timeframe: Timeframe) -> None:
    policy = Policy(now=_random_timestamp())
    time_span = convert_span(
        arrow.get(_random_timestamp()).span(timeframe, bounds="[]")
    )
    assert not policy.should_preserve_for_time_span(time_span)


def test_all(timeframe: Timeframe) -> None:
    # This should be changed when we implement infinite preservation policies
    now = _random_timestamp()
    time_span = convert_span(arrow.get(now).span(timeframe, bounds="[]"))
    policy = Policy.all(now=now)
    assert time_span in list(policy.iter_time_spans(now))
    assert policy.should_preserve_for_time_span(time_span)


def test_some_time_frames() -> None:
    policy = Policy(params=Params(years=1))
    time_span = convert_span(arrow.get().span("year", bounds="[]"))

    start, end = time_span
    assert start < policy.now < end

    assert time_span in policy.iter_time_spans(time.time())
    assert policy.should_preserve_for_time_span(time_span)


def test_alternate_now() -> None:
    policy = Policy(now=0.0, params=Params(years=1))
    assert policy.now == 0.0
    time_span = convert_span(arrow.get(0).span("year", bounds="[]"))

    assert time_span in policy.iter_time_spans(0)
    assert policy.should_preserve_for_time_span(time_span)


def test_alternate_timezone() -> None:
    with use_tzinfo(ZoneInfo("America/Los_Angeles")):
        policy = Policy(params=Params(years=1))
        time_span = convert_span(
            arrow.get(tzinfo=TZINFO.get()).span("year", bounds="[]")
        )

        assert time_span in policy.iter_time_spans(time.time())
        assert policy.should_preserve_for_time_span(time_span)
