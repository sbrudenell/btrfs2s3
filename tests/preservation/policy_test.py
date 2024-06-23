from __future__ import annotations

from random import uniform
import sys
import time
from typing import cast

import arrow
from btrfs2s3._internal.arrowutil import convert_span
from btrfs2s3.preservation import Params
from btrfs2s3.preservation import Policy
from btrfs2s3.preservation import Timeframe
from btrfs2s3.preservation import TIMEFRAMES
import pytest

if sys.version_info >= (3, 9):  # pragma: >=3.9 cover
    from zoneinfo import ZoneInfo
else:  # pragma: <3.9 cover
    from backports.zoneinfo import ZoneInfo


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
    tzinfo = ZoneInfo("America/Los_Angeles")
    policy = Policy(params=Params(years=1), tzinfo=tzinfo)
    time_span = convert_span(arrow.get(tzinfo=tzinfo).span("year", bounds="[]"))

    assert time_span in policy.iter_time_spans(time.time())
    assert policy.should_preserve_for_time_span(time_span)
