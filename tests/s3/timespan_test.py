from __future__ import annotations

import arrow

from btrfs2s3._internal.durations import Duration
from btrfs2s3._internal.s3 import apply_granularity
from btrfs2s3._internal.s3 import apply_min_duration
from btrfs2s3._internal.s3 import count_timeframes
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
    timespan = apply_granularity("hour", timespan)
    assert timespan == Timespan(
        start=arrow.get("2006-01-01T00:00:00"), end=arrow.get("2006-01-01T01:00:00")
    )


def test_apply_granularity_edge_case() -> None:
    timespan = Timespan(
        start=arrow.get("2006-01-01T00:00:00"), end=arrow.get("2006-02-01T00:00:00")
    )
    timespan = apply_granularity("hour", timespan)
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


def test_count_timeframes() -> None:
    timespan = Timespan(start=arrow.get("2006-01-05"), end=arrow.get("2006-03-05"))
    timeframes = count_timeframes("month", timespan)
    assert timeframes == 2.0

    timespan = Timespan(start=arrow.get("2006-01-21"), end=arrow.get("2006-03-09"))
    timeframes = count_timeframes("month", timespan)
    assert timeframes == (31 - 20) / 31 + 1 + 8 / 31
