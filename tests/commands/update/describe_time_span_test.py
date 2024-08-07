from __future__ import annotations

import arrow
from btrfs2s3.commands.update import describe_time_span
from btrfs2s3.zoneinfo import get_zoneinfo
import pytest
from rich.text import Span


@pytest.mark.parametrize(
    ("a_str", "b_str", "expected_plain", "expected_spans", "tzname"),
    [
        (a_str, b_str, expected_plain, expected_spans, tzname)
        for tzname in ("UTC", "America/Los_Angeles")
        for a_str, b_str, expected_plain, expected_spans in [
            ("2006", "2007", "2006 yearly", {Span(0, 4, "iso8601.date")}),
            ("2006", "2006-04-01", "2006-Q1 quarterly", {Span(0, 7, "iso8601.date")}),
            ("2006", "2006-02-01", "2006-01 monthly", {Span(0, 7, "iso8601.date")}),
            (
                "2006-01-02",
                "2006-01-09",
                "2006-W01 weekly",
                {Span(0, 8, "iso8601.date")},
            ),
            (
                "2006-01-02",
                "2006-01-03",
                "2006-01-02 daily",
                {Span(0, 10, "iso8601.date")},
            ),
            (
                "2006-01-02 15",
                "2006-01-02 16",
                "2006-01-02T15 hourly",
                {Span(0, 10, "iso8601.date"), Span(11, 13, "iso8601.time")},
            ),
            (
                "2006-01-02 15:04",
                "2006-01-02 15:05",
                "2006-01-02T15:04 minutely",
                {Span(0, 10, "iso8601.date"), Span(11, 16, "iso8601.time")},
            ),
            (
                "2006-01-02 15:04:05",
                "2006-01-02 15:04:06",
                "2006-01-02T15:04:05 secondly",
                {Span(0, 10, "iso8601.date"), Span(11, 19, "iso8601.time")},
            ),
            (
                "2006",
                "2008",
                "2006-01-01T00:00:00/2008-01-01T00:00:00",
                {
                    Span(0, 10, "iso8601.date"),
                    Span(11, 19, "iso8601.time"),
                    Span(20, 30, "iso8601.date"),
                    Span(31, 39, "iso8601.time"),
                },
            ),
        ]
    ],
)
def test_describe_time_span(
    a_str: str, b_str: str, expected_plain: str, expected_spans: set[Span], tzname: str
) -> None:
    tzinfo = get_zoneinfo(tzname)
    a = arrow.get(a_str, tzinfo=tzinfo)
    b = arrow.get(b_str, tzinfo=tzinfo)
    got = describe_time_span((a.timestamp(), b.timestamp()), tzinfo, bounds="[]")
    assert got.plain == expected_plain
    # The rich-provided highlighter returns loads of spans like iso8601.year
    # which aren't used by our theme. Testing all spans is overfitting. Just
    # test the spans that are used by our theme
    assert set(got.spans) >= expected_spans
