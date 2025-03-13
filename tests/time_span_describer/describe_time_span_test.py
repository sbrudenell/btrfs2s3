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

from zoneinfo import ZoneInfo

import arrow
import pytest
from rich.text import Span

from btrfs2s3._internal.cvar import TZINFO
from btrfs2s3._internal.cvar import use_tzinfo
from btrfs2s3._internal.time_span_describer import describe_time_span


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
    with use_tzinfo(ZoneInfo(tzname)):
        a = arrow.get(a_str, tzinfo=TZINFO.get())
        b = arrow.get(b_str, tzinfo=TZINFO.get())
        got = describe_time_span((a.timestamp(), b.timestamp()), bounds="[]")
    assert got.plain == expected_plain
    # The rich-provided highlighter returns loads of spans like iso8601.year
    # which aren't used by our theme. Testing all spans is overfitting. Just
    # test the spans that are used by our theme
    assert set(got.spans) >= expected_spans
