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

"""Utilites for manipulating Arrow datetime objects.

The third-party Arrow project implements a feature-rich replacement for the
builtin date and time types. The btrfs2s3 CLI uses this for creating
human-friendly backup schedules.
"""

from __future__ import annotations

from typing import Literal
from typing import TYPE_CHECKING

from typing_extensions import TypeAlias

if TYPE_CHECKING:
    from collections.abc import Iterable

    from arrow import Arrow

    _Bounds: TypeAlias = Literal["[)", "()", "(]", "[]"]


def iter_intersecting_time_spans(
    a: Arrow, *, bounds: _Bounds = "[)"
) -> Iterable[tuple[Arrow, Arrow]]:
    """Convenience function to generate consistent intersecting time spans.

    This does the same thing as iter_time_spans, but will generate all
    human-friendly time spans of all supported timeframes which contain the
    given point in time.

    Args:
        a: A given point in time.
        bounds: Whether the generated time spans should contain their
            endpoints. See the Arrow documentation for more.

    Yields:
        2-tuples of Arrow objects.
    """
    return iter_time_spans(
        a,
        years=(0,),
        quarters=(0,),
        months=(0,),
        weeks=(0,),
        days=(0,),
        hours=(0,),
        minutes=(0,),
        seconds=(0,),
        bounds=bounds,
    )


def iter_time_spans(
    a: Arrow,
    *,
    bounds: _Bounds = "[)",
    years: Iterable[int] = (),
    quarters: Iterable[int] = (),
    months: Iterable[int] = (),
    weeks: Iterable[int] = (),
    days: Iterable[int] = (),
    hours: Iterable[int] = (),
    minutes: Iterable[int] = (),
    seconds: Iterable[int] = (),
) -> Iterable[tuple[Arrow, Arrow]]:
    """Convenience function to generate consistent time spans of multiple timeframes.

    In the Arrow parlance, a time span is a human-friendly interval (e.g. "the
    year 2006") and a timeframe is the type of such an interval (e.g. a year
    versus a week).

    This function generates time spans starting at a given origin time, offset
    by given amounts. For instance,
    iter_time_spans(arrow.get("2006"), years=[-1, 0, 1]) will
    generate three 1-year time spans: the year 2005, the year 2006, and the
    year 2007. The "bounds" specification will be applied consistently to all
    returned time spans.

    Args:
        a: The origin time.
        bounds: Whether the generated time spans should contain their
            endpoints. See the Arrow documentation for more.
        years: An Iterable of offsets of year time spans.
        quarters: An Iterable of offsets of quarter time spans.
        months: An Iterable of offsets of month time spans.
        weeks: An Iterable of offsets of week time spans.
        days: An Iterable of offsets of day time spans.
        hours: An Iterable of offsets of hour time spans.
        minutes: An Iterable of offsets of minute time spans.
        seconds: An Iterable of offsets of second time spans.

    Yields:
        2-tuples of Arrow objects.
    """
    for y in years:
        yield a.shift(years=y).span("year", bounds=bounds)
    for q in quarters:
        yield a.shift(quarters=q).span("quarter", bounds=bounds)
    for m in months:
        yield a.shift(months=m).span("month", bounds=bounds)
    for w in weeks:
        yield a.shift(weeks=w).span("week", bounds=bounds)
    for d in days:
        yield a.shift(days=d).span("day", bounds=bounds)
    for h in hours:
        yield a.shift(hours=h).span("hour", bounds=bounds)
    for m in minutes:
        yield a.shift(minutes=m).span("minute", bounds=bounds)
    for s in seconds:
        yield a.shift(seconds=s).span("second", bounds=bounds)


def convert_span(time_span: tuple[Arrow, Arrow]) -> tuple[float, float]:
    start, end = time_span
    return start.timestamp(), end.timestamp()


def humanize_span(time_span: tuple[Arrow, Arrow], bounds: _Bounds = "[)") -> str:
    a, b = time_span
    if (a, b) == a.span("year", bounds=bounds):
        return a.format("YYYY") + "/P1Y"
    if (a, b) == a.span("quarter", bounds=bounds):
        return f"{a.year:04d}-Q{a.quarter}/P1Q"
    if (a, b) == a.span("month", bounds=bounds):
        return a.format("YYYY-MM") + "/P1M"
    if (a, b) == a.span("week", bounds=bounds):
        return f"{a.year:04d}-W{a.week:02d}/P1W"
    if (a, b) == a.span("day", bounds=bounds):
        return a.format("YYYY-MM-DD") + "/P1D"
    if (a, b) == a.span("hour", bounds=bounds):
        return a.format("YYYY-MM-DDTHH") + "/PT1H"
    if (a, b) == a.span("minute", bounds=bounds):
        return a.format("YYYY-MM-DDTHH:mm") + "/PT1M"
    if (a, b) == a.span("second", bounds=bounds):
        return a.format("YYYY-MM-DDTHH:mm:ss") + "/PT1S"
    return a.format("YYYY-MM-DDTHH:mm:ss") + "/" + b.format("YYYY-MM-DDTHH:mm:ss")
