"""Utilites for manipulating Arrow datetime objects.

The third-party Arrow project implements a feature-rich replacement for the
builtin date and time types. The btrfs2s3 CLI uses this for creating
human-friendly backup schedules.
"""

from __future__ import annotations

from typing import Iterable
from typing import Literal
from typing import TYPE_CHECKING
from typing import TypeAlias

if TYPE_CHECKING:
    from arrow import Arrow

    _Bounds: TypeAlias = Literal["[)", "()", "(]", "[]"]


def iter_intersecting_time_spans(
    a: Arrow,
    *,
    bounds: _Bounds = "[)",
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
        microseconds=(0,),
        bounds=bounds,
    )


def iter_time_spans(  # noqa: PLR0913
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
    microseconds: Iterable[int] = (),
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
        microseconds: An Iterable of offsets of microsecond time spans.

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
    for m in microseconds:
        yield a.shift(microseconds=m).span("microsecond", bounds=bounds)
