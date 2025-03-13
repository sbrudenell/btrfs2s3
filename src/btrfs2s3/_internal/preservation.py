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

"""Code related to schedule-based preservation of snapshots of data.

The design of btrfs2s3's preservation schedule is based on the "retention
policies" of
btrbk. See https://digint.ch/btrbk/doc/btrbk.conf.5.html#_retention_policy for
details.

Broadly, we define a set of time spans, and for each one we preserve the first
snapshot that was created in that time span.

The first snapshot of a year is considered the yearly snapshot. The first
snapshot of a month is the monthly snapshot. We say that a
snapshot is "nominal" for the year and/or month, respectively.

btrfs2s3 supports preserving yearly, quarterly, monthly, weekly,
daily, hourly, "minutely" and "secondly" snapshots.

btrfs2s3 only supports time spans (years, months, days, etc) that are
"grid-aligned". That is, days last from midnight to midnight, never from noon
to noon.

In btrfs2s3, the preservation schedule is significant when choosing parents
for incremental backups. If a policy keeps one monthly and one yearly
backup, then we will store the yearly backup as a full backup. We will store the
monthly backup as an incremental
backup, having the yearly full backup as the parent.

The time zone is significant when defining a preservation schedule (for example,
"midnight" corresponds to a different unix timestamp depending on the time
zone). A change to the time zone of a preservation schedule can completely change
the nominality of snapshots, which could lead to undesired deletion of data,
longer-than-expected incremental backup chains, or heavily-duplicated full
backups.

To avoid this chaos, we never honor the system locales or time zones. We require
time  zones to be explicitly specified, defaulting to UTC.

By convention, time spans are represented as
tuples of left-closed right-open intervals (that is, the same convention used by
python slices). For example, the year 2006 in UTC is represented as (1136073600.0,
1167609600.0), even though it does NOT include the upper boundary timestamp of
1167609600.0.
"""

from __future__ import annotations

import dataclasses
import re
from typing import Generic
from typing import Literal
from typing import TYPE_CHECKING
from typing import TypeVar

import arrow
from typing_extensions import Self
from typing_extensions import TypeAlias
from typing_extensions import TypedDict

from btrfs2s3._internal.arrowutil import convert_span
from btrfs2s3._internal.arrowutil import iter_time_spans
from btrfs2s3._internal.cvar import TZINFO

if TYPE_CHECKING:
    from collections.abc import Iterator

TS: TypeAlias = tuple[float, float]
"""An alias for tuple[float, float] which is used as a time span type.

This is just provided because it's frequently used, and shorter than
tuple[float, float].
"""

Timeframe: TypeAlias = Literal[
    "seconds", "minutes", "hours", "days", "weeks", "months", "quarters", "years"
]
"""A type alias for the valid arguments to Params."""
TIMEFRAMES: frozenset[Timeframe] = frozenset(
    ("seconds", "minutes", "hours", "days", "weeks", "months", "quarters", "years")
)
"""A list of valid arguments to Params."""

_T = TypeVar("_T")


class TimeframeArgs(TypedDict, Generic[_T], total=False):
    """A TypedDict for the valid arguments to Params."""

    years: _T
    quarters: _T
    months: _T
    weeks: _T
    days: _T
    hours: _T
    minutes: _T
    seconds: _T


_REGEX = re.compile(
    r"^(\b(?P<years>\d+)y\b)? ??"
    r"(\b(?P<quarters>\d+)q\b)? ??"
    r"(\b(?P<months>\d+)m\b)? ??"
    r"(\b(?P<weeks>\d+)w\b)? ??"
    r"(\b(?P<days>\d+)d\b)? ??"
    r"(\b(?P<hours>\d+)h\b)? ??"
    r"(\b(?P<minutes>\d+)M\b)? ??"
    r"(\b(?P<seconds>\d+)s\b)? ??$"
)


@dataclasses.dataclass
class Params:
    """Parameters which define a schedule of preserving snapshots or backups.

    These parameters have the same significance as in btrbk's retention
    policies. See
    https://digint.ch/btrbk/doc/btrbk.conf.5.html#_retention_policy for more.

    If any of the attributes is zero, then snapshots/backups will not be
    preserved on that interval. None of the attributes may be negative.

    Attributes:
        years: How many years back yearly backups should be preserved. The
            first backup in a year is the yearly backup.
        quarters: How many quarters back quarterly backups should be preserved.
            The first backup in a quarter is the quarterly backup.
        months: How many months back monthly backups should be preserved. The
            first backup in a month is the monthly backup.
        weeks: How many weeks back weekly backups should be preserved. The
            first backup in a week is the weekly backup.
        days: How many days back daily backups should be preserved. The first
            backup in a day is the daily backup.
        hours: How many hours back hourly backups should be preserved. The
            first backup in an hour is the hourly backup.
        minutes: How many minutes back "minutely" backups should be preserved.
            The first backup in a minute is the minutely backup.
        seconds: How many seconds back "secondly" backups should be preserved.
            The first backup in a second is the secondly backup.
    """

    years: int = 0
    quarters: int = 0
    months: int = 0
    weeks: int = 0
    days: int = 0
    hours: int = 0
    minutes: int = 0
    seconds: int = 0

    def __post_init__(self) -> None:
        """Do validation checks."""
        for arg in TIMEFRAMES:
            if getattr(self, arg) < 0:
                msg = f"parameter can't be negative: {arg}={getattr(self, arg)}"
                raise ValueError(msg)

    @classmethod
    def all(cls) -> Self:
        """Returns a Params which preserves ALL snapshots or backups.

        This is mainly useful for testing.

        It's also not currently correct, until we implement infinite
        preservation.
        """
        kwargs = {t: 1 for t in TIMEFRAMES}
        # https://github.com/python/mypy/issues/10023
        return cls(**kwargs)  # type: ignore[misc]

    @classmethod
    def parse(cls, desc: str) -> Self:
        """Parse a Params from a human-friendly description string.

        The format for the description string is based on the one used by
        btrbk. See
        https://digint.ch/btrbk/doc/btrbk.conf.5.html#_retention_policy

        The format is:

            [<yearly>y] [<quarterly>q] [<monthly>m] [<weekly>w] [<daily>d]
            [<hourly>h] [<minutely>M] [<secondly>s]

        Args:
            desc: The description string.
        """
        m = _REGEX.match(desc)
        if not m:
            msg = f"invalid preservation params: {desc}"
            raise ValueError(msg)
        kwargs = TimeframeArgs[int]()
        for timeframe in TIMEFRAMES:
            value = m.group(timeframe)
            if value is not None:
                kwargs[timeframe] = int(value)
        return cls(**kwargs)


class Policy:
    """A schedule-based policy for preserving snapshots.

    A Policy is a helper class which defines a schedule for preserving
    snapshots (or backups) of data.

    The preservation schedule is defined by Params, which are only meaningful
    relative to the "current" time. For consistency, a Policy object fixes the
    current time when it is created (the value can be supplied in the
    constructor). The return value of should_preserve_for_time_span() is considered
    relative to this creation time, not the time the function is called.
    """

    @classmethod
    def all(cls, *, now: float | None = None) -> Self:
        """Returns a Policy which preserves ALL snapshots or backups.

        This is mainly useful for testing (and not currently correct, until we
        implement infinite preservation).
        """
        return cls(params=Params.all(), now=now)

    def __init__(
        self, *, params: Params | None = None, now: float | None = None
    ) -> None:
        """Creates a Policy.

        When no args are supplied, the resulting Policy will not
        preserve anything. This can be useful to bypass schedule-based
        preservation logic.

        Args:
            params: The parameters which define the schedule of time spans for
                which backups should be preserved.
            now: A timestamp to use as the current time. If None, defaults to
                the actual current time. This is mainly useful for testing.
        """
        if params is None:
            params = Params()
        if now is None:
            now = arrow.get(tzinfo=TZINFO.get()).timestamp()

        self._params = params
        self._now = now

        kwargs = {t: range(0, -getattr(params, t), -1) for t in TIMEFRAMES}
        # https://github.com/python/mypy/issues/10023
        self._preserve_for_time_spans = {
            convert_span(span)
            for span in iter_time_spans(
                arrow.get(now, tzinfo=TZINFO.get()),
                bounds="[]",
                **kwargs,  # type: ignore[misc]
            )
        }

    @property
    def now(self) -> float:
        """Returns the current time used by this Policy."""
        return self._now

    def iter_time_spans(self, timestamp: float) -> Iterator[TS]:
        """Yields time spans which overlap with a given timestamp.

        This can be used to determine which time spans a snapshot falls into. A
        caller can then call should_preserve_for_time_span() to figure out whether the
        snapshot should be retained.

        Note that this only yields time spans which overlap the argument
        timestamp AND may EVER be relevant to this policy (otherwise, this could
        just be a top-level function). If this policy only preserves monthly backups,
        this function will only yield monthly time spans, and never yearly
        ones. This is significant for the function of btrfs2s3.

        btrfs2s3 (in resolver.py) will store backups as full or incremental
        based primarily on this function. If a backup is nominal for the FIRST
        time span returned by this function, it will be stored as a full
        backup. Otherwise, it will be stored as an incremental backup, and its
        parent will be the nominal backup of the next-earliest time span
        returned by this function for which the backup in question is NOT
        nominal. For example, if a Policy calls for yearly and monthly
        backups, then the yearly backups will be full backups (because the
        yearly time spans are returned first by iter_time_spans()) and the
        monthly backups will be incremental backups with a yearly backup as
        their parent (the monthly time spans are returned second by this
        function, so btrfs2s3 will look at yearly intervals which are returned
        earlier, and find the nominal backup for that interval to use as the
        parent).

        Args:
            timestamp: A timestamp to query.

        Yields:
            (timestamp, timestamp) time span tuples, in descending order of
                length.
        """
        kwargs = {t: (0,) if getattr(self._params, t) else () for t in TIMEFRAMES}
        # https://github.com/python/mypy/issues/10023
        for span in iter_time_spans(
            arrow.get(timestamp, tzinfo=TZINFO.get()),
            bounds="[]",
            **kwargs,  # type: ignore[misc]
        ):
            yield convert_span(span)

    def should_preserve_for_time_span(self, time_span: TS) -> bool:
        """Returns whether we want to retain a snapshot for a time span.

        The return value is meaningful relative to the Policy's creation time
        (policy.now), not the current time when the function is called.

        Args:
            time_span: The (timestamp, timestamp) time span tuple in question.

        Returns:
            Whether this policy calls for a snapshot to be retained for the
                given time span.
        """
        return time_span in self._preserve_for_time_spans
