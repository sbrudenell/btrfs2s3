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

from typing import TYPE_CHECKING

import arrow
from rich.highlighter import ISO8601Highlighter
from rich.text import Text

from btrfs2s3._internal.cvar import TZINFO

if TYPE_CHECKING:
    from typing import Literal

    from typing_extensions import TypeAlias

    from btrfs2s3._internal.preservation import TS

    _Bounds: TypeAlias = Literal["[)", "()", "(]", "[]"]

_iso8601_highlight = ISO8601Highlighter()


def describe_time_span(time_span: TS, *, bounds: _Bounds = "[)") -> Text:
    """Returns a highlighted summary of a time span in context of preservation."""
    a_timestamp, b_timestamp = time_span
    a = arrow.get(a_timestamp, tzinfo=TZINFO.get())
    b = arrow.get(b_timestamp, tzinfo=TZINFO.get())
    if (a, b) == a.span("year", bounds=bounds):
        return Text.from_markup(f"[iso8601.date]{a.year:04d}[/] yearly")
    if (a, b) == a.span("quarter", bounds=bounds):
        return Text.from_markup(f"[iso8601.date]{a.year:04d}-Q{a.quarter}[/] quarterly")
    if (a, b) == a.span("month", bounds=bounds):
        return Text.from_markup(f"[iso8601.date]{a.year:04d}-{a.month:02d}[/] monthly")
    if (a, b) == a.span("week", bounds=bounds):
        return Text.from_markup(f"[iso8601.date]{a.year:04d}-W{a.week:02d}[/] weekly")
    if (a, b) == a.span("day", bounds=bounds):
        return Text.from_markup(
            f"[iso8601.date]{a.year:04d}-{a.month:02d}-{a.day:02d}[/] daily"
        )
    if (a, b) == a.span("hour", bounds=bounds):
        return Text.from_markup(
            f"[iso8601.date]{a.year:04d}-{a.month:02d}-{a.day:02d}[/]T"
            f"[iso8601.time]{a.hour:02d}[/] hourly"
        )
    if (a, b) == a.span("minute", bounds=bounds):
        return Text.from_markup(
            f"[iso8601.date]{a.year:04d}-{a.month:02d}-{a.day:02d}[/]T"
            f"[iso8601.time]{a.hour:02d}:{a.minute:02d}[/] minutely"
        )
    if (a, b) == a.span("second", bounds=bounds):
        return Text.from_markup(
            f"[iso8601.date]{a.year:04d}-{a.month:02d}-{a.day:02d}[/]T"
            f"[iso8601.time]{a.hour:02d}:{a.minute:02d}:{a.second:02d}[/] secondly"
        )
    return (
        _iso8601_highlight(a.format("YYYY-MM-DDTHH:mm:ss"))
        .append("/")
        .append(_iso8601_highlight(b.format("YYYY-MM-DDTHH:mm:ss")))
    )
