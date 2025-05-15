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

import re
from typing import NamedTuple
from typing import TYPE_CHECKING
from typing import TypedDict

if TYPE_CHECKING:
    from typing_extensions import NotRequired
    from typing_extensions import Self


class Kwargs(TypedDict):
    years: NotRequired[int]
    months: NotRequired[int]
    weeks: NotRequired[int]
    days: NotRequired[int]
    hours: NotRequired[int]
    minutes: NotRequired[int]
    seconds: NotRequired[int]


_REGEX = re.compile(
    r"^P((?!$)"
    r"((?P<years>\d+)Y)?"
    r"((?P<months>\d+)M)?"
    r"((?P<days>\d+)D)?"
    r"(T(?!$)"
    r"((?P<hours>\d+)H)?"
    r"((?P<minutes>\d+)M)?"
    r"((?P<seconds>\d+)S)?"
    r")?|(?P<weeks>\d+)W)$"
)


class Duration(NamedTuple):
    @classmethod
    def parse(cls, value: str) -> Self:
        m = _REGEX.match(value)
        if not m:
            msg = "Not a valid ISO 8601 duration"
            raise ValueError(msg)
        years = int(y) if (y := m.group("years")) else None
        months = int(mo) if (mo := m.group("months")) else None
        weeks = int(w) if (w := m.group("weeks")) else None
        days = int(d) if (d := m.group("days")) else None
        hours = int(h) if (h := m.group("hours")) else None
        minutes = int(mi) if (mi := m.group("minutes")) else None
        seconds = int(s) if (s := m.group("seconds")) else None
        return cls(
            years=years,
            months=months,
            weeks=weeks,
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
        )

    years: int | None = None
    months: int | None = None
    weeks: int | None = None
    days: int | None = None
    hours: int | None = None
    minutes: int | None = None
    seconds: int | None = None

    def is_nonzero(self) -> bool:
        return any((v or 0) > 0 for v in self)

    def __str__(self) -> str:
        parts = ["P"]
        if self.years is not None:
            parts.extend((str(self.years), "Y"))
        if self.months is not None:
            parts.extend((str(self.months), "M"))
        if self.weeks is not None:
            parts.extend((str(self.weeks), "W"))
        if self.days is not None:
            parts.extend((str(self.days), "D"))
        if (
            self.hours is not None
            or self.minutes is not None
            or self.seconds is not None
        ):
            parts.append("T")
        if self.hours is not None:
            parts.extend((str(self.hours), "H"))
        if self.minutes is not None:
            parts.extend((str(self.minutes), "M"))
        if self.seconds is not None:
            parts.extend((str(self.seconds), "S"))
        return "".join(parts)

    def kwargs(self) -> Kwargs:
        result = Kwargs()
        if self.years is not None:
            result["years"] = self.years
        if self.months is not None:
            result["months"] = self.months
        if self.weeks is not None:
            result["weeks"] = self.weeks
        if self.days is not None:
            result["days"] = self.days
        if self.hours is not None:
            result["hours"] = self.hours
        if self.minutes is not None:
            result["minutes"] = self.minutes
        if self.seconds is not None:
            result["seconds"] = self.seconds
        return result
