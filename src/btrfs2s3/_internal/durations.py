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
from typing import cast
from typing import Literal
from typing import overload
from typing import TYPE_CHECKING
from typing import TypedDict

if TYPE_CHECKING:
    from collections.abc import Collection

    from typing_extensions import TypeAlias
    from typing_extensions import Unpack


class Kwargs(TypedDict, total=False):
    years: int
    months: int
    weeks: int
    days: int
    hours: int
    minutes: int
    seconds: int


Key: TypeAlias = Literal[
    "years", "months", "weeks", "days", "hours", "minutes", "seconds"
]
KEYS: Collection[Key] = {
    "years",
    "months",
    "weeks",
    "days",
    "hours",
    "minutes",
    "seconds",
}


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


class Duration(dict[Key, int]):
    @overload
    def __init__(self, /, value: str) -> None: ...
    @overload
    def __init__(self, **kwargs: Unpack[Kwargs]) -> None: ...
    def __init__(self, value: str | None = None, **kwargs: Unpack[Kwargs]) -> None:
        if value is not None:
            m = _REGEX.match(value)
            if not m:
                msg = "Not a valid ISO 8601 duration"
                raise ValueError(msg)
            kwargs = Kwargs()
            for key in KEYS:
                if (v := m.group(key)) is not None:
                    kwargs[key] = int(v)
        # Should validate kwargs thoroughly here
        super().__init__(cast("dict[Key, int]", kwargs))

    # Should overload __setitem__ etc to validate

    def __str__(self) -> str:
        parts = ["P"]
        if (v := self.get("years")) is not None:
            parts.extend((str(v), "Y"))
        if (v := self.get("months")) is not None:
            parts.extend((str(v), "M"))
        if (v := self.get("weeks")) is not None:
            parts.extend((str(v), "W"))
        if (v := self.get("days")) is not None:
            parts.extend((str(v), "D"))
        if any(key in self for key in ("hours", "minutes", "seconds")):
            parts.append("T")
        if (v := self.get("hours")) is not None:
            parts.extend((str(v), "H"))
        if (v := self.get("minutes")) is not None:
            parts.extend((str(v), "M"))
        if (v := self.get("seconds")) is not None:
            parts.extend((str(v), "S"))
        return "".join(parts)

    def kwargs(self) -> Kwargs:
        return cast("Kwargs", self)
