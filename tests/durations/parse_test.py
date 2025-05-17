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

import pytest

from btrfs2s3._internal.durations import Duration
from btrfs2s3._internal.durations import Kwargs


def test_malformed() -> None:
    with pytest.raises(ValueError, match="Not a valid ISO 8601 duration"):
        Duration("invalid")


def test_empty() -> None:
    with pytest.raises(ValueError, match="Not a valid ISO 8601 duration"):
        Duration("P")


def test_empty_after_time_separator() -> None:
    with pytest.raises(ValueError, match="Not a valid ISO 8601 duration"):
        Duration("P1DT")


def test_mixed_weeks() -> None:
    with pytest.raises(ValueError, match="Not a valid ISO 8601 duration"):
        Duration("P1W2D")


@pytest.mark.parametrize(
    ("value", "result"),
    [
        (
            "P1Y2M3DT4H5M6S",
            Duration(years=1, months=2, days=3, hours=4, minutes=5, seconds=6),
        ),
        ("P52W", Duration(weeks=52)),
    ],
)
def test_valid_parse_and_str(value: str, result: Duration) -> None:
    assert Duration(value) == result
    assert str(Duration(value)) == value


@pytest.mark.parametrize(
    ("value", "kwargs"),
    [
        (Duration(weeks=52), Kwargs(weeks=52)),
        (
            Duration(years=1, months=2, days=3, hours=4, minutes=5, seconds=6),
            Kwargs(years=1, months=2, days=3, hours=4, minutes=5, seconds=6),
        ),
    ],
)
def test_kwargs(value: Duration, kwargs: Kwargs) -> None:
    assert value.kwargs() == kwargs
