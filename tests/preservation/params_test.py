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

import pytest

from btrfs2s3._internal.preservation import Params


def test_args() -> None:
    c = Params(
        years=1, quarters=2, months=3, weeks=4, days=5, hours=6, minutes=7, seconds=8
    )

    assert c.years == 1
    assert c.quarters == 2
    assert c.months == 3
    assert c.weeks == 4
    assert c.days == 5
    assert c.hours == 6
    assert c.minutes == 7
    assert c.seconds == 8


def test_args_invalid() -> None:
    with pytest.raises(ValueError, match="can't be negative"):
        Params(years=-1)


def test_parse() -> None:
    c = Params.parse("1y 4q 12m 52w 365d 8760h 525600M 31536000s")

    assert c.years == 1
    assert c.quarters == 4
    assert c.months == 12
    assert c.weeks == 52
    assert c.days == 365
    assert c.hours == 8760
    assert c.minutes == 525600
    assert c.seconds == 31536000


def test_parse_fail() -> None:
    with pytest.raises(ValueError, match="invalid preservation params"):
        Params.parse("invalid")


def test_parse_fail_wrong_format() -> None:
    with pytest.raises(ValueError, match="invalid preservation params"):
        Params.parse("1m 1y")
