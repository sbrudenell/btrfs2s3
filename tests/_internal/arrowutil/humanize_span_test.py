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

from datetime import timezone

import arrow
import pytest

from btrfs2s3._internal.arrowutil import humanize_span


@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [
        ("2006", "2007", "2006/P1Y"),
        ("2006-01", "2006-04", "2006-Q1/P1Q"),
        ("2006-01", "2006-02", "2006-01/P1M"),
        ("2006-01-02", "2006-01-09", "2006-W01/P1W"),
        ("2006-01-02", "2006-01-03", "2006-01-02/P1D"),
        ("2006-01-02 15", "2006-01-02 16", "2006-01-02T15/PT1H"),
        ("2006-01-02 15:04", "2006-01-02 15:05", "2006-01-02T15:04/PT1M"),
        ("2006-01-02 15:04:05", "2006-01-02 15:04:06", "2006-01-02T15:04:05/PT1S"),
        ("2006", "2008", "2006-01-01T00:00:00/2008-01-01T00:00:00"),
    ],
)
def test_humanize_span(start: str, end: str, expected: str) -> None:
    tzinfo = timezone.utc
    span = (arrow.get(start, tzinfo=tzinfo), arrow.get(end, tzinfo=tzinfo))
    assert humanize_span(span, bounds="[]") == expected
