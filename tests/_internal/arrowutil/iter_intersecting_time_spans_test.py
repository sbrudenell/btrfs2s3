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

import arrow

from btrfs2s3._internal.arrowutil import iter_intersecting_time_spans


def test_iter_intersecting_time_spans() -> None:
    """Basic test of iter_intersecting_time_spans."""
    _ = arrow.get

    a = _("2006-01-02T15:04:05+07:00")

    got = list(iter_intersecting_time_spans(a, bounds="[]"))
    expected = [
        (_("2006-01-01T00:00:00+07:00"), _("2007-01-01T00:00:00+07:00")),
        (_("2006-01-01T00:00:00+07:00"), _("2006-04-01T00:00:00+07:00")),
        (_("2006-01-01T00:00:00+07:00"), _("2006-02-01T00:00:00+07:00")),
        (_("2006-01-02T00:00:00+07:00"), _("2006-01-09T00:00:00+07:00")),
        (_("2006-01-02T00:00:00+07:00"), _("2006-01-03T00:00:00+07:00")),
        (_("2006-01-02T15:00:00+07:00"), _("2006-01-02T16:00:00+07:00")),
        (_("2006-01-02T15:04:00+07:00"), _("2006-01-02T15:05:00+07:00")),
        (_("2006-01-02T15:04:05+07:00"), _("2006-01-02T15:04:06+07:00")),
    ]

    assert got == expected
