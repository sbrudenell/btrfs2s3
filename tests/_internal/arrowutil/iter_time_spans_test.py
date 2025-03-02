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

from btrfs2s3._internal.arrowutil import iter_time_spans


def test_iter_time_spans() -> None:
    """Basic test of iter_time_spans with supported timeframes."""
    _ = arrow.get

    a = _("2006-01-02T15:04:05+07:00")

    got = list(iter_time_spans(a, bounds="[]", years=(-1, 0, 1)))
    expected = [
        (_("2005-01-01T00:00:00+07:00"), _("2006-01-01T00:00:00+07:00")),
        (_("2006-01-01T00:00:00+07:00"), _("2007-01-01T00:00:00+07:00")),
        (_("2007-01-01T00:00:00+07:00"), _("2008-01-01T00:00:00+07:00")),
    ]
    assert got == expected

    got = list(iter_time_spans(a, bounds="[]", quarters=(-1, 0, 1)))
    expected = [
        (_("2005-10-01T00:00:00+07:00"), _("2006-01-01T00:00:00+07:00")),
        (_("2006-01-01T00:00:00+07:00"), _("2006-04-01T00:00:00+07:00")),
        (_("2006-04-01T00:00:00+07:00"), _("2006-07-01T00:00:00+07:00")),
    ]
    assert got == expected

    got = list(iter_time_spans(a, bounds="[]", months=(-1, 0, 1)))
    expected = [
        (_("2005-12-01T00:00:00+07:00"), _("2006-01-01T00:00:00+07:00")),
        (_("2006-01-01T00:00:00+07:00"), _("2006-02-01T00:00:00+07:00")),
        (_("2006-02-01T00:00:00+07:00"), _("2006-03-01T00:00:00+07:00")),
    ]
    assert got == expected

    got = list(iter_time_spans(a, bounds="[]", weeks=(-1, 0, 1)))
    expected = [
        (_("2005-12-26T00:00:00+07:00"), _("2006-01-02T00:00:00+07:00")),
        (_("2006-01-02T00:00:00+07:00"), _("2006-01-09T00:00:00+07:00")),
        (_("2006-01-09T00:00:00+07:00"), _("2006-01-16T00:00:00+07:00")),
    ]
    assert got == expected

    got = list(iter_time_spans(a, bounds="[]", days=(-1, 0, 1)))
    expected = [
        (_("2006-01-01T00:00:00+07:00"), _("2006-01-02T00:00:00+07:00")),
        (_("2006-01-02T00:00:00+07:00"), _("2006-01-03T00:00:00+07:00")),
        (_("2006-01-03T00:00:00+07:00"), _("2006-01-04T00:00:00+07:00")),
    ]
    assert got == expected

    got = list(iter_time_spans(a, bounds="[]", hours=(-1, 0, 1)))
    expected = [
        (_("2006-01-02T14:00:00+07:00"), _("2006-01-02T15:00:00+07:00")),
        (_("2006-01-02T15:00:00+07:00"), _("2006-01-02T16:00:00+07:00")),
        (_("2006-01-02T16:00:00+07:00"), _("2006-01-02T17:00:00+07:00")),
    ]
    assert got == expected

    got = list(iter_time_spans(a, bounds="[]", minutes=(-1, 0, 1)))
    expected = [
        (_("2006-01-02T15:03:00+07:00"), _("2006-01-02T15:04:00+07:00")),
        (_("2006-01-02T15:04:00+07:00"), _("2006-01-02T15:05:00+07:00")),
        (_("2006-01-02T15:05:00+07:00"), _("2006-01-02T15:06:00+07:00")),
    ]
    assert got == expected

    got = list(iter_time_spans(a, bounds="[]", seconds=(-1, 0, 1)))
    expected = [
        (_("2006-01-02T15:04:04+07:00"), _("2006-01-02T15:04:05+07:00")),
        (_("2006-01-02T15:04:05+07:00"), _("2006-01-02T15:04:06+07:00")),
        (_("2006-01-02T15:04:06+07:00"), _("2006-01-02T15:04:07+07:00")),
    ]
    assert got == expected
