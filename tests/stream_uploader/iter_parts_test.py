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

from typing import TYPE_CHECKING

from btrfs2s3._internal.stream_uploader import _iter_parts_via_tempfile

if TYPE_CHECKING:
    from typing import IO


def test_iter_parts_via_tempfile(
    stream: IO[bytes], stream_data: bytes, part_size: int
) -> None:
    expected_data = [
        stream_data[i : i + part_size] for i in range(0, len(stream_data), part_size)
    ]

    got_data = []
    for part_file in _iter_parts_via_tempfile(stream, part_size):
        assert part_file.seekable()
        assert part_file.tell() == 0
        got_data.append(part_file.read())

    assert got_data == expected_data
