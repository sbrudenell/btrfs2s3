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

import os
from tempfile import TemporaryFile
from threading import Thread
from typing import cast
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import IO


@pytest.fixture(params=[5 * 2**20, 5 * 2**20 + 512, 10 * 2**20])
def data_size(request: pytest.FixtureRequest) -> int:
    return cast(int, request.param)


@pytest.fixture(params=[5 * 2**20, 5 * 2**20 + 512, 10 * 2**20])
def part_size(request: pytest.FixtureRequest) -> int:
    return cast(int, request.param)


@pytest.fixture
def stream_data(data_size: int) -> bytes:
    return os.urandom(data_size)


@pytest.fixture(params=[-1, 0], ids=["buffered", "unbuffered"])
def buffering(request: pytest.FixtureRequest) -> int:
    return cast(int, request.param)


@pytest.fixture(params=[False, True], ids=["nonseekable", "seekable"])
def seekable(request: pytest.FixtureRequest) -> bool:
    return cast(bool, request.param)


@pytest.fixture
def stream(buffering: int, seekable: bool, stream_data: bytes) -> Iterator[IO[bytes]]:  # noqa: FBT001
    if seekable:
        with TemporaryFile(buffering=buffering) as stream:
            stream.write(stream_data)
            stream.seek(0, os.SEEK_SET)
            yield stream
    else:
        read_fd, write_fd = os.pipe()

        def fill(write_fd: int) -> None:
            os.write(write_fd, stream_data)
            os.close(write_fd)

        fill_thread = Thread(target=fill, args=(write_fd,))
        fill_thread.start()
        with open(read_fd, mode="rb", buffering=buffering) as stream:  # noqa: PTH123
            yield stream
        fill_thread.join()
