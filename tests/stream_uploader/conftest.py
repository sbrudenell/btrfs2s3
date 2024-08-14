from __future__ import annotations

import os
from tempfile import TemporaryFile
from threading import Thread
from typing import cast
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from typing import IO
    from typing import Iterator


@pytest.fixture(params=[5 * 2**20, 5 * 2**20 + 512, 10 * 2**20])
def data_size(request: pytest.FixtureRequest) -> int:
    return cast(int, request.param)


@pytest.fixture(params=[5 * 2**20, 5 * 2**20 + 512, 10 * 2**20])
def part_size(request: pytest.FixtureRequest) -> int:
    return cast(int, request.param)


@pytest.fixture()
def stream_data(data_size: int) -> bytes:
    return os.urandom(data_size)


@pytest.fixture(params=[-1, 0], ids=["buffered", "unbuffered"])
def buffering(request: pytest.FixtureRequest) -> int:
    return cast(int, request.param)


@pytest.fixture(params=[False, True], ids=["nonseekable", "seekable"])
def seekable(request: pytest.FixtureRequest) -> bool:
    return cast(bool, request.param)


@pytest.fixture()
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
