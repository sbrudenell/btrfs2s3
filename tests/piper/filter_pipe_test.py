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

from concurrent.futures import ThreadPoolExecutor
from subprocess import CalledProcessError
from typing import cast
from typing import TYPE_CHECKING

import pytest

from btrfs2s3._internal.piper import filter_pipe

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import IO


def write_then_close(w: IO[bytes], data: bytes) -> None:
    w.write(data)
    w.close()


def feed_and_check(
    r: IO[bytes], w: IO[bytes], send_data: bytes, expect_data: bytes
) -> None:
    with ThreadPoolExecutor() as executor:
        executor.submit(write_then_close, w, send_data)
        got_data = executor.submit(r.read).result()
    assert got_data == expect_data


@pytest.mark.parametrize("length", [0, 1, 2, 3])
def test_noop(length: int) -> None:
    with filter_pipe([["cat"]] * length) as (r, w):
        feed_and_check(r, w, b"hello", b"hello")


@pytest.fixture(params=[(length, i) for length in range(1, 4) for i in range(length)])
def pipeline_that_fails(request: pytest.FixtureRequest) -> Sequence[Sequence[str]]:
    length, fail_position = cast(tuple[int, int], request.param)
    cmds = [["cat"]] * length
    cmds[fail_position] = ["sh", "-c", "exit 1"]
    return cmds


def test_pipefail_raises_error(pipeline_that_fails: Sequence[Sequence[str]]) -> None:
    with pytest.raises(CalledProcessError), filter_pipe(pipeline_that_fails) as (r, w):
        w.close()
