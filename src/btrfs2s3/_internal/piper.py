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

from collections.abc import Sequence
from contextlib import contextmanager
from contextlib import ExitStack
import os
from subprocess import CalledProcessError
from subprocess import PIPE
from subprocess import Popen
from typing import AnyStr
from typing import NamedTuple
from typing import TYPE_CHECKING
from typing import Union

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import IO

    from typing_extensions import TypeAlias

_StrOrBytesPath = Union[os.PathLike[str], os.PathLike[bytes], str, bytes]
_CMD: TypeAlias = Union[_StrOrBytesPath, Sequence[_StrOrBytesPath]]


class Pipe(NamedTuple):
    r: IO[bytes]
    w: IO[bytes]


@contextmanager
def _pipe() -> Iterator[Pipe]:
    r_fd, w_fd = os.pipe()
    with (
        open(r_fd, mode="rb") as r,  # noqa: PTH123
        open(w_fd, mode="wb") as w,  # noqa: PTH123
    ):
        yield Pipe(r=r, w=w)


@contextmanager
def _pipefail(process: Popen[AnyStr]) -> Iterator[Popen[AnyStr]]:
    # We could do try/finally here to create a big exception stack on an error,
    # but there's probably not a need
    with process:
        yield process
    retcode = process.wait()
    if retcode != 0:
        raise CalledProcessError(retcode, process.args)


@contextmanager
def filter_pipe(commands: Sequence[_CMD]) -> Iterator[Pipe]:
    pipe: Pipe | None = None
    with ExitStack() as stack:
        for args in commands:
            process = Popen(args, stdin=pipe.r if pipe else PIPE, stdout=PIPE)
            stack.enter_context(_pipefail(process))
            # NB: Popen.stdout is only non-None when Popen(stdout=PIPE) is passed
            # https://github.com/python/typeshed/issues/3831
            assert process.stdout is not None
            if pipe:
                # https://docs.python.org/3/library/subprocess.html#replacing-shell-pipeline
                pipe.r.close()
                pipe = Pipe(r=process.stdout, w=pipe.w)
            else:
                assert process.stdin is not None
                pipe = Pipe(r=process.stdout, w=process.stdin)
        if pipe:
            yield pipe
        else:
            yield stack.enter_context(_pipe())
