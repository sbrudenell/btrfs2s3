import contextlib
import pathlib
import os
from typing import Iterator

@contextlib.contextmanager
def with_fd(path:pathlib.Path, flags:int) -> Iterator[int]:
    fd = os.open(path, flags)
    try:
        yield fd
    finally:
        os.close(fd)


