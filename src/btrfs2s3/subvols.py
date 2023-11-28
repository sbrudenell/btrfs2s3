from pathlib import Path
import contextlib
import btrfsutil
from btrfsutil import SubvolumeInfo
from uuid import UUID
from typing import Iterator
from typing import TypeAlias
# btrfsutil offers a large number of ways to refer to a subvolume.

# absolute path
# absolute path -> subvolume id within volume
# absolute path -> subvolume id within volume ("top") -> relative path
# file descriptor
# file descriptor -> relative path
# file descriptor -> subvolume id within volume
# file descriptor -> subvolume id within volume ("top") -> relative path

Ref:TypeAlias = (
    Path | tuple[Path, int] | tuple[Path, int, Path] |
    int | tuple[int, Path] | tuple[int, int] | tuple[int, int, Path])

class Subvol:
    def __init__(self, ref:Ref, info:SubvolumeInfo=None) -> None:
        self._ref = ref
        self._info = info

    def ref(self) -> Ref:
        return self._ref

    def info(self) -> SubvolumeInfo:
        if self._info is None:
            match self._ref:
                case int(fd):
                    self._info = btrfsutil.subvolume_info(fd)
                case _:
                    raise NotImplementedError
        return self._info

    def uuid(self) -> UUID:
        return UUID(bytes=self.info().uuid)

    def parent_uuid(self) -> UUID:
        return UUID(bytes=self.info().parent_uuid)

NULL_UUID = b"\0" * 16
FLAG_READ_ONLY = 1

def create_snapshot(src:int|Path, dst:Path, read_only=False, recursive=False) -> None:
    btrfsutil.create_snapshot(src, dst, read_only=read_only,
            recursive=recursive)

def rename(src:tuple[int, Path], dst:tuple[int, Path]) -> None:
    src_fd, src_path = src
    dst_fd, dst_path = dst
    os.rename(src_path, dst_path, src_dir_fd=src_fd, dst_dir_fd=dst_fd)


@contextlib.contextmanager
def iter(ref:int, info=False) -> Iterator[tuple[btrfsutil.SubvolumeIterator, Iterator[Subvol]]]:
    if not info:
        raise NotImplementedError
    with btrfsutil.SubvolumeIterator(ref, top=0, info=info) as subvoliter:
        def inner() -> Iterator[Subvol]:
            for path, info in subvoliter:
                yield Subvol((ref, pathlib.Path(path)), info=info)
        yield subvoliter, inner()
