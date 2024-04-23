import os
import sys
from types import TracebackType
from typing import Any
from typing import Final
from typing import Iterator
from typing import Literal
from typing import overload
from typing import TypeAlias
from typing import TypeVar

from _typeshed import structseq
from typing_extensions import final
from typing_extensions import Self

@final
class SubvolumeInfo(  # noqa: SLOT001
    structseq[Any],
    tuple[
        int, int, int, int, bytes, bytes, bytes, int, int, int, int, int, float, float
    ],
):
    if sys.version_info >= (3, 10):
        __match_args__: Final = (
            "id",
            "parent_id",
            "dir_id",
            "flags",
            "uuid",
            "parent_uuid",
            "received_uuid",
            "generation",
            "ctransid",
            "otransid",
            "stransid",
            "rtransid",
            "ctime",
            "otime",
        )
    @property
    def id(self) -> int: ...
    @property
    def parent_id(self) -> int: ...
    @property
    def dir_id(self) -> int: ...
    @property
    def flags(self) -> int: ...
    @property
    def uuid(self) -> bytes: ...
    @property
    def parent_uuid(self) -> bytes: ...
    @property
    def received_uuid(self) -> bytes: ...
    @property
    def generation(self) -> int: ...
    @property
    def ctransid(self) -> int: ...
    @property
    def otransid(self) -> int: ...
    @property
    def stransid(self) -> int: ...
    @property
    def rtransid(self) -> int: ...
    @property
    def ctime(self) -> float: ...
    @property
    def otime(self) -> float: ...
    @property
    def stime(self) -> float: ...
    @property
    def rtime(self) -> float: ...

_IT_co = TypeVar("_IT_co", covariant=True, bound=int | SubvolumeInfo)
_Path: TypeAlias = str | bytes | os.PathLike[str] | os.PathLike[bytes]

class SubvolumeIterator(Iterator[tuple[str, _IT_co]]):
    @overload
    def __new__(
        cls,
        path: _Path | int,
        *,
        info: Literal[True],
        top: int = 0,
        post_order: bool = False,
    ) -> SubvolumeIterator[SubvolumeInfo]: ...
    @overload
    def __new__(
        cls,
        path: _Path | int,
        *,
        info: Literal[False],
        top: int = 0,
        post_order: bool = False,
    ) -> SubvolumeIterator[int]: ...
    @overload
    def __new__(
        cls, path: _Path | int, *, top: int = 0, post_order: bool = False
    ) -> SubvolumeIterator[int]: ...
    def __next__(self) -> tuple[str, _IT_co]: ...
    def __iter__(self) -> Self: ...
    def close(self) -> None: ...
    def fileno(self) -> int: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> bool | None: ...

class QgroupInherit:
    @property
    def groups(self) -> list[int]: ...
    def add_group(self, qgroupid: int) -> None: ...

@overload
def create_snapshot(
    source: _Path | int,
    path: _Path,
    *,
    async_: Literal[True],
    recursive: bool = False,
    read_only: bool = False,
    qgroup_inherit: QgroupInherit | None = None,
) -> int: ...
@overload
def create_snapshot(
    source: _Path | int,
    path: _Path,
    *,
    async_: Literal[False],
    recursive: bool = False,
    read_only: bool = False,
    qgroup_inherit: QgroupInherit | None = None,
) -> None: ...
@overload
def create_snapshot(
    source: _Path | int,
    path: _Path,
    *,
    recursive: bool = False,
    read_only: bool = False,
    qgroup_inherit: QgroupInherit | None = None,
) -> None: ...
@overload
def create_subvolume(
    path: _Path, *, async_: Literal[True], qgroup_inherit: QgroupInherit | None = None
) -> int: ...
@overload
def create_subvolume(
    path: _Path, *, async_: Literal[False], qgroup_inherit: QgroupInherit | None = None
) -> None: ...
@overload
def create_subvolume(
    path: _Path, *, qgroup_inherit: QgroupInherit | None = None
) -> None: ...
def is_subvolume(path: _Path | int) -> bool: ...
def subvolume_info(path: _Path | int, id: int = 0) -> SubvolumeInfo: ...
def delete_subvolume(path: _Path, *, recursive: bool = False) -> None: ...
def sync(path: _Path | int) -> None: ...
