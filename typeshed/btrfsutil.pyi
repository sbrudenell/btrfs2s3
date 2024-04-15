import sys
from typing import Any
from typing import Final

from _typeshed import structseq
from typing_extensions import final

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
