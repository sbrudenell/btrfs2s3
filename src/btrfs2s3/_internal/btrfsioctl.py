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

from contextlib import contextmanager
from ctypes import c_char
from ctypes import c_int64
from ctypes import c_uint8
from ctypes import c_uint32
from ctypes import c_uint64
from ctypes import POINTER
from ctypes import sizeof
from ctypes import Structure
from ctypes import Union as CtypesUnion
from enum import IntFlag
from fcntl import ioctl
import os
import os.path
from pathlib import Path
import re
from subprocess import check_output
from typing import cast
from typing import NamedTuple
from typing import overload
from typing import Protocol
from typing import runtime_checkable
from typing import TYPE_CHECKING
from typing import Union

if TYPE_CHECKING:
    from collections.abc import Iterator
    from ctypes import _CData
    from typing import AnyStr

    from typing_extensions import TypeAlias


_IOC_NRBITS = 8
_IOC_TYPEBITS = 8
_IOC_SIZEBITS = 14
_IOC_DIRBITS = 2

_IOC_NONE = 0
_IOC_WRITE = 1
_IOC_READ = 2


_machine = os.uname().machine

if _machine.startswith("parisc"):  # pragma: no cover
    _IOC_WRITE = 2
    _IOC_READ = 1
elif _machine == "alpha" or _machine.startswith(
    ("mips", "sparc", "ppc", "powerpc")
):  # pragma: no cover
    _IOC_SIZEBITS = 13
    _IOC_DIRBITS = 3
    _IOC_NONE = 1
    _IOC_READ = 2
    _IOC_WRITE = 4


_IOC_NRSHIFT = 0
_IOC_TYPESHIFT = _IOC_NRSHIFT + _IOC_NRBITS
_IOC_SIZESHIFT = _IOC_TYPESHIFT + _IOC_TYPEBITS
_IOC_DIRSHIFT = _IOC_SIZESHIFT + _IOC_SIZEBITS

IOCTL_MAGIC = 0x94


def _ioc(_dir: int, nr: int, size: _CData | type[_CData] | int) -> int:
    return (
        (_dir << _IOC_DIRSHIFT)
        | (IOCTL_MAGIC << _IOC_TYPESHIFT)
        | (nr << _IOC_NRSHIFT)
        | (size if isinstance(size, int) else sizeof(size) << _IOC_SIZESHIFT)
    )


def _ior(nr: int, size: _CData | type[_CData]) -> int:
    return _ioc(_IOC_READ, nr, size)


def _iow(nr: int, size: _CData | type[_CData]) -> int:
    return _ioc(_IOC_WRITE, nr, size)


VOL_NAME_MAX = 255
SUBVOL_NAME_MAX = 4039
UUID_SIZE = 16
FS_TREE_OBJECTID = 5
FIRST_FREE_OBJECTID = 256
NULL_UUID = b"\0" * UUID_SIZE


class SubvolArgsFlag(IntFlag):
    ReadOnly = 1 << 1
    QgroupInherit = 1 << 2
    DeviceSpecById = 1 << 3
    SubvolSpecById = 1 << 4


class SubvolFlag(IntFlag):
    ReadOnly = 1 << 0


class _VolArgsV2UnionId(CtypesUnion):
    _fields_ = (
        ("name", c_char * (SUBVOL_NAME_MAX + 1)),
        ("devid", c_uint64),
        ("subvolid", c_uint64),
    )


class VolArgsV2(Structure):
    _fields_ = (
        ("fd", c_int64),
        ("transid", c_uint64),
        ("flags", c_uint64),
        # qgroup args go here, but we don't use them
        ("_unused", c_uint64 * 4),
        ("_u", _VolArgsV2UnionId),
    )
    _anonymous_ = ("_u",)


assert sizeof(VolArgsV2) == 4096  # noqa: PLR2004


class TimeSpec(Structure):
    _fields_ = (("sec", c_uint64), ("nsec", c_uint32))

    def to_float(self) -> float:
        return cast(int, self.sec) + cast(int, self.nsec) * 1e-9


class SendFlag(IntFlag):
    NoFileData = 0x1
    OmitStreamHeader = 0x2
    OmitEndCmd = 0x4
    Version = 0x8
    Compressed = 0x10


class SendArgs(Structure):
    _fields_ = (
        ("send_fd", c_int64),
        ("_clone_sources_count", c_uint64),
        ("_clone_sources", POINTER(c_uint64)),
        ("parent_root", c_uint64),
        ("flags", c_uint64),
        ("version", c_uint32),
        ("_reserved", c_uint8 * 28),
    )


assert sizeof(SendArgs) == (72 if sizeof(POINTER(c_uint64)) == 8 else 68)  # noqa: PLR2004


class SubvolInfo(NamedTuple):
    id: int = 0
    name: str = ""
    parent_id: int = 0
    dir_id: int = 0
    generation: int = 0
    flags: int = 0
    uuid: bytes = NULL_UUID
    parent_uuid: bytes | None = None
    received_uuid: bytes | None = None
    ctransid: int = 0
    otransid: int = 0
    stransid: int = 0
    rtransid: int = 0
    ctime: float = 0.0
    otime: float = 0.0
    stime: float = 0.0
    rtime: float = 0.0


class SubvolInfoStruct(Structure):
    _fields_ = (
        ("id", c_uint64),
        ("name", c_char * (VOL_NAME_MAX + 1)),
        ("parent_id", c_uint64),
        ("dir_id", c_uint64),
        ("generation", c_uint64),
        ("flags", c_uint64),
        ("uuid", c_uint8 * UUID_SIZE),
        ("parent_uuid", c_uint8 * UUID_SIZE),
        ("received_uuid", c_uint8 * UUID_SIZE),
        ("ctransid", c_uint64),
        ("otransid", c_uint64),
        ("stransid", c_uint64),
        ("rtransid", c_uint64),
        ("ctime", TimeSpec),
        ("otime", TimeSpec),
        ("stime", TimeSpec),
        ("rtime", TimeSpec),
        ("_reserved", c_uint64 * 8),
    )

    def to_tuple(self) -> SubvolInfo:
        return SubvolInfo(
            id=self.id,
            name=os.fsdecode(self.name),
            parent_id=self.parent_id,
            dir_id=self.dir_id,
            generation=self.generation,
            flags=self.flags,
            uuid=bytes(self.uuid),
            parent_uuid=bytes(self.parent_uuid)
            if bytes(self.parent_uuid) != NULL_UUID
            else None,
            received_uuid=bytes(self.received_uuid)
            if bytes(self.received_uuid) != NULL_UUID
            else None,
            ctransid=self.ctransid,
            otransid=self.otransid,
            stransid=self.stransid,
            rtransid=self.rtransid,
            ctime=self.ctime.to_float(),
            otime=self.otime.to_float(),
            stime=self.stime.to_float(),
            rtime=self.rtime.to_float(),
        )


IOC_SNAP_CREATE_V2 = _iow(23, VolArgsV2)
IOC_SUBVOL_CREATE_V2 = _iow(24, VolArgsV2)
IOC_SEND = _iow(38, SendArgs)
IOC_GET_SUBVOL_INFO = _ior(60, SubvolInfoStruct)
IOC_SNAP_DESTROY_V2 = _iow(63, VolArgsV2)


_PathLike: TypeAlias = Union[str, bytes, os.PathLike[str], os.PathLike[bytes]]


@overload
def _split(p: AnyStr) -> tuple[AnyStr, AnyStr]: ...
@overload
def _split(p: os.PathLike[AnyStr]) -> tuple[AnyStr, AnyStr]: ...
def _split(p: AnyStr | os.PathLike[AnyStr]) -> tuple[AnyStr, AnyStr]:
    return os.path.split(os.path.realpath(p))


@contextmanager
def opendir(path: _PathLike, dir_fd: int | None = None) -> Iterator[int]:
    fd = os.open(path, os.O_RDONLY, dir_fd=dir_fd)
    try:
        yield fd
    finally:
        os.close(fd)


def subvol_info(fd: int | _PathLike, /, *, dir_fd: int | None = None) -> SubvolInfo:
    if not isinstance(fd, int):
        with opendir(fd, dir_fd=dir_fd) as fd_:
            return subvol_info(fd_)
    info = SubvolInfoStruct()
    ioctl(fd, IOC_GET_SUBVOL_INFO, info)
    return info.to_tuple()


def create_snap(
    *,
    src: int | _PathLike,
    dst: _PathLike,
    src_dir_fd: int | None = None,
    dst_dir_fd: int | None = None,
    read_only: bool = False,
) -> None:
    if not isinstance(src, int):
        with opendir(src, dir_fd=src_dir_fd) as src_fd:
            create_snap(src=src_fd, dst=dst, dst_dir_fd=dst_dir_fd, read_only=read_only)
            return
    if dst_dir_fd is None:
        (dst_dir, dst) = _split(dst)
        with opendir(dst_dir) as dst_dir_fd:  # noqa: PLR1704
            create_snap(src=src, dst=dst, dst_dir_fd=dst_dir_fd, read_only=read_only)
            return
    flags = 0
    if read_only:
        flags |= SubvolArgsFlag.ReadOnly
    ioctl(
        dst_dir_fd,
        IOC_SNAP_CREATE_V2,
        VolArgsV2(fd=src, name=os.fsencode(dst), flags=flags),
    )


def create_subvol(path: _PathLike, /, *, dir_fd: int | None = None) -> None:
    if dir_fd is None:
        dir_, path = _split(path)
        with opendir(dir_) as dir_fd:  # noqa: PLR1704
            create_subvol(path, dir_fd=dir_fd)
            return
    ioctl(dir_fd, IOC_SUBVOL_CREATE_V2, VolArgsV2(name=os.fsencode(path)))


def get_kernel_send_proto() -> int:
    # /sys/fs/btrfs may not exist at all before the module is loaded. make sure
    # we raise an error in this case
    base_path = Path("/sys/fs/btrfs")
    if not base_path.exists():
        raise FileNotFoundError(base_path)  # pragma: no cover
    # send_stream_version was introduced in kernel 5.9. before that, only
    # version 1 was supported.
    path = Path("/sys/fs/btrfs/features/send_stream_version")
    if path.exists():
        return int(path.read_text())  # pragma: no cover
    return 1  # pragma: no cover


def get_userspace_send_proto() -> int:
    full_output = check_output(["btrfs", "version"], text=True)
    m = re.match(r"^btrfs-progs v(.*)$", full_output, re.MULTILINE)
    assert m is not None, full_output
    version_str = m.group(1)
    version = tuple(int(part) for part in version_str.split("."))
    if version >= (5, 19):
        return 2  # pragma: no cover
    return 1  # pragma: no cover


@runtime_checkable
class _HasFileno(Protocol):
    def fileno(self) -> int: ...


def send(
    *,
    src: int | _PathLike,
    dst: int | _HasFileno,
    proto: int | None = None,
    parent_id: int = 0,
    flags: int = 0,
) -> None:
    if not isinstance(src, int):
        with opendir(src) as fd:
            send(src=fd, dst=dst, proto=proto, parent_id=parent_id, flags=flags)
            return
    if not isinstance(dst, int):
        dst = dst.fileno()
    if proto is None:
        proto = 0
    else:
        flags |= SendFlag.Version
    ioctl(
        src,
        IOC_SEND,
        SendArgs(send_fd=dst, flags=flags, parent_root=parent_id, version=proto),
    )


@overload
def destroy_snap(*, dir_fd: int, snapshot_id: int) -> None: ...
@overload
def destroy_snap(path: _PathLike, /, *, dir_fd: int | None = None) -> None: ...
@overload
def destroy_snap(path: _PathLike, /, *, snapshot_id: int) -> None: ...
def destroy_snap(
    path: _PathLike | None = None,
    /,
    *,
    dir_fd: int | None = None,
    snapshot_id: int | None = None,
) -> None:
    if dir_fd is None:
        assert path is not None
        if snapshot_id is None:
            dir_, path = _split(path)
            with opendir(dir_) as dir_fd:  # noqa: PLR1704
                destroy_snap(path, dir_fd=dir_fd)
        else:
            with opendir(path) as dir_fd:
                destroy_snap(dir_fd=dir_fd, snapshot_id=snapshot_id)
        return
    args = VolArgsV2()
    if snapshot_id is not None:
        assert path is None
        args.flags |= SubvolArgsFlag.SubvolSpecById
        args.subvolid = snapshot_id
    else:
        assert path is not None
        args.name = os.fsencode(path)
    ioctl(dir_fd, IOC_SNAP_DESTROY_V2, args)
