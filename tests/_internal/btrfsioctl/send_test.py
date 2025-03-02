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

import os
from random import randbytes
from subprocess import check_call
from subprocess import DEVNULL
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING

import pytest

from btrfs2s3._internal.btrfsioctl import create_snap
from btrfs2s3._internal.btrfsioctl import create_subvol
from btrfs2s3._internal.btrfsioctl import destroy_snap
from btrfs2s3._internal.btrfsioctl import get_kernel_send_proto
from btrfs2s3._internal.btrfsioctl import get_userspace_send_proto
from btrfs2s3._internal.btrfsioctl import opendir
from btrfs2s3._internal.btrfsioctl import send
from btrfs2s3._internal.btrfsioctl import subvol_info
from btrfs2s3._internal.btrfsioctl import SubvolInfo

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path
    from typing import IO


@pytest.fixture(params=[False, True], ids=["by-fd", "by-path"])
def by_path(request: pytest.FixtureRequest) -> bool:
    return bool(request.param)


@pytest.fixture(params=[False, True], ids=["dst-fd", "dst-stream"])
def dst_stream(request: pytest.FixtureRequest) -> bool:
    return bool(request.param)


@pytest.fixture
def dir_fd(btrfs_mountpoint: Path) -> Iterator[int]:
    with opendir(btrfs_mountpoint) as fd:
        yield fd


@pytest.fixture
def subvol_name(dir_fd: int) -> str:
    name = "subvol"
    create_subvol(name, dir_fd=dir_fd)
    return name


@pytest.fixture
def source_fd(dir_fd: int, subvol_name: str) -> Iterator[int]:
    with opendir(subvol_name, dir_fd=dir_fd) as fd:
        yield fd


@pytest.fixture
def subvol_path(subvol_name: str, btrfs_mountpoint: Path) -> Path:
    return btrfs_mountpoint / subvol_name


@pytest.fixture
def data_a() -> bytes:
    return randbytes(4096)


@pytest.fixture
def data_b() -> bytes:
    return randbytes(4096)


@pytest.fixture
def snapshot_1_name(
    subvol_path: Path, data_a: bytes, dir_fd: int, source_fd: int
) -> str:
    name = "snapshot1"
    (subvol_path / "a").write_bytes(data_a)
    create_snap(src=source_fd, dst=name, dst_dir_fd=dir_fd, read_only=True)
    return name


@pytest.fixture
def snapshot_2_name(
    subvol_path: Path,
    data_b: bytes,
    dir_fd: int,
    source_fd: int,
    snapshot_1_name: str,  # noqa: ARG001
) -> str:
    name = "snapshot2"
    (subvol_path / "b").write_bytes(data_b)
    create_snap(src=source_fd, dst=name, dst_dir_fd=dir_fd, read_only=True)
    return name


@pytest.fixture
def snapshot_2_path(
    btrfs_mountpoint: Path, snapshot_2_name: str, data_a: bytes, data_b: bytes
) -> Path:
    path = btrfs_mountpoint / snapshot_2_name
    assert (path / "a").read_bytes() == data_a
    assert (path / "b").read_bytes() == data_b
    return path


@pytest.fixture
def snapshot_1_fd(dir_fd: int, snapshot_1_name: str) -> Iterator[int]:
    with opendir(snapshot_1_name, dir_fd=dir_fd) as fd:
        yield fd


@pytest.fixture
def snapshot_1_info(snapshot_1_fd: int) -> SubvolInfo:
    return subvol_info(snapshot_1_fd)


@pytest.fixture
def snapshot_2_info(snapshot_2_fd: int) -> SubvolInfo:
    return subvol_info(snapshot_2_fd)


@pytest.fixture
def snapshot_2_fd(dir_fd: int, snapshot_2_name: str) -> Iterator[int]:
    with opendir(snapshot_2_name, dir_fd=dir_fd) as fd:
        yield fd


@pytest.fixture
def tempfp1() -> Iterator[IO[bytes]]:
    with NamedTemporaryFile() as fp:
        yield fp


@pytest.fixture
def tempfp2() -> Iterator[IO[bytes]]:
    with NamedTemporaryFile() as fp:
        yield fp


@pytest.fixture(params=["default", "latest"])
def proto(request: pytest.FixtureRequest) -> int | None:
    if request.param == "default":
        return None
    return 0


@pytest.fixture(params=["full", "differential"])
def differential(request: pytest.FixtureRequest) -> bool:
    return bool(request.param == "differential")


def test_send_can_be_restored_with_receive(
    snapshot_2_fd: int,
    tempfp1: IO[bytes],
    proto: int | None,
    snapshot_1_info: SubvolInfo,
    snapshot_2_info: SubvolInfo,
    dir_fd: int,
    btrfs_mountpoint: Path,
    snapshot_2_path: Path,
    data_a: bytes,
    data_b: bytes,
    differential: bool,  # noqa: FBT001
    by_path: bool,  # noqa: FBT001
    dst_stream: bool,  # noqa: FBT001
) -> None:
    if proto == 0 and get_userspace_send_proto() < get_kernel_send_proto():
        pytest.xfail(  # pragma: no cover
            f"can't test kernel's latest proto ({get_kernel_send_proto()}) because "
            f"userspace is too old (supports {get_userspace_send_proto()})"
        )
    src: int | Path = snapshot_2_path if by_path else snapshot_2_fd
    parent_id = snapshot_1_info.id if differential else 0
    dst: IO[bytes] | int = tempfp1 if dst_stream else tempfp1.fileno()
    send(src=src, dst=dst, proto=proto, parent_id=parent_id)

    if not differential:
        destroy_snap(dir_fd=dir_fd, snapshot_id=snapshot_1_info.id)
    destroy_snap(dir_fd=dir_fd, snapshot_id=snapshot_2_info.id)
    tempfp1.seek(0, os.SEEK_SET)
    check_call(["btrfs", "receive", btrfs_mountpoint], stdin=tempfp1, stdout=DEVNULL)

    assert (snapshot_2_path / "a").read_bytes() == data_a
    assert (snapshot_2_path / "b").read_bytes() == data_b


def test_full_appears_larger_than_differential(
    snapshot_2_fd: int,
    snapshot_2_path: Path,
    tempfp1: IO[bytes],
    tempfp2: IO[bytes],
    proto: int | None,
    snapshot_1_info: SubvolInfo,
    by_path: bool,  # noqa: FBT001
    dst_stream: bool,  # noqa: FBT001
) -> None:
    src: int | Path = snapshot_2_path if by_path else snapshot_2_fd
    dst1: IO[bytes] | int = tempfp1 if dst_stream else tempfp1.fileno()
    dst2: IO[bytes] | int = tempfp2 if dst_stream else tempfp2.fileno()
    send(src=src, dst=dst1, proto=proto)
    send(src=src, dst=dst2, proto=proto, parent_id=snapshot_1_info.id)

    assert tempfp1.seek(0, os.SEEK_END) > tempfp2.seek(0, os.SEEK_END)
