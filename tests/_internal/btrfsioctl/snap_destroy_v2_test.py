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

from typing import TYPE_CHECKING

import pytest

from btrfs2s3._internal.btrfsioctl import create_snap
from btrfs2s3._internal.btrfsioctl import create_subvol
from btrfs2s3._internal.btrfsioctl import destroy_snap
from btrfs2s3._internal.btrfsioctl import opendir
from btrfs2s3._internal.btrfsioctl import subvol_info
from btrfs2s3._internal.btrfsioctl import SubvolInfo

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


@pytest.fixture(params=[False, True], ids=["use-dir-fd", "use-full-path"])
def use_dir_fd(request: pytest.FixtureRequest) -> bool:
    return bool(request.param)


@pytest.fixture(params=[False, True], ids=["use-name", "use-snapshot-id"])
def use_snapshot_id(request: pytest.FixtureRequest) -> bool:
    return bool(request.param)


@pytest.fixture
def dir_fd(btrfs_mountpoint: Path) -> Iterator[int]:
    with opendir(btrfs_mountpoint) as fd:
        yield fd


@pytest.fixture
def snapshot(dir_fd: int) -> tuple[str, SubvolInfo]:
    subvol_name = "subvol"
    snap_name = "snapshot"

    create_subvol(subvol_name, dir_fd=dir_fd)
    with opendir(subvol_name, dir_fd=dir_fd) as source_fd:
        create_snap(src=source_fd, dst=snap_name, dst_dir_fd=dir_fd, read_only=True)

    with opendir(snap_name, dir_fd=dir_fd) as snap_fd:
        snap_info = subvol_info(snap_fd)

    return snap_name, snap_info


def test_destroy_read_only_snapshot(
    btrfs_mountpoint: Path,
    dir_fd: int,
    snapshot: tuple[str, SubvolInfo],
    use_dir_fd: bool,  # noqa: FBT001
    use_snapshot_id: bool,  # noqa: FBT001
) -> None:
    snap_name, snap_info = snapshot

    if use_dir_fd:
        if use_snapshot_id:
            destroy_snap(dir_fd=dir_fd, snapshot_id=snap_info.id)
        else:
            destroy_snap(snap_name, dir_fd=dir_fd)
    elif use_snapshot_id:
        destroy_snap(btrfs_mountpoint, snapshot_id=snap_info.id)
    else:
        destroy_snap(btrfs_mountpoint / snap_name)

    assert not (btrfs_mountpoint / snap_name).exists()
