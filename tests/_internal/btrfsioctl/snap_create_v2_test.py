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
from btrfs2s3._internal.btrfsioctl import opendir
from btrfs2s3._internal.btrfsioctl import subvol_info
from btrfs2s3._internal.btrfsioctl import SubvolFlag

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(params=[False, True], ids=["by-fd", "by-path"])
def by_path(request: pytest.FixtureRequest) -> bool:
    return bool(request.param)


@pytest.fixture(params=["read-only", "read-write"])
def read_only(request: pytest.FixtureRequest) -> bool:
    return bool(request.param == "read-only")


def test_create_snapshot(
    btrfs_mountpoint: Path,
    read_only: bool,  # noqa: FBT001
    by_path: bool,  # noqa: FBT001
) -> None:
    subvol_name = "subvol"
    snapshot_name = "snapshot"

    if by_path:
        create_subvol(btrfs_mountpoint / subvol_name)
        source_info = subvol_info(btrfs_mountpoint / subvol_name)
        create_snap(
            src=btrfs_mountpoint / subvol_name,
            dst=btrfs_mountpoint / snapshot_name,
            read_only=read_only,
        )
        info = subvol_info(btrfs_mountpoint / snapshot_name)
    else:
        with opendir(btrfs_mountpoint) as dir_fd:
            create_subvol(subvol_name, dir_fd=dir_fd)
            with opendir(subvol_name, dir_fd=dir_fd) as source_fd:
                source_info = subvol_info(source_fd)
                create_snap(
                    src=source_fd,
                    dst=snapshot_name,
                    dst_dir_fd=dir_fd,
                    read_only=read_only,
                )
            with opendir(snapshot_name, dir_fd=dir_fd) as fd:
                info = subvol_info(fd)

    assert info.name == snapshot_name
    if read_only:
        assert info.flags & SubvolFlag.ReadOnly
    assert info.parent_uuid == source_info.uuid
