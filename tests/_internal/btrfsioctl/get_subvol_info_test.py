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

from btrfs2s3._internal.btrfsioctl import FS_TREE_OBJECTID
from btrfs2s3._internal.btrfsioctl import NULL_UUID
from btrfs2s3._internal.btrfsioctl import opendir
from btrfs2s3._internal.btrfsioctl import subvol_info
from btrfs2s3._internal.btrfsioctl import UUID_SIZE

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(params=[False, True], ids=["by-fd", "by-path"])
def by_path(request: pytest.FixtureRequest) -> bool:
    return bool(request.param)


def test_valid_info_for_root(btrfs_mountpoint: Path, by_path: bool) -> None:  # noqa: FBT001
    if by_path:
        info = subvol_info(btrfs_mountpoint)
    else:
        with opendir(btrfs_mountpoint) as fd:
            info = subvol_info(fd)

    assert info.id == FS_TREE_OBJECTID
    assert info.name == ""
    assert info.parent_id == 0
    assert info.dir_id == 0
    assert info.generation > 0
    assert info.flags == 0
    assert info.uuid != NULL_UUID
    assert len(info.uuid) == UUID_SIZE
    assert info.parent_uuid is None
    assert info.received_uuid is None
    assert info.ctransid == 0
    assert info.otransid == 0
    assert info.stransid == 0
    assert info.rtransid == 0
    assert info.ctime > 0
    assert info.otime > 0
    assert info.stime == 0.0
    assert info.rtime == 0.0
