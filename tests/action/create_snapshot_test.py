# btrfs2s3 - maintains a tree of differential backups in object storage.
#
# Copyright (C) 2024 Steven Brudenell and other contributors.
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

from pathlib import Path

import btrfsutil

from btrfs2s3._internal.action import create_snapshot
from btrfs2s3._internal.util import SubvolumeFlags


def test_call(btrfs_mountpoint: Path) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    source_info = btrfsutil.subvolume_info(source)
    path = btrfs_mountpoint / "snapshot"

    create_snapshot(source=source, path=path)

    assert btrfsutil.is_subvolume(path)
    path_info = btrfsutil.subvolume_info(path)
    assert path_info.parent_uuid == source_info.uuid
    assert path_info.flags & SubvolumeFlags.ReadOnly
