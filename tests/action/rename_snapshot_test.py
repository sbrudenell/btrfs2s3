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

from btrfs2s3._internal.action import rename_snapshot
import btrfsutil


def test_call(btrfs_mountpoint: Path) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    snapshot = btrfs_mountpoint / "snapshot"
    btrfsutil.create_snapshot(source, snapshot, read_only=True)

    target = btrfs_mountpoint / "new-snapshot"

    rename_snapshot(source=snapshot, target=target)

    assert not snapshot.exists()
    assert target.exists()
