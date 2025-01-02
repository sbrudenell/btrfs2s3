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

from pathlib import Path

import pytest

from btrfs2s3._internal.backups import BackupInfo
from btrfs2s3._internal.btrfsioctl import create_snap
from btrfs2s3._internal.btrfsioctl import create_subvol
from btrfs2s3._internal.btrfsioctl import subvol_info
from btrfs2s3._internal.planner import Source
from btrfs2s3._internal.util import backup_of_snapshot


def test_properties(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "source"
    create_subvol(path)

    with Source.create(path) as source:
        assert source.path == path
        assert source.info.parent_uuid is None
        assert Path(f"/proc/self/fd/{source.fd}").readlink() == path


def test_close(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "source"
    create_subvol(path)

    source = Source.create(path)
    source.close()


def test_close_as_context_manager(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "source"
    create_subvol(path)

    with Source.create(path) as source:
        assert source.path == path


def test_not_a_subvol(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "source"
    path.mkdir()

    with pytest.raises(ValueError, match="not a subvolume"):
        Source.create(path)


def test_get_new_snapshot_name(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "source"
    create_subvol(path)

    with Source.create(path) as source:
        name = source.get_new_snapshot_name()

    assert name.startswith(path.name)


def test_get_snapshot_name(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "source"
    create_subvol(path)
    snapshot_path = btrfs_mountpoint / "snapshot"
    create_snap(src=path, dst=snapshot_path, read_only=True)
    snapshot_info = subvol_info(snapshot_path)

    with Source.create(path) as source:
        name = source.get_snapshot_name(snapshot_info)

    assert name.startswith(path.name)


def test_get_backup_key(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "source"
    create_subvol(path)
    snapshot_path = btrfs_mountpoint / "snapshot"
    create_snap(src=path, dst=snapshot_path, read_only=True)
    snapshot_info = subvol_info(snapshot_path)
    backup_info = backup_of_snapshot(snapshot_info)

    with Source.create(path) as source:
        key = source.get_backup_key(backup_info)

    assert key.startswith(path.name)
    assert BackupInfo.from_path(key) == backup_info
