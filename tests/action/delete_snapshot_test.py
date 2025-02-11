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

import os
from pathlib import Path
import subprocess

from btrfs2s3._internal.action import delete_snapshot
import btrfsutil
import pytest


def test_call(btrfs_mountpoint: Path) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    snapshot = btrfs_mountpoint / "snapshot"
    btrfsutil.create_snapshot(source, snapshot, read_only=True)

    delete_snapshot(snapshot)

    assert not snapshot.exists()


def test_not_a_subvolume(btrfs_mountpoint: Path) -> None:
    source = btrfs_mountpoint / "source"
    source.mkdir()

    with pytest.raises(RuntimeError, match="target isn't a subvolume"):
        delete_snapshot(source)


def test_not_a_snapshot(btrfs_mountpoint: Path) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)

    with pytest.raises(RuntimeError, match="target isn't a snapshot"):
        delete_snapshot(source)


def test_not_a_read_only_snapshot(btrfs_mountpoint: Path) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    snapshot = btrfs_mountpoint / "snapshot"
    btrfsutil.create_snapshot(source, snapshot, read_only=False)

    with pytest.raises(RuntimeError, match="target isn't a read-only snapshot"):
        delete_snapshot(snapshot)


def test_delete_as_normal_user(btrfs_mountpoint: Path) -> None:
    # This test is narrow. We don't have full test coverage for non-root
    # operation currently. See https://github.com/sbrudenell/btrfs2s3/issues/49

    subprocess.check_call(
        ["mount", "-o", "remount,user_subvol_rm_allowed", btrfs_mountpoint]
    )

    uid = 1000
    os.chown(btrfs_mountpoint, uid, 0)
    os.seteuid(1000)
    try:
        source = btrfs_mountpoint / "source"
        btrfsutil.create_subvolume(source)
        snapshot = btrfs_mountpoint / "snapshot"
        btrfsutil.create_snapshot(source, snapshot, read_only=True)
        delete_snapshot(snapshot)
    finally:
        os.seteuid(0)
