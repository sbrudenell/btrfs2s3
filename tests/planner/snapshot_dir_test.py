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

from errno import ENOTTY
import os
from subprocess import check_call
from tempfile import TemporaryFile
from typing import TYPE_CHECKING

import pytest

from btrfs2s3._internal.btrfsioctl import create_snap
from btrfs2s3._internal.btrfsioctl import create_subvol
from btrfs2s3._internal.btrfsioctl import subvol_info
from btrfs2s3._internal.planner import SnapshotDir
from btrfs2s3._internal.planner import Source

if TYPE_CHECKING:
    from pathlib import Path


def test_context_manager(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "snapshots"
    path.mkdir()

    with SnapshotDir.create(path):
        pass


def test_close(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "snapshots"
    path.mkdir()

    snapshot_dir = SnapshotDir.create(path)
    snapshot_dir.close()


def test_empty(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "snapshots"
    path.mkdir()

    with SnapshotDir.create(path):
        pass


def test_properties(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "snapshots"
    path.mkdir()

    with SnapshotDir.create(path) as snapshot_dir:
        assert snapshot_dir.path == path


def test_path_does_not_exist(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "snapshots"

    with pytest.raises(FileNotFoundError):
        SnapshotDir.create(path)


def test_not_a_btrfs_directory(ext4_mountpoint: Path) -> None:
    with pytest.raises(OSError, match="Inappropriate ioctl for device") as exc_info:
        SnapshotDir.create(ext4_mountpoint)
    assert exc_info.value.errno == ENOTTY


def test_get_name(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "snapshots"
    path.mkdir()
    source_path = btrfs_mountpoint / "source"
    create_subvol(source_path)
    snap_path = path / "snapshot"
    create_snap(src=source_path, dst=snap_path, read_only=True)
    snap_info = subvol_info(snap_path)

    with SnapshotDir.create(path) as snapshot_dir:
        assert snapshot_dir.get_name(snap_info.id) == "snapshot"


def test_get_path(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "snapshots"
    path.mkdir()
    source_path = btrfs_mountpoint / "source"
    create_subvol(source_path)
    snap_path = path / "snapshot"
    create_snap(src=source_path, dst=snap_path, read_only=True)
    snap_info = subvol_info(snap_path)

    with SnapshotDir.create(path) as snapshot_dir:
        assert snapshot_dir.get_path(snap_info.id) == snap_path


def test_get_snapshots(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "snapshots"
    path.mkdir()
    source_path = btrfs_mountpoint / "source"
    create_subvol(source_path)
    snap_path = path / "snapshot"
    create_snap(src=source_path, dst=snap_path, read_only=True)
    snap_info = subvol_info(snap_path)
    unrelated_source_path = path / "unrelated"
    create_subvol(unrelated_source_path)

    with SnapshotDir.create(path) as snapshot_dir:
        with Source.create(source_path) as source:
            assert snapshot_dir.get_snapshots(source) == {snap_info.uuid: snap_info}
        with Source.create(unrelated_source_path) as unrelated_source:
            assert snapshot_dir.get_snapshots(unrelated_source) == {}


def test_ignore_read_write_snapshots(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "snapshots"
    path.mkdir()
    source_path = btrfs_mountpoint / "source"
    create_subvol(source_path)
    read_write_snap_path = path / "read-write-snapshot"
    create_snap(src=source_path, dst=read_write_snap_path)
    read_only_snap_path = path / "read-only-snapshot"
    create_snap(src=source_path, dst=read_only_snap_path, read_only=True)
    read_only_snap_info = subvol_info(read_only_snap_path)

    with SnapshotDir.create(path) as snapshot_dir:
        with Source.create(source_path) as source:
            assert snapshot_dir.get_snapshots(source) == {
                read_only_snap_info.uuid: read_only_snap_info
            }


def test_ignore_nested_snapshots(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "snapshots"
    path.mkdir()
    source_path = btrfs_mountpoint / "source"
    create_subvol(source_path)
    snap_path = path / "snapshot"
    create_snap(src=source_path, dst=snap_path, read_only=True)
    snap_info = subvol_info(snap_path)
    nested_path = path / "nested"
    nested_path.mkdir()
    nested_snap_path = nested_path / "nested-snapshot"
    create_snap(src=source_path, dst=nested_snap_path, read_only=True)

    with SnapshotDir.create(path) as snapshot_dir:
        with Source.create(source_path) as source:
            assert snapshot_dir.get_snapshots(source) == {snap_info.uuid: snap_info}


def test_ignore_subvolumes(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "snapshots"
    path.mkdir()
    source_path = btrfs_mountpoint / "source"
    create_subvol(source_path)
    snap_path = path / "snapshot"
    create_snap(src=source_path, dst=snap_path, read_only=True)
    snap_info = subvol_info(snap_path)
    other_subvol_path = path / "other-subvol"
    create_subvol(other_subvol_path)

    with SnapshotDir.create(path) as snapshot_dir:
        with Source.create(source_path) as source:
            assert snapshot_dir.get_snapshots(source) == {snap_info.uuid: snap_info}


def test_create_snapshot(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "snapshots"
    path.mkdir()
    source_path = btrfs_mountpoint / "source"
    create_subvol(source_path)
    snap_path = path / "snapshot"
    create_snap(src=source_path, dst=snap_path, read_only=True)
    snap_info = subvol_info(snap_path)

    with SnapshotDir.create(path) as snapshot_dir:
        with Source.create(source_path) as source:
            snapshot_dir.create_snapshot(source, "new-snapshot")
            new_snap_info = subvol_info(path / "new-snapshot")

            assert snapshot_dir.get_snapshots(source) == {
                snap_info.uuid: snap_info,
                new_snap_info.uuid: new_snap_info,
            }

            snapshot_dir.destroy_snapshot(new_snap_info.id)

            assert not (path / "new-snapshot").exists()
            assert snapshot_dir.get_snapshots(source) == {snap_info.uuid: snap_info}


def test_destroy_snapshot(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "snapshots"
    path.mkdir()
    source_path = btrfs_mountpoint / "source"
    create_subvol(source_path)
    snap_path = path / "snapshot"
    create_snap(src=source_path, dst=snap_path, read_only=True)
    snap_info = subvol_info(snap_path)

    with SnapshotDir.create(path) as snapshot_dir:
        with Source.create(source_path) as source:
            snapshot_dir.destroy_snapshot(snap_info.id)
            assert snapshot_dir.get_snapshots(source) == {}


def test_rename_snapshot(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "snapshots"
    path.mkdir()
    source_path = btrfs_mountpoint / "source"
    create_subvol(source_path)
    snap_path = path / "snapshot"
    create_snap(src=source_path, dst=snap_path, read_only=True)
    snap_info = subvol_info(snap_path)

    with SnapshotDir.create(path) as snapshot_dir:
        with Source.create(source_path) as source:
            snapshot_dir.rename_snapshot(snap_info.id, "new-name")

            assert snapshot_dir.get_name(snap_info.id) == "new-name"
            assert subvol_info(path / "new-name").id == snap_info.id
            assert snapshot_dir.get_snapshots(source) == {snap_info.uuid: snap_info}


def test_send(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "snapshots"
    path.mkdir()
    source_path = btrfs_mountpoint / "source"
    create_subvol(source_path)
    snap_path = path / "snapshot"
    create_snap(src=source_path, dst=snap_path, read_only=True)
    snap_info = subvol_info(snap_path)

    with TemporaryFile(buffering=0) as tempfp:
        with SnapshotDir.create(path) as snapshot_dir:
            snapshot_dir.send(dst=tempfp, snapshot_id=snap_info.id)
        tempfp.seek(0, os.SEEK_SET)
        check_call(["btrfs", "receive", "--dump"], stdin=tempfp)


def test_send_moved_snapshot(btrfs_mountpoint: Path) -> None:
    path = btrfs_mountpoint / "snapshots"
    path.mkdir()
    source_path = btrfs_mountpoint / "source"
    create_subvol(source_path)
    snap_path = path / "snapshot"
    create_snap(src=source_path, dst=snap_path, read_only=True)
    snap_info = subvol_info(snap_path)
    snap2_path = path / "snapshot2"
    create_snap(src=source_path, dst=snap2_path, read_only=True)

    with TemporaryFile(buffering=0) as tempfp:
        with SnapshotDir.create(path) as snapshot_dir:
            snap_path.rename(path / "snapshot3")
            snap2_path.rename(path / "snapshot")
            with pytest.raises(RuntimeError, match="snapshot moved or renamed"):
                snapshot_dir.send(dst=tempfp, snapshot_id=snap_info.id)
