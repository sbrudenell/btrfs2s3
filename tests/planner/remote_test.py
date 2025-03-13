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

from functools import partial
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError
from botocore.exceptions import ParamValidationError
import pytest

from btrfs2s3._internal.backups import BackupInfo
from btrfs2s3._internal.btrfsioctl import create_snap
from btrfs2s3._internal.btrfsioctl import create_subvol
from btrfs2s3._internal.btrfsioctl import subvol_info
from btrfs2s3._internal.piper import filter_pipe
from btrfs2s3._internal.planner import BackupObject
from btrfs2s3._internal.planner import ObjectStat
from btrfs2s3._internal.planner import Remote
from btrfs2s3._internal.planner import SnapshotDir
from btrfs2s3._internal.planner import Source
from btrfs2s3._internal.util import backup_of_snapshot

if TYPE_CHECKING:
    from pathlib import Path

    from types_boto3_s3.client import S3Client

    from tests.conftest import DownloadAndPipe

noop_pipe = partial(filter_pipe, [])


def test_properties(s3: S3Client, bucket: str) -> None:
    remote = Remote.create(name="test", s3=s3, bucket=bucket)

    assert remote.name == "test"
    assert remote.s3 == s3
    assert remote.bucket == bucket


def test_get_objects(btrfs_mountpoint: Path, s3: S3Client, bucket: str) -> None:
    source_path = btrfs_mountpoint / "source"
    create_subvol(source_path)
    source_info = subvol_info(source_path)
    unrelated_source_path = btrfs_mountpoint / "unrelated"
    create_subvol(unrelated_source_path)
    snapshot_dir_path = btrfs_mountpoint / "snapshots"
    snapshot_dir_path.mkdir()
    snap_path = snapshot_dir_path / "snapshot"
    create_snap(src=source_path, dst=snap_path, read_only=True)
    snap_info = subvol_info(snap_path)
    info = BackupInfo(
        uuid=snap_info.uuid,
        parent_uuid=source_info.uuid,
        send_parent_uuid=None,
        ctransid=1,
        ctime=123.0,
    )
    key = f"test{''.join(info.get_path_suffixes())}"
    s3.put_object(Bucket=bucket, Key=key, Body=b"dummy")

    remote = Remote.create(name="test", s3=s3, bucket=bucket)

    with Source.create(source_path) as source:
        assert remote.get_objects(source) == {
            info.uuid: BackupObject(
                key=key, info=info, stat=ObjectStat(size=5, storage_class="STANDARD")
            )
        }
    with Source.create(unrelated_source_path) as unrelated_source:
        assert remote.get_objects(unrelated_source) == {}


def test_upload(
    btrfs_mountpoint: Path,
    s3: S3Client,
    bucket: str,
    download_and_pipe: DownloadAndPipe,
) -> None:
    snapshot_dir_path = btrfs_mountpoint / "snapshots"
    snapshot_dir_path.mkdir()
    source_path = btrfs_mountpoint / "source"
    create_subvol(source_path)
    snap_path = snapshot_dir_path / "snapshot"
    create_snap(src=source_path, dst=snap_path, read_only=True)
    snap_info = subvol_info(snap_path)
    backup_info = backup_of_snapshot(snap_info)
    key = f"test{''.join(backup_info.get_path_suffixes())}"
    remote = Remote.create(name="test", s3=s3, bucket=bucket)

    with SnapshotDir.create(snapshot_dir_path) as snapshot_dir:
        stat = remote.upload(
            snapshot_dir=snapshot_dir,
            snapshot_id=snap_info.id,
            send_parent_id=None,
            key=key,
            create_pipe=noop_pipe,
        )

    assert stat == ObjectStat()
    download_and_pipe(key, ["btrfs", "receive", "--dump"])

    with Source.create(source_path) as source:
        assert remote.get_objects(source) == {
            snap_info.uuid: BackupObject(key=key, info=backup_info, stat=ObjectStat())
        }


def test_upload_send_fails(btrfs_mountpoint: Path, s3: S3Client, bucket: str) -> None:
    snapshot_dir_path = btrfs_mountpoint / "snapshots"
    snapshot_dir_path.mkdir()
    remote = Remote.create(name="test", s3=s3, bucket=bucket)

    with SnapshotDir.create(snapshot_dir_path) as snapshot_dir:
        with pytest.raises(KeyError, match="-123"):
            remote.upload(
                snapshot_dir=snapshot_dir,
                snapshot_id=-123,
                send_parent_id=None,
                key="test-key",
                create_pipe=noop_pipe,
            )

    # Ensure dummy backup was deleted
    with pytest.raises(ClientError):
        s3.head_object(Bucket=bucket, Key="test-key")


def test_upload_put_object_fails(
    btrfs_mountpoint: Path, s3: S3Client, bucket: str
) -> None:
    snapshot_dir_path = btrfs_mountpoint / "snapshots"
    snapshot_dir_path.mkdir()
    source_path = btrfs_mountpoint / "source"
    create_subvol(source_path)
    snap_path = snapshot_dir_path / "snapshot"
    create_snap(src=source_path, dst=snap_path, read_only=True)
    snap_info = subvol_info(snap_path)
    remote = Remote.create(name="test", s3=s3, bucket=bucket)

    with SnapshotDir.create(snapshot_dir_path) as snapshot_dir:
        with pytest.raises(ParamValidationError):
            remote.upload(
                snapshot_dir=snapshot_dir,
                snapshot_id=snap_info.id,
                send_parent_id=None,
                key="",
                create_pipe=noop_pipe,
            )


def test_delete(btrfs_mountpoint: Path, s3: S3Client, bucket: str) -> None:
    snapshot_dir_path = btrfs_mountpoint / "snapshots"
    snapshot_dir_path.mkdir()
    source_path = btrfs_mountpoint / "source"
    create_subvol(source_path)
    snap_path = snapshot_dir_path / "snapshot"
    create_snap(src=source_path, dst=snap_path, read_only=True)
    snap_info = subvol_info(snap_path)
    backup_info = backup_of_snapshot(snap_info)
    key = f"test{''.join(backup_info.get_path_suffixes())}"
    remote = Remote.create(name="test", s3=s3, bucket=bucket)

    with SnapshotDir.create(snapshot_dir_path) as snapshot_dir:
        remote.upload(
            snapshot_dir=snapshot_dir,
            snapshot_id=snap_info.id,
            send_parent_id=None,
            key=key,
            create_pipe=noop_pipe,
        )

    remote.delete(key)

    with Source.create(source_path) as source:
        assert remote.get_objects(source) == {}
    with pytest.raises(ClientError):
        s3.head_object(Bucket=bucket, Key=key)
