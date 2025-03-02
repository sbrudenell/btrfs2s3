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

from __future__ import annotations

import functools
from typing import TYPE_CHECKING

import arrow
from botocore.exceptions import ClientError
import btrfsutil
from btrfsutil import SubvolumeInfo
import pytest

from btrfs2s3._internal.action import Actions
from btrfs2s3._internal.util import backup_of_snapshot

if TYPE_CHECKING:
    from pathlib import Path

    from mypy_boto3_s3.client import S3Client

    from tests.conftest import DownloadAndPipe


def test_empty() -> None:
    actions = Actions()
    assert actions.empty()


def test_create_and_rename(btrfs_mountpoint: Path, s3: S3Client, bucket: str) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    (source / "dummy-file").write_bytes(b"dummy")

    actions = Actions()

    initial_path = btrfs_mountpoint / "snapshot"

    @functools.lru_cache
    def get_info() -> SubvolumeInfo:
        return btrfsutil.subvolume_info(initial_path)

    def get_target_path() -> Path:
        ctime = arrow.get(get_info().ctime)
        return btrfs_mountpoint / f"snapshot-{ctime.isoformat()}"

    actions.create_snapshot(source=source, path=initial_path)
    actions.rename_snapshot(source=initial_path, target=get_target_path)
    assert not actions.empty()

    actions.execute(s3, bucket)

    # Ensure the snapshot was created and renamed
    assert (get_target_path() / "dummy-file").read_bytes() == b"dummy"


def test_create_rename_backup(
    btrfs_mountpoint: Path,
    s3: S3Client,
    bucket: str,
    download_and_pipe: DownloadAndPipe,
) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    (source / "dummy-file").write_bytes(b"dummy")
    snapshot1 = btrfs_mountpoint / "snapshot1"
    btrfsutil.create_snapshot(source, snapshot1, read_only=True)
    (source / "dummy-file").write_bytes(b"dummy2")

    actions = Actions()

    initial_path = btrfs_mountpoint / "snapshot2"

    @functools.lru_cache
    def get_info() -> SubvolumeInfo:
        return btrfsutil.subvolume_info(initial_path)

    def get_target_path() -> Path:
        ctime = arrow.get(get_info().ctime)
        return btrfs_mountpoint / f"snapshot-{ctime.isoformat()}"

    def get_send_parent() -> Path | None:
        return snapshot1

    def get_key() -> str:
        backup = backup_of_snapshot(
            get_info(), send_parent=btrfsutil.subvolume_info(snapshot1)
        )
        return f"{source.name}{''.join(backup.get_path_suffixes())}"

    actions.create_snapshot(source=source, path=initial_path)
    actions.rename_snapshot(source=initial_path, target=get_target_path)
    actions.create_backup(
        source=source,
        snapshot=get_target_path,
        send_parent=get_send_parent,
        key=get_key,
    )
    assert not actions.empty()

    actions.execute(s3, bucket)

    # Ensure the snapshot was created and renamed
    assert (get_target_path() / "dummy-file").read_bytes() == b"dummy2"

    # Test the backup exists and is a valid btrfs archive
    download_and_pipe(get_key(), ["btrfs", "receive", "--dump"])


def test_other_actions(
    btrfs_mountpoint: Path,
    s3: S3Client,
    bucket: str,
    download_and_pipe: DownloadAndPipe,
) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)

    def mksnapshot(name: str) -> Path:
        path = btrfs_mountpoint / name
        btrfsutil.create_snapshot(source, path, read_only=True)
        return path

    actions = Actions()

    # Rename a snapshot
    rename_me = mksnapshot("rename-me")
    rename_to = rename_me.parent / "rename-to"
    actions.rename_snapshot(source=rename_me, target=rename_to)

    # Delete a snapshot
    delete_me = mksnapshot("delete-me")
    actions.delete_snapshot(delete_me)

    # Create a backup
    backup_of_me = mksnapshot("backup-of-me")
    backup_of_me_key = "backup-key"
    actions.create_backup(
        source=source, snapshot=backup_of_me, send_parent=None, key=backup_of_me_key
    )

    # Delete a backup
    delete_me_key = "delete-me-key"
    s3.put_object(Bucket=bucket, Key=delete_me_key, Body=b"dummy")
    actions.delete_backup(delete_me_key)
    assert not actions.empty()

    actions.execute(s3, bucket)

    # Check rename
    assert not rename_me.exists()
    assert rename_to.exists()

    # Check delete
    assert not delete_me.exists()

    # Check backup
    download_and_pipe(backup_of_me_key, ["btrfs", "receive", "--dump"])

    # Check delete
    with pytest.raises(ClientError):
        s3.head_object(Bucket=bucket, Key=delete_me_key)
