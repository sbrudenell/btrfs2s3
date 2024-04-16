from __future__ import annotations

import functools
from subprocess import DEVNULL
from subprocess import PIPE
from subprocess import Popen
from typing import Sequence
from typing import TYPE_CHECKING

import arrow
from botocore.exceptions import ClientError
from btrfs2s3._internal.util import backup_of_snapshot
from btrfs2s3.action import Plan
import btrfsutil
from btrfsutil import SubvolumeInfo
import pytest

if TYPE_CHECKING:
    from pathlib import Path

    from mypy_boto3_s3.client import S3Client


def download_and_pipe_to_command(
    s3: S3Client, bucket: str, key: str, args: Sequence[str | Path]
) -> None:
    process = Popen(args, stdin=PIPE, stdout=DEVNULL)
    # https://github.com/python/typeshed/issues/3831
    assert process.stdin is not None
    s3.download_fileobj(bucket, key, process.stdin)
    # download_fileobj doesn't close its target
    process.stdin.close()
    assert process.wait() == 0


def test_create_rename_backup(
    btrfs_mountpoint: Path, s3: S3Client, bucket: str
) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    (source / "dummy-file").write_bytes(b"dummy")
    snapshot1 = btrfs_mountpoint / "snapshot1"
    btrfsutil.create_snapshot(source, snapshot1, read_only=True)
    (source / "dummy-file").write_bytes(b"dummy2")

    plan = Plan()

    initial_target = btrfs_mountpoint / "snapshot2"

    @functools.cache
    def get_info() -> SubvolumeInfo:
        return btrfsutil.subvolume_info(initial_target)

    def get_target_path() -> Path:
        ctime = arrow.get(get_info().ctime)
        return btrfs_mountpoint / f"snapshot-{ctime.isoformat()}"

    def get_send_parent() -> Path | None:
        return snapshot1

    @functools.cache
    def get_key() -> str:
        backup = backup_of_snapshot(
            get_info(), send_parent=btrfsutil.subvolume_info(snapshot1)
        )
        return f"{source.name}{''.join(backup.get_path_suffixes())}"

    plan.create_rename_backup(
        source=source,
        initial_path=initial_target,
        get_target_path=get_target_path,
        get_send_parent=get_send_parent,
        get_key=get_key,
    )
    plan.execute(s3, bucket)

    # Ensure the snapshot was created and renamed
    assert (get_target_path() / "dummy-file").read_bytes() == b"dummy2"

    # Test the backup exists and is a valid btrfs archive
    download_and_pipe_to_command(s3, bucket, get_key(), ["btrfs", "receive", "--dump"])


def test_other_actions(btrfs_mountpoint: Path, s3: S3Client, bucket: str) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)

    def mksnapshot(name: str) -> Path:
        path = btrfs_mountpoint / name
        btrfsutil.create_snapshot(source, path, read_only=True)
        return path

    plan = Plan()

    # Rename a snapshot
    rename_me = mksnapshot("rename-me")
    rename_to = rename_me.parent / "rename-to"
    plan.rename_snapshot(source=rename_me, target=rename_to)

    # Delete a snapshot
    delete_me = mksnapshot("delete-me")
    plan.delete_snapshot(delete_me)

    # Create a backup
    backup_of_me = mksnapshot("backup-of-me")
    backup_of_me_key = "backup-key"
    plan.create_backup(
        source=source, snapshot=backup_of_me, send_parent=None, key=backup_of_me_key
    )

    # Delete a backup
    delete_me_key = "delete-me-key"
    s3.put_object(Bucket=bucket, Key=delete_me_key, Body=b"dummy")
    plan.delete_backup(delete_me_key)

    plan.execute(s3, bucket)

    # Check rename
    assert not rename_me.exists()
    assert rename_to.exists()

    # Check delete
    assert not delete_me.exists()

    # Check backup
    download_and_pipe_to_command(
        s3, bucket, backup_of_me_key, ["btrfs", "receive", "--dump"]
    )

    # Check delete
    with pytest.raises(ClientError):
        s3.head_object(Bucket=bucket, Key=delete_me_key)
