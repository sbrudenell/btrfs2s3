from __future__ import annotations

from subprocess import DEVNULL
from subprocess import PIPE
from subprocess import Popen
from typing import Sequence
from typing import TYPE_CHECKING

from btrfs2s3.action import create_backup
import btrfsutil
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


def test_creates_a_valid_btrfs_archive(
    btrfs_mountpoint: Path, s3: S3Client, bucket: str
) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)

    snapshot = btrfs_mountpoint / "snapshot"
    btrfsutil.create_snapshot(source, snapshot, read_only=True)

    key = "test-backup"

    create_backup(s3=s3, bucket=bucket, key=key, snapshot=snapshot, send_parent=None)

    # Just check the archive is valid
    download_and_pipe_to_command(s3, bucket, key, ["btrfs", "receive", "--dump"])


def test_large_archive_multipart_upload(
    btrfs_mountpoint: Path, s3: S3Client, bucket: str
) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)

    # The default multipart threshold is 8MB. So create a 16mb file
    (source / "large-file").write_bytes(b"\xff" * (16 * 2**20))

    snapshot = btrfs_mountpoint / "snapshot"
    btrfsutil.create_snapshot(source, snapshot, read_only=True)

    key = "test-backup"

    create_backup(s3=s3, bucket=bucket, snapshot=snapshot, send_parent=None, key=key)

    # Just check the archive is valid
    download_and_pipe_to_command(s3, bucket, key, ["btrfs", "receive", "--dump"])


def test_send_full_and_delta_archives_and_restore(
    btrfs_mountpoint: Path, s3: S3Client, bucket: str
) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    large_file = source / "large-file"

    # Write some data in the source
    large_file.write_bytes(b"\xff" * (16 * 2**20))
    # Create a snapshot
    snapshot1 = btrfs_mountpoint / "snapshot1"
    btrfsutil.create_snapshot(source, snapshot1, read_only=True)
    # Back up the first snapshot
    key1 = "test-backup1"
    create_backup(s3=s3, bucket=bucket, snapshot=snapshot1, send_parent=None, key=key1)

    # Write some more data in the source (appending this time)
    with large_file.open(mode="ab") as fp:
        fp.write(b"\xff" * (16 * 2**20))

    # Create another snapshot
    snapshot2 = btrfs_mountpoint / "snapshot2"
    btrfsutil.create_snapshot(source, snapshot2, read_only=True)
    # Back up the first snapshot, using the first as a send-parent
    key2 = "test-backup2"
    create_backup(
        s3=s3, bucket=bucket, snapshot=snapshot2, send_parent=snapshot1, key=key2
    )

    # Delete the snapshots and the source
    btrfsutil.delete_subvolume(source)
    btrfsutil.delete_subvolume(snapshot1)
    btrfsutil.delete_subvolume(snapshot2)

    # Restore the snapshots
    download_and_pipe_to_command(
        s3, bucket, key1, ["btrfs", "receive", btrfs_mountpoint]
    )
    download_and_pipe_to_command(
        s3, bucket, key2, ["btrfs", "receive", btrfs_mountpoint]
    )

    # The restored latest snapshot should contain the full set of data
    assert (snapshot2 / "large-file").read_bytes() == b"\xff" * (32 * 2**20)


def test_raise_error_on_send_failure(
    tmp_path: Path, capfd: pytest.CaptureFixture[str], s3: S3Client, bucket: str
) -> None:
    snapshot = tmp_path / "not-a-btrfs-subvolume"
    key = "test-backup"

    with pytest.raises(RuntimeError, match="'btrfs send' exited with code "):
        create_backup(
            s3=s3, bucket=bucket, snapshot=snapshot, send_parent=None, key=key
        )
    out, err = capfd.readouterr()
    assert out == ""
    assert err != ""
