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

from base64 import b64decode
import gzip
import subprocess
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError
from btrfs2s3._internal.action import create_backup
import btrfsutil
import pytest

if TYPE_CHECKING:
    from pathlib import Path

    from mypy_boto3_s3.client import S3Client

    from tests.conftest import DownloadAndPipe


def test_creates_a_valid_btrfs_archive(
    btrfs_mountpoint: Path,
    s3: S3Client,
    bucket: str,
    download_and_pipe: DownloadAndPipe,
) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)

    snapshot = btrfs_mountpoint / "snapshot"
    btrfsutil.create_snapshot(source, snapshot, read_only=True)

    key = "test-backup"

    create_backup(s3=s3, bucket=bucket, key=key, snapshot=snapshot, send_parent=None)

    # Just check the archive is valid
    download_and_pipe(key, ["btrfs", "receive", "--dump"])


def test_large_archive_multipart_upload(
    btrfs_mountpoint: Path,
    s3: S3Client,
    bucket: str,
    download_and_pipe: DownloadAndPipe,
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
    download_and_pipe(key, ["btrfs", "receive", "--dump"])


def test_send_full_and_delta_archives_and_restore(
    btrfs_mountpoint: Path,
    s3: S3Client,
    bucket: str,
    download_and_pipe: DownloadAndPipe,
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
    download_and_pipe(key1, ["btrfs", "receive", btrfs_mountpoint])
    download_and_pipe(key2, ["btrfs", "receive", btrfs_mountpoint])

    # The restored latest snapshot should contain the full set of data
    assert (snapshot2 / "large-file").read_bytes() == b"\xff" * (32 * 2**20)


def test_raise_error_on_send_failure(
    tmp_path: Path, capfd: pytest.CaptureFixture[str], s3: S3Client, bucket: str
) -> None:
    snapshot = tmp_path / "not-a-btrfs-subvolume"
    key = "test-backup"

    with pytest.raises(RuntimeError, match="exited with code "):
        create_backup(
            s3=s3, bucket=bucket, snapshot=snapshot, send_parent=None, key=key
        )
    out, err = capfd.readouterr()
    assert out == ""
    assert err != ""

    # Check delete
    with pytest.raises(ClientError):
        s3.head_object(Bucket=bucket, Key=key)


def test_pipe_success(btrfs_mountpoint: Path, s3: S3Client, bucket: str) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)

    snapshot = btrfs_mountpoint / "snapshot"
    btrfsutil.create_snapshot(source, snapshot, read_only=True)

    key = "test-backup"

    create_backup(
        s3=s3,
        bucket=bucket,
        key=key,
        snapshot=snapshot,
        send_parent=None,
        pipe_through=[["gzip"], ["base64"]],
    )

    # Just check the archive is valid
    compressed_encoded_data = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    data = gzip.decompress(b64decode(compressed_encoded_data))
    subprocess.run(["btrfs", "receive", "--dump"], input=data, check=True)


def test_end_of_pipe_fails(
    btrfs_mountpoint: Path, s3: S3Client, bucket: str, capfd: pytest.CaptureFixture[str]
) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)

    snapshot = btrfs_mountpoint / "snapshot"
    btrfsutil.create_snapshot(source, snapshot, read_only=True)

    key = "test-backup"

    with pytest.raises(RuntimeError, match="exited with code "):
        create_backup(
            s3=s3,
            bucket=bucket,
            snapshot=snapshot,
            send_parent=None,
            key=key,
            pipe_through=[["gzip"], ["base64", "--bad-option"]],
        )
    out, err = capfd.readouterr()
    assert out == ""
    assert err != ""

    # Check delete
    with pytest.raises(ClientError):
        s3.head_object(Bucket=bucket, Key=key)


def test_middle_of_pipe_fails(
    btrfs_mountpoint: Path, s3: S3Client, bucket: str, capfd: pytest.CaptureFixture[str]
) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)

    snapshot = btrfs_mountpoint / "snapshot"
    btrfsutil.create_snapshot(source, snapshot, read_only=True)

    key = "test-backup"

    with pytest.raises(RuntimeError, match="exited with code "):
        create_backup(
            s3=s3,
            bucket=bucket,
            snapshot=snapshot,
            send_parent=None,
            key=key,
            pipe_through=[["gzip", "--bad-option"], ["base64"]],
        )
    out, err = capfd.readouterr()
    assert out == ""
    assert err != ""

    # Check delete
    with pytest.raises(ClientError):
        s3.head_object(Bucket=bucket, Key=key)


def test_start_of_pipe_fails(
    tmp_path: Path, s3: S3Client, bucket: str, capfd: pytest.CaptureFixture[str]
) -> None:
    snapshot = tmp_path / "not-a-btrfs-subvolume"
    key = "test-backup"

    with pytest.raises(RuntimeError, match="exited with code "):
        create_backup(
            s3=s3,
            bucket=bucket,
            snapshot=snapshot,
            send_parent=None,
            key=key,
            pipe_through=[["gzip"], ["base64"]],
        )
    out, err = capfd.readouterr()
    assert out == ""
    assert err != ""

    # Check delete
    with pytest.raises(ClientError):
        s3.head_object(Bucket=bucket, Key=key)
