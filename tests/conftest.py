from __future__ import annotations

import os
from pathlib import Path
import subprocess
from subprocess import DEVNULL
from subprocess import PIPE
from subprocess import Popen
import tempfile
from typing import Protocol
from typing import TYPE_CHECKING

import boto3
from moto import mock_aws
import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator
    from collections.abc import Sequence

    from mypy_boto3_s3.client import S3Client


@pytest.fixture(autouse=True, scope="session")
def _aws_credentials() -> None:
    # Always stub these out for testing
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture()
def _aws(_aws_credentials: None) -> Iterator[None]:
    with mock_aws():
        yield


@pytest.fixture()
def btrfs_mountpoint() -> Iterator[Path]:
    with tempfile.NamedTemporaryFile() as loop_file:
        loop_file.truncate(2**30)
        subprocess.check_call(["mkfs.btrfs", "-q", loop_file.name])
        with tempfile.TemporaryDirectory() as mount_temp_dir:
            subprocess.check_call(["mount", loop_file.name, mount_temp_dir])
            try:
                yield Path(mount_temp_dir)
            finally:
                subprocess.check_call(["umount", mount_temp_dir])


@pytest.fixture()
def ext4_mountpoint() -> Iterator[Path]:
    with tempfile.NamedTemporaryFile() as loop_file:
        loop_file.truncate(2**30)
        subprocess.check_call(["mkfs.ext4", "-q", loop_file.name])
        with tempfile.TemporaryDirectory() as mount_temp_dir:
            subprocess.check_call(["mount", loop_file.name, mount_temp_dir])
            try:
                yield Path(mount_temp_dir)
            finally:
                subprocess.check_call(["umount", mount_temp_dir])


@pytest.fixture()
def s3(_aws: None) -> S3Client:
    return boto3.client("s3")


@pytest.fixture()
def bucket(s3: S3Client) -> str:
    s3.create_bucket(Bucket="test-bucket")
    return "test-bucket"


class DownloadAndPipe(Protocol):
    def __call__(self, key: str, args: Sequence[str | Path]) -> int: ...


@pytest.fixture()
def download_and_pipe(s3: S3Client, bucket: str) -> DownloadAndPipe:
    def inner(key: str, args: Sequence[str | Path]) -> int:
        process = Popen(args, stdin=PIPE, stdout=DEVNULL)
        # https://github.com/python/typeshed/issues/3831
        assert process.stdin is not None
        s3.download_fileobj(bucket, key, process.stdin)
        # download_fileobj doesn't close its target
        process.stdin.close()
        assert process.wait() == 0
        return process.wait()

    return inner


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if "btrfs_mountpoint" in item.fixturenames:  # type: ignore[attr-defined]
            item.add_marker("root")
