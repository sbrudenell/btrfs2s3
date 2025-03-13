# btrfs2s3 - maintains a tree of differential backups in object storage.
#
# Copyright (C) 2024-2025 Steven Brudenell and other contributors.
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

from datetime import timezone
from io import StringIO
import os
from pathlib import Path
import subprocess
from subprocess import DEVNULL
from subprocess import PIPE
from subprocess import Popen
import tempfile
from typing import Protocol
from typing import TYPE_CHECKING
from warnings import warn

import boto3
from moto import mock_aws
import pytest
from rich.console import Console

from btrfs2s3._internal.console import THEME
from btrfs2s3._internal.cvar import use_tzinfo

if TYPE_CHECKING:
    from collections.abc import Iterator
    from collections.abc import Sequence
    from typing import IO

    from types_boto3_s3.client import S3Client


@pytest.fixture(autouse=True, scope="session")
def _aws_credentials() -> None:
    # Always stub these out for testing
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def _aws(_aws_credentials: None) -> Iterator[None]:
    with mock_aws():
        yield


@pytest.fixture
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


@pytest.fixture
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


@pytest.fixture
def s3(_aws: None) -> S3Client:
    return boto3.client("s3")


@pytest.fixture
def bucket(s3: S3Client) -> str:
    s3.create_bucket(Bucket="test-bucket")
    return "test-bucket"


class DownloadAndPipe(Protocol):
    def __call__(self, key: str, args: Sequence[str | Path]) -> int: ...


@pytest.fixture
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


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--update-golden", action="store_true")
    parser.addoption("--remove-stale-golden", action="store_true")


@pytest.fixture(scope="session")
def touched_golden(request: pytest.FixtureRequest) -> Iterator[set[Path]]:
    touched: set[Path] = set()
    yield touched
    found = set()
    for root, _, files in os.walk("tests"):
        for file in files:
            path = Path(root) / file
            if path.suffix == ".golden":
                found.add(path)
    stale = found - touched
    if stale:  # pragma: no cover
        if request.config.getoption("--remove-stale-golden"):
            for path in stale:
                path.unlink()
        else:
            warn(
                "possible stale golden files, re-run with "
                f"--remove-stale-golden: {stale}",
                stacklevel=1,
            )


class Goldify(Protocol):
    def __call__(self, value: str) -> None: ...


@pytest.fixture
def goldify(request: pytest.FixtureRequest, touched_golden: set[Path]) -> Goldify:
    path = Path(request.node.nodeid + ".golden")
    touched_golden.add(path)

    def inner(value: str) -> None:
        if request.config.getoption("--update-golden"):
            path.write_text(value)  # pragma: no cover
        else:
            assert value == path.read_text()

    return inner


class ConsoleFactory(Protocol):
    def __call__(self, file: IO[str] | None = None) -> Console: ...


@pytest.fixture
def console_factory() -> ConsoleFactory:
    def inner(file: IO[str] | None = None) -> Console:
        return Console(
            file=file, theme=THEME, width=88, height=30, color_system="truecolor"
        )

    return inner


@pytest.fixture
def goldifyconsole(
    console_factory: ConsoleFactory, goldify: Goldify
) -> Iterator[Console]:
    file = StringIO()
    console = console_factory(file=file)
    yield console
    goldify(file.getvalue())


@pytest.fixture(autouse=True)
def _utc() -> Iterator[None]:
    with use_tzinfo(timezone.utc):
        yield
