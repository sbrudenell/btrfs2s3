import os
from pathlib import Path
import subprocess
import tempfile
from typing import Iterator

from moto import mock_aws
import pytest


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
