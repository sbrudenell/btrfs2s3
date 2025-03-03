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

from typing import TYPE_CHECKING
from unittest.mock import patch

import btrfsutil
import pytest
from rich.console import Console

from btrfs2s3._internal.console import THEME
from btrfs2s3._internal.main import main
from btrfs2s3._internal.s3 import iter_backups

if TYPE_CHECKING:
    from pathlib import Path

    from mypy_boto3_s3.client import S3Client

    from tests.conftest import DownloadAndPipe


def test_pretend(
    tmp_path: Path,
    btrfs_mountpoint: Path,
    bucket: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Create a subvolume
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    # Snapshot dir, but no snapshots
    snapshot_dir = btrfs_mountpoint / "snapshots"
    snapshot_dir.mkdir()
    # Modify some data in the source
    (source / "dummy-file").write_bytes(b"dummy")
    btrfsutil.sync(source)

    console = Console(force_terminal=True, theme=THEME, width=88, height=30)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"""
      timezone: UTC
      sources:
      - path: {source}
        snapshots: {snapshot_dir}
        upload_to_remotes:
        - id: aws
          preserve: 1y
      remotes:
      - id: aws
        s3:
          bucket: {bucket}
    """)
    assert main(console=console, argv=["update", "--pretend", str(config_path)]) == 0

    (out, err) = capsys.readouterr()
    # No idea how to stabilize this for golden testing
    assert "assessment and proposed new state" in out
    assert err == ""


@pytest.fixture(params=[False, True], ids=["noterminal", "terminal"])
def terminal(request: pytest.FixtureRequest) -> bool:
    return bool(request.param)


def test_force(
    tmp_path: Path,
    btrfs_mountpoint: Path,
    s3: S3Client,
    bucket: str,
    capsys: pytest.CaptureFixture[str],
    download_and_pipe: DownloadAndPipe,
    terminal: bool,  # noqa: FBT001
) -> None:
    # Create a subvolume
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    # Snapshot dir, but no snapshots
    snapshot_dir = btrfs_mountpoint / "snapshots"
    snapshot_dir.mkdir()
    # Modify some data in the source
    (source / "dummy-file").write_bytes(b"dummy")
    btrfsutil.sync(source)

    console = Console(force_terminal=terminal, theme=THEME, width=88, height=30)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"""
      timezone: UTC
      sources:
      - path: {source}
        snapshots: {snapshot_dir}
        upload_to_remotes:
        - id: aws
          preserve: 1y
      remotes:
      - id: aws
        s3:
          bucket: {bucket}
    """)
    argv = ["update", "--force", str(config_path)]
    assert main(console=console, argv=argv) == 0

    (out, err) = capsys.readouterr()
    if terminal:
        # No idea how to stabilize this for golden testing
        assert "assessment and proposed new state" in out
    else:
        assert out == ""
    assert err == ""

    (snapshot,) = snapshot_dir.iterdir()
    info = btrfsutil.subvolume_info(snapshot)
    assert info.parent_uuid == btrfsutil.subvolume_info(source).uuid
    ((obj, backup),) = iter_backups(s3, bucket)
    assert backup.uuid == info.uuid
    assert backup.send_parent_uuid is None
    download_and_pipe(obj["Key"], ["btrfs", "receive", "--dump"])

    # Second run should be no-op
    assert main(console=console, argv=argv) == 0

    (out, err) = capsys.readouterr()
    assert "nothing to be done" in out


def test_refuse_to_run_unattended_without_pretend_or_force(
    tmp_path: Path, goldifyconsole: Console
) -> None:
    # This shouldn't get to the point of verifying arguments
    config_path = tmp_path / "config.yaml"
    config_path.write_text("""
      timezone: UTC
      sources:
      - path: dummy_source
        snapshots: dummy_snapshot_dir
        upload_to_remotes:
        - id: aws
          preserve: 1y
      remotes:
      - id: aws
        s3:
          bucket: dummy_bucket
    """)
    assert main(argv=["update", str(config_path)], console=goldifyconsole) == 1


def test_reject_continue_prompt(
    tmp_path: Path,
    btrfs_mountpoint: Path,
    bucket: str,
    capsys: pytest.CaptureFixture[str],
    s3: S3Client,
) -> None:
    # Create a subvolume
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    # Snapshot dir, but no snapshots
    snapshot_dir = btrfs_mountpoint / "snapshots"
    snapshot_dir.mkdir()
    # Modify some data in the source
    (source / "dummy-file").write_bytes(b"dummy")
    btrfsutil.sync(source)

    console = Console(force_terminal=True, theme=THEME, width=88, height=30)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"""
      timezone: UTC
      sources:
      - path: {source}
        snapshots: {snapshot_dir}
        upload_to_remotes:
        - id: aws
          preserve: 1y
      remotes:
      - id: aws
        s3:
          bucket: {bucket}
    """)
    with patch("rich.console.input", return_value="n"):
        assert main(console=console, argv=["update", str(config_path)]) == 0

    (out, err) = capsys.readouterr()
    # No idea how to stabilize this for golden testing
    assert "continue?" in out
    assert err == ""

    # Ensure there were no side effects
    assert list(snapshot_dir.iterdir()) == []
    assert s3.list_objects_v2(Bucket=bucket).get("Contents", []) == []
