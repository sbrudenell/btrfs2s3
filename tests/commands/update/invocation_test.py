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

from enum import Enum
from typing import cast
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from rich.console import Console

from btrfs2s3._internal.btrfsioctl import create_subvol
from btrfs2s3._internal.btrfsioctl import subvol_info
from btrfs2s3._internal.console import THEME
from btrfs2s3._internal.main import main
from btrfs2s3._internal.s3 import iter_backups

if TYPE_CHECKING:
    from pathlib import Path

    from mypy_boto3_s3.client import S3Client

    from tests.conftest import DownloadAndPipe


class Impl(Enum):
    Update = "update"
    Update2 = "update2"


@pytest.fixture(params=[Impl.Update, Impl.Update2])
def impl(request: pytest.FixtureRequest) -> Impl:
    return cast(Impl, request.param)


def test_pretend(
    tmp_path: Path,
    btrfs_mountpoint: Path,
    bucket: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Create a subvolume
    source = btrfs_mountpoint / "source"
    create_subvol(source)
    # Snapshot dir, but no snapshots
    snapshot_dir = btrfs_mountpoint / "snapshots"
    snapshot_dir.mkdir()
    # Modify some data in the source
    (source / "dummy-file").write_bytes(b"dummy")

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
    impl: Impl,
) -> None:
    # Create a subvolume
    source = btrfs_mountpoint / "source"
    create_subvol(source)
    # Snapshot dir, but no snapshots
    snapshot_dir = btrfs_mountpoint / "snapshots"
    snapshot_dir.mkdir()
    # Modify some data in the source
    (source / "dummy-file").write_bytes(b"dummy")

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
    argv = [impl.value, "--force", str(config_path)]
    assert main(console=console, argv=argv) == 0

    (out, err) = capsys.readouterr()
    if terminal:
        # No idea how to stabilize this for golden testing
        if impl == Impl.Update:
            assert "assessment and proposed new state" in out
        else:
            assert "actions to take" in out
    else:
        assert out == ""
    assert err == ""

    (snapshot,) = snapshot_dir.iterdir()
    info = subvol_info(snapshot)
    assert info.parent_uuid == subvol_info(source).uuid
    ((obj, backup),) = iter_backups(s3, bucket)
    assert backup.uuid == info.uuid
    assert backup.send_parent_uuid is None
    download_and_pipe(obj["Key"], ["btrfs", "receive", "--dump"])

    # Second run should be no-op
    assert main(console=console, argv=argv) == 0

    (out, err) = capsys.readouterr()
    if impl == Impl.Update or terminal:
        assert "nothing to be done" in out
    else:
        assert out == ""


def test_refuse_to_run_unattended_without_pretend_or_force(
    tmp_path: Path, goldifyconsole: Console, impl: Impl
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
    assert main(argv=[impl.value, str(config_path)], console=goldifyconsole) == 1


@pytest.fixture(params=[False, True])
def undo(request: pytest.FixtureRequest) -> bool:
    return bool(request.param)


def test_reject_continue_prompt(
    tmp_path: Path,
    btrfs_mountpoint: Path,
    bucket: str,
    capsys: pytest.CaptureFixture[str],
    s3: S3Client,
    impl: Impl,
    undo: bool,  # noqa: FBT001
) -> None:
    if impl == Impl.Update and undo:
        pytest.xfail("not a case")
    # Create a subvolume
    source = btrfs_mountpoint / "source"
    create_subvol(source)
    # Snapshot dir, but no snapshots
    snapshot_dir = btrfs_mountpoint / "snapshots"
    snapshot_dir.mkdir()
    # Modify some data in the source
    (source / "dummy-file").write_bytes(b"dummy")

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
    with patch("rich.console.input", return_value="u" if undo else "n"):
        assert main(console=console, argv=[impl.value, str(config_path)]) == 0

    (out, err) = capsys.readouterr()
    # No idea how to stabilize this for golden testing
    assert "continue?" in out
    assert err == ""

    # Ensure there were no side effects
    if impl == Impl.Update or undo:
        assert list(snapshot_dir.iterdir()) == []
    assert s3.list_objects_v2(Bucket=bucket).get("Contents", []) == []
