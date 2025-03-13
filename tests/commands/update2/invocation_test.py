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

import pytest
from rich.console import Console

from btrfs2s3._internal.btrfsioctl import create_snap
from btrfs2s3._internal.btrfsioctl import create_subvol
from btrfs2s3._internal.btrfsioctl import subvol_info
from btrfs2s3._internal.commands.update2 import NAME
from btrfs2s3._internal.console import THEME
from btrfs2s3._internal.main import main
from btrfs2s3._internal.s3 import iter_backups

if TYPE_CHECKING:
    from pathlib import Path

    from types_boto3_s3.client import S3Client

    from tests.conftest import DownloadAndPipe


@pytest.mark.parametrize(
    ("terminal", "force", "with_created_snapshots"),
    [
        (True, True, True),
        (True, True, False),
        (True, False, True),
        (True, False, False),
        (False, True, True),
        (False, True, False),
    ],
)
def test_execute(
    tmp_path: Path,
    btrfs_mountpoint: Path,
    s3: S3Client,
    bucket: str,
    capsys: pytest.CaptureFixture[str],
    download_and_pipe: DownloadAndPipe,
    terminal: bool,  # noqa: FBT001
    force: bool,  # noqa: FBT001
    with_created_snapshots: bool,  # noqa: FBT001
) -> None:
    # Create a subvolume
    source = btrfs_mountpoint / "source"
    create_subvol(source)
    # Snapshot dir, but no snapshots
    snapshot_dir = btrfs_mountpoint / "snapshots"
    snapshot_dir.mkdir()
    # Modify some data in the source
    (source / "dummy-file").write_bytes(b"dummy")
    if not with_created_snapshots:
        create_snap(src=source, dst=snapshot_dir / "snapshot", read_only=True)

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
    argv = [NAME, "--force", str(config_path)] if force else [NAME, str(config_path)]
    if terminal and not force:
        with patch("rich.console.input", return_value="y"):
            assert main(console=console, argv=argv) == 0
    else:
        assert main(console=console, argv=argv) == 0

    (out, err) = capsys.readouterr()
    if terminal:
        # No idea how to stabilize this for golden testing
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
    if terminal:
        assert "nothing to be done" in out
    else:
        assert out == ""
    assert err == ""


def test_refuse_to_run_unattended_without_force(
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
    assert main(argv=[NAME, str(config_path)], console=goldifyconsole) == 1


@pytest.mark.parametrize(
    ("with_created_snapshots", "undo"), [(True, True), (True, False), (False, False)]
)
def test_reject_continue_prompt(
    tmp_path: Path,
    btrfs_mountpoint: Path,
    bucket: str,
    capsys: pytest.CaptureFixture[str],
    s3: S3Client,
    undo: bool,  # noqa: FBT001
    with_created_snapshots: bool,  # noqa: FBT001
) -> None:
    # Create a subvolume
    source = btrfs_mountpoint / "source"
    create_subvol(source)
    # Snapshot dir, but no snapshots
    snapshot_dir = btrfs_mountpoint / "snapshots"
    snapshot_dir.mkdir()
    # Modify some data in the source
    (source / "dummy-file").write_bytes(b"dummy")
    if not with_created_snapshots:
        create_snap(src=source, dst=snapshot_dir / "snapshot", read_only=True)

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
        assert main(console=console, argv=[NAME, str(config_path)]) == 0

    (out, err) = capsys.readouterr()
    # No idea how to stabilize this for golden testing
    assert "continue?" in out
    assert err == ""

    # Ensure there were no side effects
    if with_created_snapshots and undo:
        assert list(snapshot_dir.iterdir()) == []
    assert s3.list_objects_v2(Bucket=bucket).get("Contents", []) == []


def test_execute_with_multiple_sources(
    tmp_path: Path,
    btrfs_mountpoint: Path,
    s3: S3Client,
    bucket: str,
    download_and_pipe: DownloadAndPipe,
) -> None:
    # Create subvolumes
    source1 = btrfs_mountpoint / "source1"
    source2 = btrfs_mountpoint / "source2"
    create_subvol(source1)
    create_subvol(source2)
    # Snapshot dir, but no snapshots
    snapshot_dir = btrfs_mountpoint / "snapshots"
    snapshot_dir.mkdir()
    # Modify some data in the source
    (source1 / "dummy-file").write_bytes(b"dummy")
    (source2 / "dummy-file").write_bytes(b"dummy")

    console = Console(force_terminal=False, theme=THEME, width=88, height=30)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"""
      timezone: UTC
      sources:
      - path: {source1}
        snapshots: {snapshot_dir}
        upload_to_remotes:
        - id: aws
          preserve: 1y
      - path: {source2}
        snapshots: {snapshot_dir}
        upload_to_remotes:
        - id: aws
          preserve: 1y
      remotes:
      - id: aws
        s3:
          bucket: {bucket}
    """)
    assert main(console=console, argv=[NAME, "--force", str(config_path)]) == 0

    (snapshot1, snapshot2) = snapshot_dir.iterdir()
    info1 = subvol_info(snapshot1)
    info2 = subvol_info(snapshot2)
    assert {info1.parent_uuid, info2.parent_uuid} == {
        subvol_info(source1).uuid,
        subvol_info(source2).uuid,
    }
    ((obj1, backup1), (obj2, backup2)) = iter_backups(s3, bucket)
    assert {backup1.uuid, backup2.uuid} == {info1.uuid, info2.uuid}
    assert backup1.send_parent_uuid is None
    assert backup2.send_parent_uuid is None
    download_and_pipe(obj1["Key"], ["btrfs", "receive", "--dump"])
    download_and_pipe(obj2["Key"], ["btrfs", "receive", "--dump"])
