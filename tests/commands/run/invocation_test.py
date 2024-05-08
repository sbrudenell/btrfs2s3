from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import patch

from btrfs2s3.console import THEME
from btrfs2s3.main import main
from btrfs2s3.s3 import iter_backups
import btrfsutil
from rich.console import Console

if TYPE_CHECKING:
    from pathlib import Path

    from mypy_boto3_s3.client import S3Client
    import pytest

    from tests.conftest import DownloadAndPipe


def test_pretend(
    btrfs_mountpoint: Path, bucket: str, capsys: pytest.CaptureFixture[str]
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
    argv = [
        "run",
        "--pretend",
        "--source",
        str(source),
        "--snapshot-dir",
        str(snapshot_dir),
        "--bucket",
        bucket,
        "--timezone",
        "UTC",
        "--preserve",
        "1y",
    ]
    assert main(console=console, argv=argv) == 0

    (out, err) = capsys.readouterr()
    # No idea how to stabilize this for golden testing
    assert "assessment and proposed new state" in out
    assert err == ""


def test_force(
    btrfs_mountpoint: Path,
    s3: S3Client,
    bucket: str,
    capsys: pytest.CaptureFixture[str],
    download_and_pipe: DownloadAndPipe,
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
    argv = [
        "run",
        "--force",
        "--source",
        str(source),
        "--snapshot-dir",
        str(snapshot_dir),
        "--bucket",
        bucket,
        "--timezone",
        "UTC",
        "--preserve",
        "1y",
    ]
    assert main(console=console, argv=argv) == 0

    (out, err) = capsys.readouterr()
    # No idea how to stabilize this for golden testing
    assert "assessment and proposed new state" in out
    assert err == ""

    (snapshot,) = snapshot_dir.iterdir()
    info = btrfsutil.subvolume_info(snapshot)
    assert info.parent_uuid == btrfsutil.subvolume_info(source).uuid
    ((obj, backup),) = iter_backups(s3, bucket)
    assert backup.uuid == info.uuid
    assert backup.send_parent_uuid is None
    download_and_pipe(obj["Key"], ["btrfs", "receive", "--dump"])

    # Second run should be no-op
    assert main(argv=argv) == 0

    (out, err) = capsys.readouterr()
    assert "nothing to be done" in out


def test_refuse_to_run_unattended_without_pretend_or_force(
    goldifyconsole: Console,
) -> None:
    # This shouldn't get to the point of verifying arguments
    argv = [
        "run",
        "--source",
        "dummy_source",
        "--snapshot-dir",
        "dummy_snapshot_dir",
        "--bucket",
        "dummy_bucket",
        "--timezone",
        "UTC",
        "--preserve",
        "1y",
    ]
    assert main(argv=argv, console=goldifyconsole) == 1


def test_reject_continue_prompt(
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
    argv = [
        "run",
        "--source",
        str(source),
        "--snapshot-dir",
        str(snapshot_dir),
        "--bucket",
        bucket,
        "--timezone",
        "UTC",
        "--preserve",
        "1y",
    ]
    with patch("rich.console.input", return_value="n"):
        assert main(console=console, argv=argv) == 0

    (out, err) = capsys.readouterr()
    # No idea how to stabilize this for golden testing
    assert "continue?" in out
    assert err == ""

    # Ensure there were no side effects
    assert list(snapshot_dir.iterdir()) == []
    assert s3.list_objects_v2(Bucket=bucket).get("Contents", []) == []
