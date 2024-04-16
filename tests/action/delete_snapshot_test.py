from pathlib import Path

from btrfs2s3.action import delete_snapshot
import btrfsutil
import pytest


def test_call(btrfs_mountpoint: Path) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    snapshot = btrfs_mountpoint / "snapshot"
    btrfsutil.create_snapshot(source, snapshot, read_only=True)

    delete_snapshot(snapshot)

    assert not snapshot.exists()


def test_not_a_subvolume(btrfs_mountpoint: Path) -> None:
    source = btrfs_mountpoint / "source"
    source.mkdir()

    with pytest.raises(RuntimeError, match="target isn't a subvolume"):
        delete_snapshot(source)


def test_not_a_snapshot(btrfs_mountpoint: Path) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)

    with pytest.raises(RuntimeError, match="target isn't a snapshot"):
        delete_snapshot(source)


def test_not_a_read_only_snapshot(btrfs_mountpoint: Path) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    snapshot = btrfs_mountpoint / "snapshot"
    btrfsutil.create_snapshot(source, snapshot, read_only=False)

    with pytest.raises(RuntimeError, match="target isn't a read-only snapshot"):
        delete_snapshot(snapshot)
