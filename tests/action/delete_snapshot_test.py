from pathlib import Path

from btrfs2s3.action import DeleteSnapshot
import btrfsutil
import pytest


def test_call(btrfs_mountpoint: Path) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    snapshot = btrfs_mountpoint / "snapshot"
    btrfsutil.create_snapshot(source, snapshot, read_only=True)

    action = DeleteSnapshot(snapshot)
    action()

    assert not snapshot.exists()


def test_not_a_subvolume(btrfs_mountpoint: Path) -> None:
    source = btrfs_mountpoint / "source"
    source.mkdir()

    action = DeleteSnapshot(source)
    with pytest.raises(RuntimeError, match="target isn't a subvolume"):
        action()


def test_not_a_snapshot(btrfs_mountpoint: Path) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)

    action = DeleteSnapshot(source)
    with pytest.raises(RuntimeError, match="target isn't a snapshot"):
        action()


def test_not_a_read_only_snapshot(btrfs_mountpoint: Path) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    snapshot = btrfs_mountpoint / "snapshot"
    btrfsutil.create_snapshot(source, snapshot, read_only=False)

    action = DeleteSnapshot(snapshot)
    with pytest.raises(RuntimeError, match="target isn't a read-only snapshot"):
        action()
