from pathlib import Path

from btrfs2s3.action import rename_snapshot
import btrfsutil


def test_call(btrfs_mountpoint: Path) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    snapshot = btrfs_mountpoint / "snapshot"
    btrfsutil.create_snapshot(source, snapshot, read_only=True)

    target = btrfs_mountpoint / "new-snapshot"

    rename_snapshot(source=snapshot, target=target)

    assert not snapshot.exists()
    assert target.exists()
