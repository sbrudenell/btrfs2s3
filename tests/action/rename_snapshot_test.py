from pathlib import Path

from btrfs2s3.action import rename_snapshots
from btrfs2s3.action import RenameSnapshot
import btrfsutil


def test_call(btrfs_mountpoint: Path) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    snapshot = btrfs_mountpoint / "snapshot"
    btrfsutil.create_snapshot(source, snapshot, read_only=True)

    target = btrfs_mountpoint / "new-snapshot"

    action = RenameSnapshot(source=snapshot, get_target=lambda: target)
    rename_snapshots(action)

    assert not snapshot.exists()
    assert target.exists()
