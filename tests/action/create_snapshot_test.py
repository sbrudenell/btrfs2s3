from pathlib import Path

from btrfs2s3._internal.action import create_snapshot
from btrfs2s3._internal.util import SubvolumeFlags
import btrfsutil


def test_call(btrfs_mountpoint: Path) -> None:
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    source_info = btrfsutil.subvolume_info(source)
    path = btrfs_mountpoint / "snapshot"

    create_snapshot(source=source, path=path)

    assert btrfsutil.is_subvolume(path)
    path_info = btrfsutil.subvolume_info(path)
    assert path_info.parent_uuid == source_info.uuid
    assert path_info.flags & SubvolumeFlags.ReadOnly
