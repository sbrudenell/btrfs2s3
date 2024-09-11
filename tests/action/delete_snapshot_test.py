import os
from pathlib import Path
import subprocess

from btrfs2s3._internal.action import delete_snapshot
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


def test_delete_as_normal_user(btrfs_mountpoint: Path) -> None:
    # This test is narrow. We don't have full test coverage for non-root
    # operation currently. See https://github.com/sbrudenell/btrfs2s3/issues/49

    subprocess.check_call(
        ["mount", "-o", "remount,user_subvol_rm_allowed", btrfs_mountpoint]
    )

    uid = 1000
    os.chown(btrfs_mountpoint, uid, 0)
    os.seteuid(1000)
    try:
        source = btrfs_mountpoint / "source"
        btrfsutil.create_subvolume(source)
        snapshot = btrfs_mountpoint / "snapshot"
        btrfsutil.create_snapshot(source, snapshot, read_only=True)
        delete_snapshot(snapshot)
    finally:
        os.seteuid(0)
