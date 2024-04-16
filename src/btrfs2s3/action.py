"""Actions that modify snapshots or backups."""

from __future__ import annotations

import dataclasses
import logging
from subprocess import PIPE
from subprocess import Popen
from typing import Callable
from typing import TYPE_CHECKING

import btrfsutil

from btrfs2s3._internal.util import NULL_UUID
from btrfs2s3._internal.util import SubvolumeFlags

if TYPE_CHECKING:
    from pathlib import Path

    from mypy_boto3_s3.client import S3Client

_LOG = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class CreateSnapshot:
    """An intent to create a read-only snapshot of a subvolume."""

    source: Path
    path: Path


def create_snapshot(source: Path, path: Path) -> None:
    """Create a read-only snapshot of a subvolume.

    Args:
        source: The source subvolume.
        path: The path at which to create a read-only snapshot.
    """
    _LOG.info("creating read-only snapshot of %s at %s", source, path)
    btrfsutil.create_snapshot(source, path, read_only=True)


@dataclasses.dataclass(frozen=True)
class DeleteSnapshot:
    """An intent to delete a read-only snapshot."""

    path: Path


def delete_snapshot(path: Path) -> None:
    """Delete a read-only snapshot of a subvolume.

    Args:
        path: The path to a read-only snapshot to be deleted.

    Raises:
        RuntimeError: If one of the arguments does not refer to a read-only
            snapshot of a subvolume.
    """
    # Do some extra checks to make sure we only ever delete read-only
    # snapshots, not source subvolumes.
    if not btrfsutil.is_subvolume(path):
        msg = "target isn't a subvolume"
        raise RuntimeError(msg)
    info = btrfsutil.subvolume_info(path)
    if info.parent_uuid == NULL_UUID:
        msg = "target isn't a snapshot"
        raise RuntimeError(msg)
    if not info.flags & SubvolumeFlags.ReadOnly:
        msg = "target isn't a read-only snapshot"
        raise RuntimeError(msg)
    _LOG.info("deleting read-only snapshot %s", path)
    btrfsutil.delete_subvolume(path)


@dataclasses.dataclass(frozen=True)
class RenameSnapshot:
    """An intent to rename a read-only snapshot."""

    source: Path
    get_target: Callable[[], Path]


def rename_snapshot(source: Path, target: Path) -> None:
    """Rename a read-only snapshot of of a subvolume.

    Args:
        source: The source snapshot.
        target: The new path of the snapshot.
    """
    _LOG.info("renaming %s -> %s", source, target)
    source.rename(target)


@dataclasses.dataclass(frozen=True)
class CreateBackup:
    """An intent to create a backup of a read-only snapshot."""

    source: Path
    get_snapshot: Callable[[], Path]
    get_send_parent: Callable[[], Path | None]
    get_key: Callable[[], str]


def create_backup(s3: S3Client, bucket: str, arg: CreateBackup) -> None:
    """Stores a btrfs archive in S3.

    This will spawn "btrfs -q send" as a subprocess, as there is currently no way
    to create a btrfs-send stream via pure python.

    Args:
        s3: An S3 client.
        bucket: The bucket in which to store the archive.
        arg: The other arguments.
    """
    snapshot = arg.get_snapshot()
    send_parent = arg.get_send_parent()
    key = arg.get_key()

    _LOG.info(
        "creating backup of %s (%s)",
        snapshot,
        f"delta from {send_parent}" if send_parent else "full",
    )
    send_args: list[str | Path] = ["btrfs", "-q", "send"]
    if send_parent is not None:
        send_args += ["-p", send_parent]
    send_args += [snapshot]
    send_process = Popen(send_args, stdout=PIPE)  # noqa: S603
    # https://github.com/python/typeshed/issues/3831
    assert send_process.stdout is not None  # noqa: S101

    s3.upload_fileobj(send_process.stdout, bucket, key)

    if send_process.wait() != 0:
        msg = f"'btrfs send' exited with code {send_process.returncode}"
        raise RuntimeError(msg)
