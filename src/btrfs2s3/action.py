"""Actions that modify snapshots or backups."""

import dataclasses
import logging
from pathlib import Path
from typing import Callable

import btrfsutil

from btrfs2s3._internal.util import NULL_UUID
from btrfs2s3._internal.util import SubvolumeFlags

_LOG = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class CreateSnapshot:
    """An intent to create a read-only snapshot of a subvolume."""

    source: Path
    path: Path


def create_snapshots(*args: CreateSnapshot) -> None:
    """Create one or more read-only snapshots of subvolumes.

    Args:
        *args: The arguments for which read-only snapshots to be created.
    """
    for arg in args:
        _LOG.info("creating read-only snapshot of %s at %s", arg.source, arg.path)
        btrfsutil.create_snapshot(arg.source, arg.path, read_only=True)


@dataclasses.dataclass(frozen=True)
class DeleteSnapshot:
    """An intent to delete a read-only snapshot."""

    path: Path


def delete_snapshots(*args: DeleteSnapshot) -> None:
    """Delete one or more read-only snapshots of subvolumes.

    Args:
        *args: The arguments for which read-only snapshots to be deleted.

    Raises:
        RuntimeError: If one of the arguments does not refer to a read-only
            snapshot of a subvolume.
    """
    for arg in args:
        # Do some extra checks to make sure we only ever delete read-only
        # snapshots, not source subvolumes.
        if not btrfsutil.is_subvolume(arg.path):
            msg = "target isn't a subvolume"
            raise RuntimeError(msg)
        info = btrfsutil.subvolume_info(arg.path)
        if info.parent_uuid == NULL_UUID:
            msg = "target isn't a snapshot"
            raise RuntimeError(msg)
        if not info.flags & SubvolumeFlags.ReadOnly:
            msg = "target isn't a read-only snapshot"
            raise RuntimeError(msg)
        _LOG.info("deleting read-only snapshot %s", arg.path)
        btrfsutil.delete_subvolume(arg.path)


@dataclasses.dataclass(frozen=True)
class RenameSnapshot:
    """An intent to rename a read-only snapshot."""

    source: Path
    get_target: Callable[[], Path]


def rename_snapshots(*args: RenameSnapshot) -> None:
    """Renames one or more read-only snapshots of subvolumes.

    Args:
        *args: The arguments for renamings to be done.
    """
    for arg in args:
        source = arg.source
        target = arg.get_target()
        _LOG.info("renaming %s -> %s", source, target)
        source.rename(target)
