"""Actions that modify snapshots or backups."""

from __future__ import annotations

import dataclasses
import functools
from itertools import chain
import logging
from subprocess import PIPE
from subprocess import Popen
from typing import Callable
from typing import Iterator
from typing import TYPE_CHECKING

import btrfsutil

from btrfs2s3._internal.util import NULL_UUID
from btrfs2s3._internal.util import SubvolumeFlags

if TYPE_CHECKING:
    from pathlib import Path

    from mypy_boto3_s3.client import S3Client

_LOG = logging.getLogger(__name__)


def create_snapshot(*, source: Path, path: Path) -> None:
    """Create a read-only snapshot of a subvolume.

    Args:
        source: The source subvolume.
        path: The path at which to create a read-only snapshot.
    """
    _LOG.info("creating read-only snapshot of %s at %s", source, path)
    btrfsutil.create_snapshot(source, path, read_only=True)


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


def rename_snapshot(*, source: Path, target: Path) -> None:
    """Rename a read-only snapshot of of a subvolume.

    Args:
        source: The source snapshot.
        target: The new path of the snapshot.
    """
    _LOG.info("renaming %s -> %s", source, target)
    source.rename(target)


def create_backup(
    *, s3: S3Client, bucket: str, snapshot: Path, send_parent: Path | None, key: str
) -> None:
    """Stores a btrfs archive in S3.

    This will spawn "btrfs -q send" as a subprocess, as there is currently no way
    to create a btrfs-send stream via pure python.

    Args:
        s3: An S3 client.
        bucket: The bucket in which to store the archive.
        snapshot: The snapshot to back up.
        send_parent: The parent snapshot for delta backups. When not None, this
            will be supplied to "btrfs send -p".
        key: The S3 object key.
    """
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


def delete_backups(s3: S3Client, bucket: str, *keys: str) -> None:
    """Batch delete backups from S3.

    This will use the DeleteObjects API call, which can delete multiple keys in
    batches.

    Args:
        s3: An S3 client.
        bucket: The bucket from which to delete keys.
        *keys: The keys to delete.
    """

    def batches() -> Iterator[tuple[str, ...]]:
        for i in range(0, len(keys), 1000):
            yield keys[i : i + 1000]

    for batch in batches():
        for key in batch:
            _LOG.info("deleting backup %s", key)
        # Do we need to inspect the response for individual errors, or will we
        # raise an exception in this case? The docs are thousands of words long
        # but don't explain this
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Quiet": True, "Objects": [{"Key": key} for key in batch]},
        )


@dataclasses.dataclass(frozen=True, order=True)
class CreateSnapshot:
    """An intent to create a read-only snapshot of a subvolume."""

    source: Path
    path: Path


@dataclasses.dataclass(frozen=True, order=True)
class DeleteSnapshot:
    """An intent to delete a read-only snapshot."""

    path: Path


@dataclasses.dataclass(frozen=True, order=True)
class RenameSnapshot:
    """An intent to rename a read-only snapshot."""

    source: Path
    get_target: Callable[[], Path]


@dataclasses.dataclass(frozen=True, order=True)
class CreateBackup:
    """An intent to create a backup of a read-only snapshot."""

    source: Path
    get_snapshot: Callable[[], Path]
    get_send_parent: Callable[[], Path | None]
    get_key: Callable[[], str]


@dataclasses.dataclass(frozen=True, order=True)
class DeleteBackup:
    """An intent to delete a backup."""

    key: str


class Plan:
    def __init__(self) -> None:
        self._create_snapshots: list[CreateSnapshot] = []
        self._delete_snapshots: list[DeleteSnapshot] = []
        self._rename_snapshots: list[RenameSnapshot] = []
        self._create_backups: list[CreateBackup] = []
        self._delete_backups: list[DeleteBackup] = []

        self._future_rename_snapshots: list[RenameSnapshot] = []
        self._future_create_backups: list[CreateBackup] = []

    def create_rename_backup(  # noqa: PLR0913
        self,
        *,
        source: Path,
        initial_path: Path,
        get_target_path: Callable[[], Path],
        get_send_parent: Callable[[], Path | None],
        get_key: Callable[[], str],
    ) -> None:
        get_target_path = functools.cache(get_target_path)
        self._create_snapshots.append(CreateSnapshot(source=source, path=initial_path))
        self._future_rename_snapshots.append(
            RenameSnapshot(source=initial_path, get_target=get_target_path)
        )
        self._future_create_backups.append(
            CreateBackup(
                source=source,
                get_snapshot=get_target_path,
                get_send_parent=get_send_parent,
                get_key=get_key,
            )
        )

    def rename_snapshot(self, *, source: Path, target: Path) -> None:
        self._rename_snapshots.append(
            RenameSnapshot(source=source, get_target=lambda: target)
        )

    def delete_snapshot(self, snapshot: Path) -> None:
        self._delete_snapshots.append(DeleteSnapshot(snapshot))

    def create_backup(
        self, *, source: Path, snapshot: Path, send_parent: Path | None, key: str
    ) -> None:
        self._create_backups.append(
            CreateBackup(
                source=source,
                get_snapshot=lambda: snapshot,
                get_send_parent=lambda: send_parent,
                get_key=lambda: key,
            )
        )

    def delete_backup(self, key: str) -> None:
        self._delete_backups.append(DeleteBackup(key))

    def iter_create_snapshot_intents(self) -> Iterator[CreateSnapshot]:
        yield from sorted(self._create_snapshots)

    def iter_delete_snapshot_intents(self) -> Iterator[DeleteSnapshot]:
        yield from sorted(self._delete_snapshots)

    def iter_rename_snapshot_intents(self) -> Iterator[RenameSnapshot]:
        yield from sorted(self._rename_snapshots)

    def iter_future_rename_snapshot_intents(self) -> Iterator[RenameSnapshot]:
        yield from sorted(self._future_rename_snapshots)

    def iter_create_backup_intents(self) -> Iterator[CreateBackup]:
        yield from sorted(
            self._create_backups, key=lambda i: (i.source, i.get_snapshot())
        )

    def iter_future_create_backup_intents(self) -> Iterator[CreateBackup]:
        yield from sorted(self._future_create_backups)

    def iter_delete_backup_intents(self) -> Iterator[DeleteBackup]:
        yield from sorted(self._delete_backups)

    def execute(self, s3: S3Client, bucket: str) -> None:
        for create_snapshot_intent in self.iter_create_snapshot_intents():
            create_snapshot(
                source=create_snapshot_intent.source, path=create_snapshot_intent.path
            )

        rename_snapshot_intents = chain(
            self.iter_rename_snapshot_intents(),
            self.iter_future_rename_snapshot_intents(),
        )
        for rename_snapshot_intent in rename_snapshot_intents:
            rename_snapshot(
                source=rename_snapshot_intent.source,
                target=rename_snapshot_intent.get_target(),
            )

        create_backup_intents = chain(
            self.iter_create_backup_intents(), self.iter_future_create_backup_intents()
        )
        for create_backup_intent in create_backup_intents:
            create_backup(
                s3=s3,
                bucket=bucket,
                snapshot=create_backup_intent.get_snapshot(),
                send_parent=create_backup_intent.get_send_parent(),
                key=create_backup_intent.get_key(),
            )

        for delete_snapshot_intent in self.iter_delete_snapshot_intents():
            delete_snapshot(delete_snapshot_intent.path)

        keys = tuple(d.key for d in self.iter_delete_backup_intents())
        delete_backups(s3, bucket, *keys)
