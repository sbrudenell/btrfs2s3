"""Actions that modify snapshots or backups."""

from __future__ import annotations

import dataclasses
from itertools import chain
import logging
from subprocess import PIPE
from subprocess import Popen
from typing import TYPE_CHECKING

import btrfsutil

from btrfs2s3._internal.util import NULL_UUID
from btrfs2s3._internal.util import SubvolumeFlags
from btrfs2s3.thunk import Thunk
from btrfs2s3.thunk import ThunkArg

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Iterator
    from typing import Sequence

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


def create_backup(  # noqa: PLR0913
    *,
    s3: S3Client,
    bucket: str,
    snapshot: Path,
    send_parent: Path | None,
    key: str,
    pipe_through: Sequence[Sequence[str]] = (),
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
        pipe_through: A sequence of shell commands through which the archive
            should be piped before uploading.
    """
    _LOG.info(
        "creating backup of %s (%s)",
        snapshot,
        f"delta from {send_parent}" if send_parent else "full",
    )
    send_args: list[str | Path] = ["btrfs", "send", "-q"]
    if send_parent is not None:
        send_args += ["-p", send_parent]
    send_args += [snapshot]

    pipeline: list[Popen[bytes]] = []
    for args in chain((send_args,), pipe_through):
        prev_stdout = pipeline[-1].stdout if pipeline else None
        pipeline.append(Popen(args, stdin=prev_stdout, stdout=PIPE))  # noqa: S603
        # https://docs.python.org/3/library/subprocess.html#replacing-shell-pipeline
        if prev_stdout:
            prev_stdout.close()

    pipeline_stdout = pipeline[-1].stdout
    # https://github.com/python/typeshed/issues/3831
    assert pipeline_stdout is not None  # noqa: S101
    try:
        s3.upload_fileobj(pipeline_stdout, bucket, key)
    finally:
        # Allow the pipeline to fail if the upload fails
        pipeline_stdout.close()

    # reverse order to match the semantics of pipefail
    for process in reversed(pipeline):
        if process.wait() != 0:
            msg = f"{process.args!r}: exited with code {process.returncode}"
            try:
                raise RuntimeError(msg)
            finally:
                # Assume the backup is corrupted
                delete_backups(s3, bucket, key)


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


# Future: all the fields of the intent objects should really be
# descriptor-typed fields that convert things to Thunk.


@dataclasses.dataclass(frozen=True, order=True)
class CreateSnapshot:
    """An intent to create a read-only snapshot of a subvolume."""

    source: Thunk[Path]
    path: Thunk[Path]


@dataclasses.dataclass(frozen=True, order=True)
class DeleteSnapshot:
    """An intent to delete a read-only snapshot."""

    path: Thunk[Path]


@dataclasses.dataclass(frozen=True, order=True)
class RenameSnapshot:
    """An intent to rename a read-only snapshot."""

    source: Thunk[Path]
    target: Thunk[Path]


@dataclasses.dataclass(frozen=True, order=True)
class CreateBackup:
    """An intent to create a backup of a read-only snapshot."""

    source: Thunk[Path]
    snapshot: Thunk[Path]
    send_parent: Thunk[Path | None]
    key: Thunk[str]


@dataclasses.dataclass(frozen=True, order=True)
class DeleteBackup:
    """An intent to delete a backup."""

    key: Thunk[str]


class Actions:
    """A list of actions to be executed.

    This class just stores a list of intents. Intents of actions can be added
    with the various functions, and then examined with the iter_*_intents()
    functions. It doesn't actually perform any actions until execute() is called.

    All of the arguments to the intent functions are converted to Thunks.

    Note that when creating Thunks, it's critical to understand the order that
    actions are performed. See execute().
    """

    def __init__(self) -> None:
        """Construct an empty Actions object."""
        self._create_snapshots: list[CreateSnapshot] = []
        self._delete_snapshots: list[DeleteSnapshot] = []
        self._rename_snapshots: list[RenameSnapshot] = []
        self._create_backups: list[CreateBackup] = []
        self._delete_backups: list[DeleteBackup] = []

    def empty(self) -> bool:
        """Returns True if there are no intended actions."""
        return not (
            self._create_snapshots
            or self._delete_snapshots
            or self._rename_snapshots
            or self._create_backups
            or self._delete_backups
        )

    def create_snapshot(self, *, source: ThunkArg[Path], path: ThunkArg[Path]) -> None:
        """Add an intent to create a read-only snapshot of a subvolume.

        Args:
            source: The path to the source subvolume, of which a snapshot
                should be created.
            path: The path where the new snapshot should be created.
        """
        self._create_snapshots.append(
            CreateSnapshot(source=Thunk(source), path=Thunk(path))
        )

    def rename_snapshot(
        self, *, source: ThunkArg[Path], target: ThunkArg[Path]
    ) -> None:
        """Add an intent to rename a snapshot.

        Args:
            source: The initial path of the snapshot.
            target: The new path of the snapshot.
        """
        self._rename_snapshots.append(
            RenameSnapshot(source=Thunk(source), target=Thunk(target))
        )

    def delete_snapshot(self, path: ThunkArg[Path]) -> None:
        """Add an intent to delete a snapshot.

        Args:
            path: The path of the snapshot to be deleted.
        """
        self._delete_snapshots.append(DeleteSnapshot(path=Thunk(path)))

    def create_backup(
        self,
        *,
        source: ThunkArg[Path],
        snapshot: ThunkArg[Path],
        send_parent: ThunkArg[Path | None],
        key: ThunkArg[str],
    ) -> None:
        """Add an intent to create a new backup of a read-only snapshot.

        The backup will be created using `btrfs send` (if send_parent is not
        None, then `btrfs send -p` will be used). The backup will be streamed
        into an S3 object with the given key.

        Args:
            source: The path to the source subvolume of the snapshot. This
                has no effect when creating the backup, but it's very useful
                when examining intents with iter_create_backup_intents().
            snapshot: The path to the read-only snapshot to be backed up.
            send_parent: The path to a read-only snapshot to be used as a
                parent (will be passed to `btrfs send -p`).
            key: The S3 object key at which to store the backup.
        """
        self._create_backups.append(
            CreateBackup(
                source=Thunk(source),
                snapshot=Thunk(snapshot),
                send_parent=Thunk(send_parent),
                key=Thunk(key),
            )
        )

    def delete_backup(self, key: ThunkArg[str]) -> None:
        """Add an intent to delete an S3 object.

        The target S3 object should ostensibly be a backup previously created,
        but we don't verify this.

        Args:
            key: The S3 object key to be deleted.
        """
        self._delete_backups.append(DeleteBackup(Thunk(key)))

    def iter_create_snapshot_intents(self) -> Iterator[CreateSnapshot]:
        """Iterates all the CreateSnapshot intents."""
        yield from sorted(self._create_snapshots)

    def iter_delete_snapshot_intents(self) -> Iterator[DeleteSnapshot]:
        """Iterates all the DeleteSnapshot intents."""
        yield from sorted(self._delete_snapshots)

    def iter_rename_snapshot_intents(self) -> Iterator[RenameSnapshot]:
        """Iterates all the RenameSnapshot intents."""
        yield from sorted(self._rename_snapshots)

    def iter_create_backup_intents(self) -> Iterator[CreateBackup]:
        """Iterates all the CreateBackup intents."""
        yield from sorted(self._create_backups)

    def iter_delete_backup_intents(self) -> Iterator[DeleteBackup]:
        """Iterates all the DeleteBackup intents."""
        yield from sorted(self._delete_backups)

    def execute(
        self, s3: S3Client, bucket: str, pipe_through: Sequence[Sequence[str]] = ()
    ) -> None:
        """Executes the intended actions.

        This performs all the side effects described in the various intent
        objects. It will create/rename/delete snapshots using btrfsutil, and
        create/delete backups in the supplied S3 bucket.

        The actions are performed in the following order:

        - Create snapshots
        - Rename snapshots
        - Create backups
        - Delete snapshots
        - Delete backups

        Within an action type, the actions are performed in the same order
        returned from the iter_*_intents() functions. That is, snapshots are
        created in the same order that they are returned from
        iter_create_snapshot_intents(), etc.

        Args:
            s3: The S3 client object to use to manipulate S3 objects.
            bucket: The name of the bucket where backups are stored.
            pipe_through: A sequence of shell commands through which backup
                archives should be piped before uploading.
        """
        for create_snapshot_intent in self.iter_create_snapshot_intents():
            create_snapshot(
                source=create_snapshot_intent.source(),
                path=create_snapshot_intent.path(),
            )

        for rename_snapshot_intent in self.iter_rename_snapshot_intents():
            rename_snapshot(
                source=rename_snapshot_intent.source(),
                target=rename_snapshot_intent.target(),
            )

        for create_backup_intent in self.iter_create_backup_intents():
            create_backup(
                s3=s3,
                bucket=bucket,
                snapshot=create_backup_intent.snapshot(),
                send_parent=create_backup_intent.send_parent(),
                key=create_backup_intent.key(),
                pipe_through=pipe_through,
            )

        for delete_snapshot_intent in self.iter_delete_snapshot_intents():
            delete_snapshot(delete_snapshot_intent.path())

        keys = tuple(d.key() for d in self.iter_delete_backup_intents())
        delete_backups(s3, bucket, *keys)
