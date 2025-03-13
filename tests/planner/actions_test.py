# btrfs2s3 - maintains a tree of differential backups in object storage.
#
# Copyright (C) 2025 Steven Brudenell and other contributors.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

from contextlib import ExitStack
from functools import partial
from typing import TYPE_CHECKING

import pytest

from btrfs2s3._internal.btrfsioctl import create_snap
from btrfs2s3._internal.btrfsioctl import create_subvol
from btrfs2s3._internal.btrfsioctl import subvol_info
from btrfs2s3._internal.piper import filter_pipe
from btrfs2s3._internal.planner import Actions
from btrfs2s3._internal.planner import AssessedBackup
from btrfs2s3._internal.planner import AssessedSnapshot
from btrfs2s3._internal.planner import Assessment
from btrfs2s3._internal.planner import assessment_to_actions
from btrfs2s3._internal.planner import BackupObject
from btrfs2s3._internal.planner import DeleteBackup
from btrfs2s3._internal.planner import DestroySnapshot
from btrfs2s3._internal.planner import ObjectStat
from btrfs2s3._internal.planner import Remote
from btrfs2s3._internal.planner import RenameSnapshot
from btrfs2s3._internal.planner import SnapshotDir
from btrfs2s3._internal.planner import Source
from btrfs2s3._internal.planner import UploadBackup
from btrfs2s3._internal.resolver import Flags
from btrfs2s3._internal.resolver import KeepMeta
from btrfs2s3._internal.resolver import Reasons
from btrfs2s3._internal.util import backup_of_snapshot

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from types_boto3_s3.client import S3Client

    from tests.conftest import DownloadAndPipe


@pytest.fixture
def source1_path(btrfs_mountpoint: Path) -> Path:
    path = btrfs_mountpoint / "source1"
    create_subvol(path)
    return path


@pytest.fixture
def snapshot_dir1_path(btrfs_mountpoint: Path) -> Path:
    path = btrfs_mountpoint / "snapshot_dir1"
    path.mkdir()
    return path


@pytest.fixture
def remote1(s3: S3Client, bucket: str) -> Remote:
    return Remote.create(name="test1", s3=s3, bucket=bucket)


@pytest.fixture
def stack() -> Iterator[ExitStack]:
    with ExitStack() as stack:
        yield stack


noop_pipe = partial(filter_pipe, [])


def test_noop_with_snapshot_and_backup(
    source1_path: Path, snapshot_dir1_path: Path, remote1: Remote, stack: ExitStack
) -> None:
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy")
    # Create an initial snapshot
    snapshot_path = snapshot_dir1_path / "snapshot1"
    create_snap(src=source1_path, dst=snapshot_path, read_only=True)
    info = subvol_info(snapshot_path)
    source1 = stack.enter_context(Source.create(source1_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))
    # Rename the snapshot
    snapshot_dir1.rename_snapshot(info.id, source1.get_snapshot_name(info))
    stat = remote1.upload(
        snapshot_dir=snapshot_dir1,
        snapshot_id=info.id,
        send_parent_id=None,
        key=source1.get_backup_key(backup_of_snapshot(info)),
        create_pipe=noop_pipe,
    )

    asmt = Assessment(
        snapshots={
            info.uuid: AssessedSnapshot(
                source=source1,
                snapshot_dir=snapshot_dir1,
                info=info,
                meta=KeepMeta(reasons=Reasons.MostRecent),
            )
        },
        backups={
            (remote1, info.uuid): AssessedBackup(
                source=source1,
                remote=remote1,
                info=backup_of_snapshot(info),
                stat=stat,
                key=source1.get_backup_key(backup_of_snapshot(info)),
                meta=KeepMeta(reasons=Reasons.MostRecent),
                create_pipe=noop_pipe,
            )
        },
    )

    actions = assessment_to_actions(asmt)

    assert actions == Actions(
        rename_snapshots=[], upload_backups=[], destroy_snapshots=[], delete_backups=[]
    )

    actions.execute()


def test_rename_snapshot(
    source1_path: Path, snapshot_dir1_path: Path, stack: ExitStack
) -> None:
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy")
    # Create an initial snapshot
    snapshot_path = snapshot_dir1_path / "snapshot1"
    create_snap(src=source1_path, dst=snapshot_path, read_only=True)
    info = subvol_info(snapshot_path)
    source1 = stack.enter_context(Source.create(source1_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))

    asmt = Assessment(
        snapshots={
            info.uuid: AssessedSnapshot(
                source=source1,
                snapshot_dir=snapshot_dir1,
                info=info,
                meta=KeepMeta(reasons=Reasons.MostRecent),
            )
        },
        backups={},
    )

    actions = assessment_to_actions(asmt)

    assert actions == Actions(
        rename_snapshots=[
            RenameSnapshot(
                snapshot_dir=snapshot_dir1,
                info=info,
                target_name=source1.get_snapshot_name(info),
            )
        ],
        upload_backups=[],
        destroy_snapshots=[],
        delete_backups=[],
    )

    actions.execute()

    assert (
        subvol_info(snapshot_dir1_path / source1.get_snapshot_name(info)).id == info.id
    )


def test_destroy_snapshot(
    source1_path: Path, snapshot_dir1_path: Path, stack: ExitStack
) -> None:
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy")
    # Create an initial snapshot
    snapshot_path = snapshot_dir1_path / "snapshot1"
    create_snap(src=source1_path, dst=snapshot_path, read_only=True)
    info = subvol_info(snapshot_path)
    source1 = stack.enter_context(Source.create(source1_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))

    asmt = Assessment(
        snapshots={
            info.uuid: AssessedSnapshot(
                source=source1, snapshot_dir=snapshot_dir1, info=info, meta=KeepMeta()
            )
        },
        backups={},
    )
    actions = assessment_to_actions(asmt)

    assert actions == Actions(
        rename_snapshots=[],
        upload_backups=[],
        destroy_snapshots=[DestroySnapshot(snapshot_dir=snapshot_dir1, info=info)],
        delete_backups=[],
    )

    actions.execute()

    assert list(snapshot_dir1_path.iterdir()) == []


def test_upload_backup(
    source1_path: Path,
    snapshot_dir1_path: Path,
    remote1: Remote,
    stack: ExitStack,
    download_and_pipe: DownloadAndPipe,
) -> None:
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy")
    # Create an initial snapshot
    snapshot_path = snapshot_dir1_path / "snapshot1"
    create_snap(src=source1_path, dst=snapshot_path, read_only=True)
    info = subvol_info(snapshot_path)
    source1 = stack.enter_context(Source.create(source1_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))
    # Rename the snapshot
    snapshot_dir1.rename_snapshot(info.id, source1.get_snapshot_name(info))

    asmt = Assessment(
        snapshots={
            info.uuid: AssessedSnapshot(
                source=source1,
                snapshot_dir=snapshot_dir1,
                info=info,
                meta=KeepMeta(reasons=Reasons.MostRecent),
            )
        },
        backups={
            (remote1, info.uuid): AssessedBackup(
                source=source1,
                remote=remote1,
                info=backup_of_snapshot(info),
                stat=None,
                key=source1.get_backup_key(backup_of_snapshot(info)),
                meta=KeepMeta(flags=Flags.New, reasons=Reasons.MostRecent),
                create_pipe=noop_pipe,
            )
        },
    )

    actions = assessment_to_actions(asmt)

    assert actions == Actions(
        rename_snapshots=[],
        upload_backups=[
            UploadBackup(
                remote=remote1,
                key=source1.get_backup_key(backup_of_snapshot(info)),
                snapshot_dir=snapshot_dir1,
                info=info,
                send_parent=None,
                create_pipe=noop_pipe,
            )
        ],
        destroy_snapshots=[],
        delete_backups=[],
    )

    actions.execute()

    assert remote1.get_objects(source1) == {
        info.uuid: BackupObject(
            key=source1.get_backup_key(backup_of_snapshot(info)),
            info=backup_of_snapshot(info),
            stat=ObjectStat(),
        )
    }
    download_and_pipe(
        source1.get_backup_key(backup_of_snapshot(info)), ["btrfs", "receive", "--dump"]
    )


def test_delete_backup(
    source1_path: Path, snapshot_dir1_path: Path, remote1: Remote, stack: ExitStack
) -> None:
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy")
    # Create an initial snapshot
    snapshot_path = snapshot_dir1_path / "snapshot1"
    create_snap(src=source1_path, dst=snapshot_path, read_only=True)
    info = subvol_info(snapshot_path)
    source1 = stack.enter_context(Source.create(source1_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))
    # Rename the snapshot
    snapshot_dir1.rename_snapshot(info.id, source1.get_snapshot_name(info))
    # Upload the backup
    stat = remote1.upload(
        snapshot_dir=snapshot_dir1,
        snapshot_id=info.id,
        send_parent_id=None,
        key=source1.get_backup_key(backup_of_snapshot(info)),
        create_pipe=noop_pipe,
    )

    asmt = Assessment(
        snapshots={},
        backups={
            (remote1, info.uuid): AssessedBackup(
                source=source1,
                remote=remote1,
                info=backup_of_snapshot(info),
                stat=stat,
                key=source1.get_backup_key(backup_of_snapshot(info)),
                meta=KeepMeta(),
                create_pipe=noop_pipe,
            )
        },
    )

    actions = assessment_to_actions(asmt)

    assert actions == Actions(
        rename_snapshots=[],
        upload_backups=[],
        destroy_snapshots=[],
        delete_backups=[
            DeleteBackup(
                remote=remote1,
                key=source1.get_backup_key(backup_of_snapshot(info)),
                info=backup_of_snapshot(info),
                stat=stat,
            )
        ],
    )

    actions.execute()

    assert remote1.get_objects(source1) == {}
