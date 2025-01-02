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
import time
from typing import cast
from typing import TYPE_CHECKING

import pytest

from btrfs2s3._internal.btrfsioctl import create_snap
from btrfs2s3._internal.btrfsioctl import create_subvol
from btrfs2s3._internal.btrfsioctl import subvol_info
from btrfs2s3._internal.btrfsioctl import SubvolFlag
from btrfs2s3._internal.piper import filter_pipe
from btrfs2s3._internal.planner import CreatedSnapshotArgs
from btrfs2s3._internal.planner import DeleteBackupArgs
from btrfs2s3._internal.planner import DestroySnapshotArgs
from btrfs2s3._internal.planner import KeepBackupArgs
from btrfs2s3._internal.planner import KeepSnapshotArgs
from btrfs2s3._internal.planner import ObjectStat
from btrfs2s3._internal.planner import Plan
from btrfs2s3._internal.planner import Remote
from btrfs2s3._internal.planner import RenameSnapshotArgs
from btrfs2s3._internal.planner import SnapshotDir
from btrfs2s3._internal.planner import Source
from btrfs2s3._internal.planner import UpdateArgs
from btrfs2s3._internal.planner import UploadBackupArgs
from btrfs2s3._internal.preservation import Params
from btrfs2s3._internal.preservation import Policy
from btrfs2s3._internal.resolver import Flags
from btrfs2s3._internal.resolver import KeepMeta
from btrfs2s3._internal.resolver import Reasons
from btrfs2s3._internal.s3 import iter_backups
from btrfs2s3._internal.util import backup_of_snap

if TYPE_CHECKING:
    from collections.abc import Iterator
    from collections.abc import Sequence
    from pathlib import Path

    from mypy_boto3_s3.client import S3Client

    from tests.conftest import DownloadAndPipe


@pytest.fixture
def source1_path(btrfs_mountpoint: Path) -> Path:
    path = btrfs_mountpoint / "source1"
    create_subvol(path)
    return path


@pytest.fixture
def source2_path(btrfs_mountpoint: Path) -> Path:
    path = btrfs_mountpoint / "source2"
    create_subvol(path)
    return path


@pytest.fixture
def snapshot_dir1_path(btrfs_mountpoint: Path) -> Path:
    path = btrfs_mountpoint / "snapshot_dir1"
    path.mkdir()
    return path


@pytest.fixture
def snapshot_dir2_path(btrfs_mountpoint: Path) -> Path:
    path = btrfs_mountpoint / "snapshot_dir2"
    path.mkdir()
    return path


@pytest.fixture
def remote1(s3: S3Client, bucket: str) -> Remote:
    return Remote.create(name="test1", s3=s3, bucket=bucket)


@pytest.fixture
def remote2(s3: S3Client) -> Remote:
    s3.create_bucket(Bucket="test-bucket-2")
    return Remote.create(name="test2", s3=s3, bucket="test-bucket-2")


@pytest.fixture
def stack() -> Iterator[ExitStack]:
    with ExitStack() as stack:
        yield stack


noop_pipe = partial(filter_pipe, [])


def test_create_and_backup_new_snapshot(
    download_and_pipe: DownloadAndPipe,
    source1_path: Path,
    snapshot_dir1_path: Path,
    remote1: Remote,
    stack: ExitStack,
) -> None:
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy")

    source1 = stack.enter_context(Source.create(source1_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))
    plan = Plan.create()
    with plan.update() as update:
        update(
            source=source1,
            snapshot_dir=snapshot_dir1,
            remote=remote1,
            policy=Policy(),
            create_pipe=noop_pipe,
        )

    (created,) = plan.created_snapshots.values()
    snapshot = created.snapshot
    backup = backup_of_snap(snapshot)
    key = source1.get_backup_key(backup)
    assert plan == Plan(
        keep_snapshots={
            snapshot.uuid: KeepSnapshotArgs(
                source=source1,
                snapshot_dir=snapshot_dir1,
                snapshot=snapshot,
                meta=KeepMeta(reasons=Reasons.MostRecent),
            )
        },
        keep_backups={
            snapshot.uuid: KeepBackupArgs(
                source=source1,
                remote=remote1,
                info=backup,
                stat=None,
                key=key,
                meta=KeepMeta(reasons=Reasons.MostRecent, flags=Flags.New),
            )
        },
        created_snapshots={
            snapshot.uuid: CreatedSnapshotArgs(
                source=source1, snapshot_dir=snapshot_dir1, snapshot=snapshot
            )
        },
        rename_snapshots=[
            RenameSnapshotArgs(
                snapshot_dir=snapshot_dir1,
                snapshot=snapshot,
                target_name=source1.get_snapshot_name(snapshot),
            )
        ],
        upload_backups=[
            UploadBackupArgs(
                remote=remote1,
                key=key,
                snapshot_dir=snapshot_dir1,
                snapshot=snapshot,
                send_parent=None,
                create_pipe=noop_pipe,
            )
        ],
        delete_backups=[],
        destroy_snapshots=[],
    )

    plan.execute()

    (snapshot_path,) = list(snapshot_dir1_path.iterdir())
    assert snapshot_path.name.startswith(source1.path.name)
    snapshot_info = subvol_info(snapshot_path)
    assert snapshot_info.parent_uuid == source1.info.uuid

    ((obj, backup),) = list(iter_backups(remote1.s3, remote1.bucket))
    assert backup.uuid == snapshot_info.uuid
    assert backup.parent_uuid == snapshot_info.parent_uuid
    assert backup.send_parent_uuid is None

    download_and_pipe(obj["Key"], ["btrfs", "receive", "--dump"])


def test_create_and_backup_with_parent(
    download_and_pipe: DownloadAndPipe,
    source1_path: Path,
    snapshot_dir1_path: Path,
    remote1: Remote,
    stack: ExitStack,
) -> None:
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy")
    # Create an initial snapshot
    create_snap(src=source1_path, dst=snapshot_dir1_path / "snapshot1", read_only=True)
    # Modify the source again
    (source1_path / "dummy-file").write_bytes(b"dummy2")

    # This isn't guaranteed to work at year boundaries. Can't think of a better
    # way to do it right now.
    now = time.time()
    policy = Policy(now=now, params=Params(years=1))

    source1 = stack.enter_context(Source.create(source1_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))
    plan = Plan.create()
    with plan.update() as update:
        update(
            source=source1,
            snapshot_dir=snapshot_dir1,
            remote=remote1,
            policy=policy,
            create_pipe=noop_pipe,
        )

    expected_time_span = next(policy.iter_time_spans(now))
    snapshot1 = subvol_info(snapshot_dir1_path / "snapshot1")
    backup1 = backup_of_snap(snapshot1)
    key1 = source1.get_backup_key(backup1)
    (created,) = plan.created_snapshots.values()
    snapshot2 = created.snapshot
    backup2 = backup_of_snap(snapshot2, send_parent=snapshot1)
    key2 = source1.get_backup_key(backup2)
    assert plan == Plan(
        keep_snapshots={
            snapshot1.uuid: KeepSnapshotArgs(
                source=source1,
                snapshot_dir=snapshot_dir1,
                snapshot=snapshot1,
                meta=KeepMeta(
                    reasons=Reasons.Preserved, time_spans={expected_time_span}
                ),
            ),
            snapshot2.uuid: KeepSnapshotArgs(
                source=source1,
                snapshot_dir=snapshot_dir1,
                snapshot=snapshot2,
                meta=KeepMeta(reasons=Reasons.MostRecent),
            ),
        },
        keep_backups={
            snapshot1.uuid: KeepBackupArgs(
                source=source1,
                remote=remote1,
                info=backup1,
                stat=None,
                key=key1,
                meta=KeepMeta(
                    reasons=Reasons.Preserved,
                    flags=Flags.New,
                    time_spans={expected_time_span},
                ),
            ),
            snapshot2.uuid: KeepBackupArgs(
                source=source1,
                remote=remote1,
                info=backup2,
                stat=None,
                key=key2,
                meta=KeepMeta(reasons=Reasons.MostRecent, flags=Flags.New),
            ),
        },
        created_snapshots={
            snapshot2.uuid: CreatedSnapshotArgs(
                source=source1, snapshot_dir=snapshot_dir1, snapshot=snapshot2
            )
        },
        rename_snapshots=[
            RenameSnapshotArgs(
                snapshot_dir=snapshot_dir1,
                snapshot=snapshot1,
                target_name=source1.get_snapshot_name(snapshot1),
            ),
            RenameSnapshotArgs(
                snapshot_dir=snapshot_dir1,
                snapshot=snapshot2,
                target_name=source1.get_snapshot_name(snapshot2),
            ),
        ],
        upload_backups=[
            UploadBackupArgs(
                remote=remote1,
                key=key1,
                snapshot_dir=snapshot_dir1,
                snapshot=snapshot1,
                send_parent=None,
                create_pipe=noop_pipe,
            ),
            UploadBackupArgs(
                remote=remote1,
                key=key2,
                snapshot_dir=snapshot_dir1,
                snapshot=snapshot2,
                send_parent=snapshot1,
                create_pipe=noop_pipe,
            ),
        ],
        delete_backups=[],
        destroy_snapshots=[],
    )

    plan.execute()

    (snapshot1_path, snapshot2_path) = snapshot_dir1_path.iterdir()
    assert snapshot1_path.name.startswith(source1.path.name)
    assert snapshot2_path.name.startswith(source1.path.name)

    snapshot1_info = subvol_info(snapshot1_path)
    snapshot2_info = subvol_info(snapshot2_path)
    snapshot1_info, snapshot2_info = sorted(
        (snapshot1_info, snapshot2_info), key=lambda i: i.ctransid
    )
    assert snapshot1_info.ctransid < snapshot2_info.ctransid
    assert snapshot1_info.parent_uuid == source1.info.uuid
    assert snapshot2_info.parent_uuid == source1.info.uuid

    ((full_obj, full_backup), (delta_obj, delta_backup)) = sorted(
        iter_backups(remote1.s3, remote1.bucket), key=lambda x: x[1].ctransid
    )
    assert full_backup.uuid == snapshot1_info.uuid
    assert full_backup.send_parent_uuid is None
    assert delta_backup.uuid == snapshot2_info.uuid
    assert delta_backup.send_parent_uuid == full_backup.uuid

    download_and_pipe(full_obj["Key"], ["btrfs", "receive", "--dump"])
    download_and_pipe(delta_obj["Key"], ["btrfs", "receive", "--dump"])


def test_rename_snapshot(
    source1_path: Path,
    snapshot_dir1_path: Path,
    download_and_pipe: DownloadAndPipe,
    stack: ExitStack,
    remote1: Remote,
) -> None:
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy")
    # One snapshot, but wrongly-named
    snapshot_path = (
        snapshot_dir1_path / "initial-snapshot-name-that-needs-to-be-updated"
    )
    create_snap(src=source1_path, dst=snapshot_path, read_only=True)

    source1 = stack.enter_context(Source.create(source1_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))
    plan = Plan.create()
    with plan.update() as update:
        update(
            source=source1,
            snapshot_dir=snapshot_dir1,
            remote=remote1,
            policy=Policy(),
            create_pipe=noop_pipe,
        )

    snapshot1 = subvol_info(snapshot_path)
    backup1 = backup_of_snap(snapshot1)
    key1 = source1.get_backup_key(backup1)
    assert plan == Plan(
        keep_snapshots={
            snapshot1.uuid: KeepSnapshotArgs(
                source=source1,
                snapshot_dir=snapshot_dir1,
                snapshot=snapshot1,
                meta=KeepMeta(reasons=Reasons.MostRecent),
            )
        },
        keep_backups={
            snapshot1.uuid: KeepBackupArgs(
                source=source1,
                remote=remote1,
                info=backup1,
                stat=None,
                key=key1,
                meta=KeepMeta(reasons=Reasons.MostRecent, flags=Flags.New),
            )
        },
        created_snapshots={},
        rename_snapshots=[
            RenameSnapshotArgs(
                snapshot_dir=snapshot_dir1,
                snapshot=snapshot1,
                target_name=source1.get_snapshot_name(snapshot1),
            )
        ],
        upload_backups=[
            UploadBackupArgs(
                remote=remote1,
                key=key1,
                snapshot_dir=snapshot_dir1,
                snapshot=snapshot1,
                send_parent=None,
                create_pipe=noop_pipe,
            )
        ],
        delete_backups=[],
        destroy_snapshots=[],
    )

    plan.execute()

    (snapshot,) = list(snapshot_dir1_path.iterdir())
    assert snapshot.name.startswith(source1.path.name)
    snapshot_info = subvol_info(snapshot)
    assert snapshot_info.parent_uuid == source1.info.uuid

    ((obj, backup),) = list(iter_backups(remote1.s3, remote1.bucket))
    assert backup.uuid == snapshot_info.uuid
    assert backup.parent_uuid == snapshot_info.parent_uuid
    assert backup.send_parent_uuid is None

    download_and_pipe(obj["Key"], ["btrfs", "receive", "--dump"])


def test_delete_only_snapshot_because_proposed_would_be_newer(
    source1_path: Path,
    snapshot_dir1_path: Path,
    remote1: Remote,
    download_and_pipe: DownloadAndPipe,
    stack: ExitStack,
) -> None:
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy")
    # Initial snapshot, name doesn't matter
    initial_snapshot = snapshot_dir1_path / "initial-snapshot"
    create_snap(src=source1_path, dst=initial_snapshot, read_only=True)
    # Modify the source again
    (source1_path / "dummy-file").write_bytes(b"dummy2")

    source1 = stack.enter_context(Source.create(source1_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))
    plan = Plan.create()
    with plan.update() as update:
        update(
            source=source1,
            snapshot_dir=snapshot_dir1,
            remote=remote1,
            policy=Policy(),
            create_pipe=noop_pipe,
        )

    snapshot1 = subvol_info(initial_snapshot)
    (created,) = plan.created_snapshots.values()
    snapshot2 = created.snapshot
    backup2 = backup_of_snap(snapshot2)
    key2 = source1.get_backup_key(backup2)
    assert plan == Plan(
        keep_snapshots={
            snapshot1.uuid: KeepSnapshotArgs(
                source=source1,
                snapshot_dir=snapshot_dir1,
                snapshot=snapshot1,
                meta=KeepMeta(),
            ),
            snapshot2.uuid: KeepSnapshotArgs(
                source=source1,
                snapshot_dir=snapshot_dir1,
                snapshot=snapshot2,
                meta=KeepMeta(reasons=Reasons.MostRecent),
            ),
        },
        keep_backups={
            snapshot2.uuid: KeepBackupArgs(
                source=source1,
                remote=remote1,
                info=backup2,
                stat=None,
                key=key2,
                meta=KeepMeta(reasons=Reasons.MostRecent, flags=Flags.New),
            )
        },
        created_snapshots={
            snapshot2.uuid: CreatedSnapshotArgs(
                source=source1, snapshot_dir=snapshot_dir1, snapshot=snapshot2
            )
        },
        rename_snapshots=[
            RenameSnapshotArgs(
                snapshot_dir=snapshot_dir1,
                snapshot=snapshot2,
                target_name=source1.get_snapshot_name(snapshot2),
            )
        ],
        upload_backups=[
            UploadBackupArgs(
                remote=remote1,
                key=key2,
                snapshot_dir=snapshot_dir1,
                snapshot=snapshot2,
                send_parent=None,
                create_pipe=noop_pipe,
            )
        ],
        delete_backups=[],
        destroy_snapshots=[
            DestroySnapshotArgs(snapshot_dir=snapshot_dir1, snapshot=snapshot1)
        ],
    )

    plan.execute()

    (snapshot,) = list(snapshot_dir1_path.iterdir())
    assert (snapshot / "dummy-file").read_bytes() == b"dummy2"
    assert snapshot.name.startswith(source1.path.name)
    snapshot_info = subvol_info(snapshot)
    assert snapshot_info.parent_uuid == source1.info.uuid

    ((obj, backup),) = list(iter_backups(remote1.s3, remote1.bucket))
    assert backup.uuid == snapshot_info.uuid
    assert backup.parent_uuid == snapshot_info.parent_uuid
    assert backup.send_parent_uuid is None

    download_and_pipe(obj["Key"], ["btrfs", "receive", "--dump"])


def test_ignore_read_write_snapshot(
    source1_path: Path, snapshot_dir1_path: Path, remote1: Remote, stack: ExitStack
) -> None:
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy")
    # Initial snapshot, name doesn't matter
    initial_snapshot = snapshot_dir1_path / "initial-snapshot"
    create_snap(src=source1_path, dst=initial_snapshot)

    source1 = stack.enter_context(Source.create(source1_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))
    plan = Plan.create()
    with plan.update() as update:
        update(
            source=source1,
            snapshot_dir=snapshot_dir1,
            remote=remote1,
            policy=Policy(),
            create_pipe=noop_pipe,
        )

    plan.execute()

    (snap1, snap2) = sorted(
        snapshot_dir1_path.iterdir(), key=lambda p: p == initial_snapshot
    )
    assert snap2 == initial_snapshot
    info = subvol_info(snap1)
    assert info.flags & SubvolFlag.ReadOnly


def test_ignore_snapshots_from_unrelated_sources(
    source1_path: Path,
    source2_path: Path,
    snapshot_dir1_path: Path,
    remote1: Remote,
    stack: ExitStack,
) -> None:
    # Modify some data in the sources
    (source1_path / "dummy-file").write_bytes(b"dummy")
    (source2_path / "dummy-file").write_bytes(b"dummy")
    # Initial snapshot, name doesn't matter
    ignore_snapshot = snapshot_dir1_path / "ignore-snapshot"
    create_snap(src=source2_path, dst=ignore_snapshot, read_only=True)

    source1 = stack.enter_context(Source.create(source1_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))
    plan = Plan.create()
    with plan.update() as update:
        update(
            source=source1,
            snapshot_dir=snapshot_dir1,
            remote=remote1,
            policy=Policy(),
            create_pipe=noop_pipe,
        )

    plan.execute()

    (snap1, snap2) = sorted(
        snapshot_dir1_path.iterdir(), key=lambda p: p == ignore_snapshot
    )
    assert snap2 == ignore_snapshot
    info = subvol_info(snap1)
    assert info.flags & SubvolFlag.ReadOnly


@pytest.fixture(params=[0, 1], ids=["source_a_1", "source_a_2"])
def source_a_path(
    request: pytest.FixtureRequest, source1_path: Path, source2_path: Path
) -> Path:
    return (source1_path, source2_path)[cast(int, request.param)]


@pytest.fixture(params=[0, 1], ids=["source_b_1", "source_b_2"])
def source_b_path(
    request: pytest.FixtureRequest, source1_path: Path, source2_path: Path
) -> Path:
    return (source1_path, source2_path)[cast(int, request.param)]


@pytest.fixture(params=[0, 1], ids=["snapshot_dir_a_1", "snapshot_dir_a_2"])
def snapshot_dir_a_path(
    request: pytest.FixtureRequest, snapshot_dir1_path: Path, snapshot_dir2_path: Path
) -> Path:
    return (snapshot_dir1_path, snapshot_dir2_path)[cast(int, request.param)]


@pytest.fixture(params=[0, 1], ids=["snapshot_dir_b_1", "snapshot_dir_b_2"])
def snapshot_dir_b_path(
    request: pytest.FixtureRequest, snapshot_dir1_path: Path, snapshot_dir2_path: Path
) -> Path:
    return (snapshot_dir1_path, snapshot_dir2_path)[cast(int, request.param)]


@pytest.fixture(params=[0, 1], ids=["remote_a_1", "remote_a_2"])
def remote_a(
    request: pytest.FixtureRequest, remote1: Remote, remote2: Remote
) -> Remote:
    return (remote1, remote2)[cast(int, request.param)]


@pytest.fixture(params=[0, 1], ids=["remote_b_1", "remote_b_2"])
def remote_b(
    request: pytest.FixtureRequest, remote1: Remote, remote2: Remote
) -> Remote:
    return (remote1, remote2)[cast(int, request.param)]


@pytest.fixture
def updates(
    source_a_path: Path,
    source_b_path: Path,
    snapshot_dir_a_path: Path,
    snapshot_dir_b_path: Path,
    remote_a: Remote,
    remote_b: Remote,
    stack: ExitStack,
) -> Sequence[UpdateArgs]:
    if source_a_path == source_b_path and snapshot_dir_a_path != snapshot_dir_b_path:
        pytest.xfail(
            "updating with the same source and multiple snapshot dirs is not supported"
        )
    if source_a_path == source_b_path and remote_a == remote_b:
        pytest.xfail("updating the same source/remote pair twice is not supported")

    # Modify some data in each source
    (source_a_path / "dummy-file-a").write_bytes(b"dummy")
    (source_b_path / "dummy-file-b").write_bytes(b"dummy")

    source_a = stack.enter_context(Source.create(source_a_path))
    if source_a_path == source_b_path:
        source_b = source_a
    else:
        source_b = stack.enter_context(Source.create(source_b_path))
    snapshot_dir_a = stack.enter_context(SnapshotDir.create(snapshot_dir_a_path))
    if snapshot_dir_a_path == snapshot_dir_b_path:
        snapshot_dir_b = snapshot_dir_a
    else:
        snapshot_dir_b = stack.enter_context(SnapshotDir.create(snapshot_dir_b_path))

    return (
        UpdateArgs(
            source=source_a,
            snapshot_dir=snapshot_dir_a,
            remote=remote_a,
            policy=Policy(),
            create_pipe=noop_pipe,
        ),
        UpdateArgs(
            source=source_b,
            snapshot_dir=snapshot_dir_b,
            remote=remote_b,
            policy=Policy(),
            create_pipe=noop_pipe,
        ),
    )


def test_multiple_updates(
    updates: Sequence[UpdateArgs], download_and_pipe: DownloadAndPipe
) -> None:
    plan = Plan.create()
    with plan.update() as update:
        for args in updates:
            update(**args._asdict())

    plan.execute()

    for args in updates:
        paths = list(args.snapshot_dir.path.iterdir())
        (snapshot_path,) = (
            p for p in paths if subvol_info(p).parent_uuid == args.source.info.uuid
        )
        snapshot_info = subvol_info(snapshot_path)
        assert snapshot_path.name.startswith(args.source.path.name)

        backups = list(iter_backups(args.remote.s3, args.remote.bucket))
        ((obj, backup),) = ((o, b) for o, b in backups if b.uuid == snapshot_info.uuid)
        assert backup.parent_uuid == snapshot_info.parent_uuid
        assert backup.send_parent_uuid is None

        download_and_pipe(
            obj["Key"], ["btrfs", "receive", "--dump"], bucket=args.remote.bucket
        )


def test_undo_created_snapshots(
    source1_path: Path, snapshot_dir1_path: Path, remote1: Remote, stack: ExitStack
) -> None:
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy")

    source1 = stack.enter_context(Source.create(source1_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))
    plan = Plan.create()
    with plan.update() as update:
        update(
            source=source1,
            snapshot_dir=snapshot_dir1,
            remote=remote1,
            policy=Policy(),
            create_pipe=noop_pipe,
        )

    plan.execute()

    plan.undo_created_snapshots()

    assert list(snapshot_dir1_path.iterdir()) == []


def test_keep_existing_backup(
    source1_path: Path, snapshot_dir1_path: Path, stack: ExitStack, remote1: Remote
) -> None:
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy")
    # Create an initial snapshot
    snap_path = snapshot_dir1_path / "snapshot1"
    create_snap(src=source1_path, dst=snap_path, read_only=True)
    snapshot = subvol_info(snap_path)
    # Create backup of snapshot
    backup = backup_of_snap(snapshot)
    source1 = stack.enter_context(Source.create(source1_path))
    key = source1.get_backup_key(backup)
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))
    remote1.upload(
        snapshot_dir=snapshot_dir1,
        snapshot_id=snapshot.id,
        send_parent_id=None,
        key=key,
        create_pipe=noop_pipe,
    )

    plan = Plan.create()
    with plan.update() as update:
        update(
            source=source1,
            snapshot_dir=snapshot_dir1,
            remote=remote1,
            policy=Policy(),
            create_pipe=noop_pipe,
        )

    assert plan == Plan(
        keep_snapshots={
            snapshot.uuid: KeepSnapshotArgs(
                source=source1,
                snapshot_dir=snapshot_dir1,
                snapshot=snapshot,
                meta=KeepMeta(reasons=Reasons.MostRecent),
            )
        },
        keep_backups={
            snapshot.uuid: KeepBackupArgs(
                source=source1,
                remote=remote1,
                info=backup,
                stat=ObjectStat(size=None, storage_class=None),
                key=key,
                meta=KeepMeta(reasons=Reasons.MostRecent),
            )
        },
        created_snapshots={},
        rename_snapshots=[
            RenameSnapshotArgs(
                snapshot_dir=snapshot_dir1,
                snapshot=snapshot,
                target_name=source1.get_snapshot_name(snapshot),
            )
        ],
        upload_backups=[],
        delete_backups=[],
        destroy_snapshots=[],
    )


def test_any_actions(
    source1_path: Path, snapshot_dir1_path: Path, stack: ExitStack, remote1: Remote
) -> None:
    snapshot_path = snapshot_dir1_path / "snapshot"
    create_snap(src=source1_path, dst=snapshot_path, read_only=True)
    snapshot = subvol_info(snapshot_path)
    backup = backup_of_snap(snapshot)
    source1 = stack.enter_context(Source.create(source1_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))
    key = source1.get_backup_key(backup)

    plan = Plan(
        keep_snapshots={},
        keep_backups={},
        created_snapshots={},
        rename_snapshots=[
            RenameSnapshotArgs(
                snapshot_dir=snapshot_dir1,
                snapshot=snapshot,
                target_name=source1.get_snapshot_name(snapshot),
            )
        ],
        upload_backups=[],
        delete_backups=[],
        destroy_snapshots=[],
    )
    assert plan.any_actions()

    plan = Plan(
        keep_snapshots={},
        keep_backups={},
        created_snapshots={},
        rename_snapshots=[],
        upload_backups=[
            UploadBackupArgs(
                remote=remote1,
                key=key,
                snapshot_dir=snapshot_dir1,
                snapshot=snapshot,
                send_parent=None,
                create_pipe=noop_pipe,
            )
        ],
        delete_backups=[],
        destroy_snapshots=[],
    )
    assert plan.any_actions()

    plan = Plan(
        keep_snapshots={},
        keep_backups={},
        created_snapshots={},
        rename_snapshots=[],
        upload_backups=[],
        delete_backups=[
            DeleteBackupArgs(
                remote=remote1, key=key, info=backup, stat=ObjectStat.create()
            )
        ],
        destroy_snapshots=[],
    )
    assert plan.any_actions()

    plan = Plan(
        keep_snapshots={},
        keep_backups={},
        created_snapshots={},
        rename_snapshots=[],
        upload_backups=[],
        delete_backups=[],
        destroy_snapshots=[
            DestroySnapshotArgs(snapshot_dir=snapshot_dir1, snapshot=snapshot)
        ],
    )
    assert plan.any_actions()

    plan = Plan.create()
    assert not plan.any_actions()
