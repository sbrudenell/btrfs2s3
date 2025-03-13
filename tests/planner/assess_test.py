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
from typing import TYPE_CHECKING

import pytest

from btrfs2s3._internal.btrfsioctl import create_snap
from btrfs2s3._internal.btrfsioctl import create_subvol
from btrfs2s3._internal.btrfsioctl import subvol_info
from btrfs2s3._internal.piper import filter_pipe
from btrfs2s3._internal.planner import assess
from btrfs2s3._internal.planner import AssessedBackup
from btrfs2s3._internal.planner import AssessedSnapshot
from btrfs2s3._internal.planner import Assessment
from btrfs2s3._internal.planner import ConfigTuple
from btrfs2s3._internal.planner import destroy_new_snapshots
from btrfs2s3._internal.planner import Remote
from btrfs2s3._internal.planner import SnapshotDir
from btrfs2s3._internal.planner import Source
from btrfs2s3._internal.preservation import Params
from btrfs2s3._internal.preservation import Policy
from btrfs2s3._internal.resolver import Flags
from btrfs2s3._internal.resolver import KeepMeta
from btrfs2s3._internal.resolver import Reasons
from btrfs2s3._internal.util import backup_of_snapshot

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from types_boto3_s3.client import S3Client


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


def test_one_snapshot(
    source1_path: Path, snapshot_dir1_path: Path, remote1: Remote, stack: ExitStack
) -> None:
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy")
    # Create an initial snapshot
    snapshot_path = snapshot_dir1_path / "snapshot1"
    create_snap(src=source1_path, dst=snapshot_path, read_only=True)

    source1 = stack.enter_context(Source.create(source1_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))
    asmt = assess(
        ConfigTuple(
            source=source1,
            snapshot_dir=snapshot_dir1,
            remote=remote1,
            policy=Policy(),
            create_pipe=noop_pipe,
        )
    )

    info = subvol_info(snapshot_path)
    assert asmt == Assessment(
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
                meta=KeepMeta(reasons=Reasons.MostRecent, flags=Flags.New),
                create_pipe=noop_pipe,
            )
        },
    )


def test_create_snapshot_none_exist(
    source1_path: Path, snapshot_dir1_path: Path, remote1: Remote, stack: ExitStack
) -> None:
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy")

    source1 = stack.enter_context(Source.create(source1_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))
    asmt = assess(
        ConfigTuple(
            source=source1,
            snapshot_dir=snapshot_dir1,
            remote=remote1,
            policy=Policy(),
            create_pipe=noop_pipe,
        )
    )

    info = subvol_info(snapshot_dir1_path / source1.get_new_snapshot_name())
    assert asmt == Assessment(
        snapshots={
            info.uuid: AssessedSnapshot(
                source=source1,
                snapshot_dir=snapshot_dir1,
                info=info,
                meta=KeepMeta(reasons=Reasons.MostRecent, flags=Flags.New),
            )
        },
        backups={
            (remote1, info.uuid): AssessedBackup(
                source=source1,
                remote=remote1,
                info=backup_of_snapshot(info),
                stat=None,
                key=source1.get_backup_key(backup_of_snapshot(info)),
                meta=KeepMeta(reasons=Reasons.MostRecent, flags=Flags.New),
                create_pipe=noop_pipe,
            )
        },
    )


def test_create_snapshot_one_exists(
    source1_path: Path, snapshot_dir1_path: Path, remote1: Remote, stack: ExitStack
) -> None:
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy")
    # Create an initial snapshot
    snapshot1_path = snapshot_dir1_path / "snapshot1"
    create_snap(src=source1_path, dst=snapshot1_path, read_only=True)
    info1 = subvol_info(snapshot1_path)
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy2")

    source1 = stack.enter_context(Source.create(source1_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))
    asmt = assess(
        ConfigTuple(
            source=source1,
            snapshot_dir=snapshot_dir1,
            remote=remote1,
            policy=Policy(),
            create_pipe=noop_pipe,
        )
    )

    info2 = subvol_info(snapshot_dir1_path / source1.get_new_snapshot_name())
    assert asmt == Assessment(
        snapshots={
            info1.uuid: AssessedSnapshot(
                source=source1, snapshot_dir=snapshot_dir1, info=info1, meta=KeepMeta()
            ),
            info2.uuid: AssessedSnapshot(
                source=source1,
                snapshot_dir=snapshot_dir1,
                info=info2,
                meta=KeepMeta(reasons=Reasons.MostRecent, flags=Flags.New),
            ),
        },
        backups={
            (remote1, info2.uuid): AssessedBackup(
                source=source1,
                remote=remote1,
                info=backup_of_snapshot(info2),
                stat=None,
                key=source1.get_backup_key(backup_of_snapshot(info2)),
                meta=KeepMeta(reasons=Reasons.MostRecent, flags=Flags.New),
                create_pipe=noop_pipe,
            )
        },
    )


def test_create_snapshot_and_destroy_new(
    source1_path: Path, snapshot_dir1_path: Path, remote1: Remote, stack: ExitStack
) -> None:
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy")
    # Create an initial snapshot
    snapshot1_path = snapshot_dir1_path / "snapshot1"
    create_snap(src=source1_path, dst=snapshot1_path, read_only=True)
    info1 = subvol_info(snapshot1_path)
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy2")

    source1 = stack.enter_context(Source.create(source1_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))
    asmt = assess(
        ConfigTuple(
            source=source1,
            snapshot_dir=snapshot_dir1,
            remote=remote1,
            policy=Policy(),
            create_pipe=noop_pipe,
        )
    )

    info2 = subvol_info(snapshot_dir1_path / source1.get_new_snapshot_name())
    assert asmt == Assessment(
        snapshots={
            info1.uuid: AssessedSnapshot(
                source=source1, snapshot_dir=snapshot_dir1, info=info1, meta=KeepMeta()
            ),
            info2.uuid: AssessedSnapshot(
                source=source1,
                snapshot_dir=snapshot_dir1,
                info=info2,
                meta=KeepMeta(reasons=Reasons.MostRecent, flags=Flags.New),
            ),
        },
        backups={
            (remote1, info2.uuid): AssessedBackup(
                source=source1,
                remote=remote1,
                info=backup_of_snapshot(info2),
                stat=None,
                key=source1.get_backup_key(backup_of_snapshot(info2)),
                meta=KeepMeta(reasons=Reasons.MostRecent, flags=Flags.New),
                create_pipe=noop_pipe,
            )
        },
    )

    destroy_new_snapshots(asmt)
    assert snapshot_dir1.get_snapshots(source1) == {info1.uuid: info1}


def test_keep_existing_backup(
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
    stat = remote1.upload(
        snapshot_dir=snapshot_dir1,
        snapshot_id=info.id,
        send_parent_id=None,
        key=source1.get_backup_key(backup_of_snapshot(info)),
        create_pipe=noop_pipe,
    )

    asmt = assess(
        ConfigTuple(
            source=source1,
            snapshot_dir=snapshot_dir1,
            remote=remote1,
            policy=Policy(),
            create_pipe=noop_pipe,
        )
    )

    assert asmt == Assessment(
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


def test_backup_with_parent(
    source1_path: Path, snapshot_dir1_path: Path, remote1: Remote, stack: ExitStack
) -> None:
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy")
    # Create an initial snapshot
    snapshot1_path = snapshot_dir1_path / "snapshot1"
    create_snap(src=source1_path, dst=snapshot1_path, read_only=True)
    # Modify the source again
    (source1_path / "dummy-file").write_bytes(b"dummy2")
    # Create a second snapshot
    snapshot2_path = snapshot_dir1_path / "snapshot2"
    create_snap(src=source1_path, dst=snapshot2_path, read_only=True)

    # This isn't guaranteed to work at year boundaries. Can't think of a better
    # way to do it right now.
    now = time.time()
    policy = Policy(now=now, params=Params(years=1))

    source1 = stack.enter_context(Source.create(source1_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))
    asmt = assess(
        ConfigTuple(
            source=source1,
            snapshot_dir=snapshot_dir1,
            remote=remote1,
            policy=policy,
            create_pipe=noop_pipe,
        )
    )

    expected_time_span = next(policy.iter_time_spans(now))
    info1 = subvol_info(snapshot1_path)
    info2 = subvol_info(snapshot2_path)
    assert asmt == Assessment(
        snapshots={
            info1.uuid: AssessedSnapshot(
                source=source1,
                snapshot_dir=snapshot_dir1,
                info=info1,
                meta=KeepMeta(
                    reasons=Reasons.Preserved, time_spans={expected_time_span}
                ),
            ),
            info2.uuid: AssessedSnapshot(
                source=source1,
                snapshot_dir=snapshot_dir1,
                info=info2,
                meta=KeepMeta(reasons=Reasons.MostRecent),
            ),
        },
        backups={
            (remote1, info1.uuid): AssessedBackup(
                source=source1,
                remote=remote1,
                info=backup_of_snapshot(info1),
                stat=None,
                key=source1.get_backup_key(backup_of_snapshot(info1)),
                meta=KeepMeta(
                    reasons=Reasons.Preserved,
                    flags=Flags.New,
                    time_spans={expected_time_span},
                ),
                create_pipe=noop_pipe,
            ),
            (remote1, info2.uuid): AssessedBackup(
                source=source1,
                remote=remote1,
                info=backup_of_snapshot(info2, send_parent=info1),
                stat=None,
                key=source1.get_backup_key(
                    backup_of_snapshot(info2, send_parent=info1)
                ),
                meta=KeepMeta(reasons=Reasons.MostRecent, flags=Flags.New),
                create_pipe=noop_pipe,
            ),
        },
    )


def test_destroy_snapshot(
    source1_path: Path, snapshot_dir1_path: Path, remote1: Remote, stack: ExitStack
) -> None:
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy")
    # Create an initial snapshot
    snapshot1_path = snapshot_dir1_path / "snapshot1"
    create_snap(src=source1_path, dst=snapshot1_path, read_only=True)
    # Modify the source again
    (source1_path / "dummy-file").write_bytes(b"dummy2")
    # Create a second snapshot
    snapshot2_path = snapshot_dir1_path / "snapshot2"
    create_snap(src=source1_path, dst=snapshot2_path, read_only=True)

    source1 = stack.enter_context(Source.create(source1_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))
    asmt = assess(
        ConfigTuple(
            source=source1,
            snapshot_dir=snapshot_dir1,
            remote=remote1,
            policy=Policy(),
            create_pipe=noop_pipe,
        )
    )

    info1 = subvol_info(snapshot1_path)
    info2 = subvol_info(snapshot2_path)
    assert asmt == Assessment(
        snapshots={
            info1.uuid: AssessedSnapshot(
                source=source1, snapshot_dir=snapshot_dir1, info=info1, meta=KeepMeta()
            ),
            info2.uuid: AssessedSnapshot(
                source=source1,
                snapshot_dir=snapshot_dir1,
                info=info2,
                meta=KeepMeta(reasons=Reasons.MostRecent),
            ),
        },
        backups={
            (remote1, info2.uuid): AssessedBackup(
                source=source1,
                remote=remote1,
                info=backup_of_snapshot(info2),
                stat=None,
                key=source1.get_backup_key(backup_of_snapshot(info2)),
                meta=KeepMeta(reasons=Reasons.MostRecent, flags=Flags.New),
                create_pipe=noop_pipe,
            )
        },
    )


def test_delete_backup(
    source1_path: Path, snapshot_dir1_path: Path, remote1: Remote, stack: ExitStack
) -> None:
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy")
    # Create an initial snapshot
    snapshot1_path = snapshot_dir1_path / "snapshot1"
    create_snap(src=source1_path, dst=snapshot1_path, read_only=True)
    # Modify the source again
    (source1_path / "dummy-file").write_bytes(b"dummy2")
    # Create a second snapshot
    snapshot2_path = snapshot_dir1_path / "snapshot2"
    create_snap(src=source1_path, dst=snapshot2_path, read_only=True)
    info1 = subvol_info(snapshot1_path)
    info2 = subvol_info(snapshot2_path)
    source1 = stack.enter_context(Source.create(source1_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))
    # Upload a backup of the initial snapshot
    stat1 = remote1.upload(
        snapshot_dir=snapshot_dir1,
        snapshot_id=info1.id,
        send_parent_id=None,
        key=source1.get_backup_key(backup_of_snapshot(info1)),
        create_pipe=noop_pipe,
    )
    asmt = assess(
        ConfigTuple(
            source=source1,
            snapshot_dir=snapshot_dir1,
            remote=remote1,
            policy=Policy(),
            create_pipe=noop_pipe,
        )
    )

    assert asmt == Assessment(
        snapshots={
            info1.uuid: AssessedSnapshot(
                source=source1, snapshot_dir=snapshot_dir1, info=info1, meta=KeepMeta()
            ),
            info2.uuid: AssessedSnapshot(
                source=source1,
                snapshot_dir=snapshot_dir1,
                info=info2,
                meta=KeepMeta(reasons=Reasons.MostRecent),
            ),
        },
        backups={
            (remote1, info1.uuid): AssessedBackup(
                source=source1,
                remote=remote1,
                info=backup_of_snapshot(info1),
                stat=stat1,
                key=source1.get_backup_key(backup_of_snapshot(info1)),
                meta=KeepMeta(),
                create_pipe=noop_pipe,
            ),
            (remote1, info2.uuid): AssessedBackup(
                source=source1,
                remote=remote1,
                info=backup_of_snapshot(info2),
                stat=None,
                key=source1.get_backup_key(backup_of_snapshot(info2)),
                meta=KeepMeta(reasons=Reasons.MostRecent, flags=Flags.New),
                create_pipe=noop_pipe,
            ),
        },
    )


def test_one_source_two_remotes(
    source1_path: Path,
    snapshot_dir1_path: Path,
    remote1: Remote,
    remote2: Remote,
    stack: ExitStack,
) -> None:
    # Modify some data in the source
    (source1_path / "dummy-file").write_bytes(b"dummy")
    # Create an initial snapshot
    snapshot_path = snapshot_dir1_path / "snapshot"
    create_snap(src=source1_path, dst=snapshot_path, read_only=True)
    now = time.time()
    policy1 = Policy(now=now, params=Params(months=1))
    policy2 = Policy(now=now, params=Params(years=1))

    source1 = stack.enter_context(Source.create(source1_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))
    asmt = assess(
        ConfigTuple(
            source=source1,
            snapshot_dir=snapshot_dir1,
            remote=remote1,
            policy=policy1,
            create_pipe=noop_pipe,
        ),
        ConfigTuple(
            source=source1,
            snapshot_dir=snapshot_dir1,
            remote=remote2,
            policy=policy2,
            create_pipe=noop_pipe,
        ),
    )

    info = subvol_info(snapshot_path)
    policy1_expected_time_span = next(policy1.iter_time_spans(now))
    policy2_expected_time_span = next(policy2.iter_time_spans(now))
    assert asmt == Assessment(
        snapshots={
            info.uuid: AssessedSnapshot(
                source=source1,
                snapshot_dir=snapshot_dir1,
                info=info,
                meta=KeepMeta(
                    reasons=Reasons.MostRecent | Reasons.Preserved,
                    time_spans={policy1_expected_time_span, policy2_expected_time_span},
                ),
            )
        },
        backups={
            (remote1, info.uuid): AssessedBackup(
                source=source1,
                remote=remote1,
                info=backup_of_snapshot(info),
                stat=None,
                key=source1.get_backup_key(backup_of_snapshot(info)),
                meta=KeepMeta(
                    reasons=Reasons.MostRecent | Reasons.Preserved,
                    flags=Flags.New,
                    time_spans={policy1_expected_time_span},
                ),
                create_pipe=noop_pipe,
            ),
            (remote2, info.uuid): AssessedBackup(
                source=source1,
                remote=remote2,
                info=backup_of_snapshot(info),
                stat=None,
                key=source1.get_backup_key(backup_of_snapshot(info)),
                meta=KeepMeta(
                    reasons=Reasons.MostRecent | Reasons.Preserved,
                    flags=Flags.New,
                    time_spans={policy2_expected_time_span},
                ),
                create_pipe=noop_pipe,
            ),
        },
    )


@pytest.mark.parametrize(
    "shared_snapshot_dir",
    [True, False],
    ids=["shared_snapshot_dir", "separate_snapshot_dirs"],
)
@pytest.mark.parametrize(
    "shared_remote", [True, False], ids=["shared_remote", "separate_remotes"]
)
def test_two_sources(
    source1_path: Path,
    source2_path: Path,
    snapshot_dir1_path: Path,
    snapshot_dir2_path: Path,
    remote1: Remote,
    remote2: Remote,
    stack: ExitStack,
    shared_snapshot_dir: bool,  # noqa: FBT001
    shared_remote: bool,  # noqa: FBT001
) -> None:
    if shared_snapshot_dir:
        snapshot_dir2_path = snapshot_dir1_path
    if shared_remote:
        remote2 = remote1
    # Modify some data in the sources
    (source1_path / "dummy-file").write_bytes(b"dummy")
    (source2_path / "dummy-file").write_bytes(b"dummy")
    # Create snapshots
    snapshot1_path = snapshot_dir1_path / "snapshot1"
    snapshot2_path = snapshot_dir2_path / "snapshot2"
    create_snap(src=source1_path, dst=snapshot1_path, read_only=True)
    create_snap(src=source2_path, dst=snapshot2_path, read_only=True)
    now = time.time()
    policy1 = Policy(now=now, params=Params(months=1))
    policy2 = Policy(now=now, params=Params(years=1))

    source1 = stack.enter_context(Source.create(source1_path))
    source2 = stack.enter_context(Source.create(source2_path))
    snapshot_dir1 = stack.enter_context(SnapshotDir.create(snapshot_dir1_path))
    if shared_snapshot_dir:
        snapshot_dir2 = snapshot_dir1
    else:
        snapshot_dir2 = stack.enter_context(SnapshotDir.create(snapshot_dir2_path))
    asmt = assess(
        ConfigTuple(
            source=source1,
            snapshot_dir=snapshot_dir1,
            remote=remote1,
            policy=policy1,
            create_pipe=noop_pipe,
        ),
        ConfigTuple(
            source=source2,
            snapshot_dir=snapshot_dir2,
            remote=remote2,
            policy=policy2,
            create_pipe=noop_pipe,
        ),
    )

    info1 = subvol_info(snapshot1_path)
    info2 = subvol_info(snapshot2_path)
    policy1_expected_time_span = next(policy1.iter_time_spans(now))
    policy2_expected_time_span = next(policy2.iter_time_spans(now))
    assert asmt == Assessment(
        snapshots={
            info1.uuid: AssessedSnapshot(
                source=source1,
                snapshot_dir=snapshot_dir1,
                info=info1,
                meta=KeepMeta(
                    reasons=Reasons.MostRecent | Reasons.Preserved,
                    time_spans={policy1_expected_time_span},
                ),
            ),
            info2.uuid: AssessedSnapshot(
                source=source2,
                snapshot_dir=snapshot_dir2,
                info=info2,
                meta=KeepMeta(
                    reasons=Reasons.MostRecent | Reasons.Preserved,
                    time_spans={policy2_expected_time_span},
                ),
            ),
        },
        backups={
            (remote1, info1.uuid): AssessedBackup(
                source=source1,
                remote=remote1,
                info=backup_of_snapshot(info1),
                stat=None,
                key=source1.get_backup_key(backup_of_snapshot(info1)),
                meta=KeepMeta(
                    reasons=Reasons.MostRecent | Reasons.Preserved,
                    flags=Flags.New,
                    time_spans={policy1_expected_time_span},
                ),
                create_pipe=noop_pipe,
            ),
            (remote2, info2.uuid): AssessedBackup(
                source=source2,
                remote=remote2,
                info=backup_of_snapshot(info2),
                stat=None,
                key=source2.get_backup_key(backup_of_snapshot(info2)),
                meta=KeepMeta(
                    reasons=Reasons.MostRecent | Reasons.Preserved,
                    flags=Flags.New,
                    time_spans={policy2_expected_time_span},
                ),
                create_pipe=noop_pipe,
            ),
        },
    )
