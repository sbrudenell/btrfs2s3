# btrfs2s3 - maintains a tree of differential backups in object storage.
#
# Copyright (C) 2024 Steven Brudenell and other contributors.
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

from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

import arrow
import pytest

from btrfs2s3._internal.btrfsioctl import SubvolInfo
from btrfs2s3._internal.commands.update2 import print_plan
from btrfs2s3._internal.piper import filter_pipe
from btrfs2s3._internal.planner import AssessedBackup
from btrfs2s3._internal.planner import AssessedSnapshot
from btrfs2s3._internal.planner import Assessment
from btrfs2s3._internal.planner import assessment_to_actions
from btrfs2s3._internal.planner import BackupObject
from btrfs2s3._internal.planner import ObjectStat
from btrfs2s3._internal.planner import Remote
from btrfs2s3._internal.planner import SnapshotDir
from btrfs2s3._internal.planner import Source
from btrfs2s3._internal.resolver import Flags
from btrfs2s3._internal.resolver import KeepMeta
from btrfs2s3._internal.resolver import Reasons
from btrfs2s3._internal.util import backup_of_snapshot

if TYPE_CHECKING:
    from rich.console import Console
    from types_boto3_s3.client import S3Client


def _t(t: str) -> float:
    return arrow.get(t).timestamp()


noop_pipe = partial(filter_pipe, [])


@pytest.fixture
def golden_asmt(s3: S3Client) -> Assessment:
    source1 = Source(
        path=Path("/path/to/source1"),
        info=SubvolInfo(
            id=12345, uuid=b"\01" * 16, ctransid=123, ctime=_t("2006-01-15")
        ),
        fd=-1,
    )
    source2 = Source(
        path=Path("/path/to/source2"),
        info=SubvolInfo(
            id=23456, uuid=b"\02" * 16, ctransid=234, ctime=_t("2006-02-15")
        ),
        fd=-1,
    )
    source3 = Source(
        path=Path("/path/to/source3"),
        info=SubvolInfo(
            id=34567, uuid=b"\x03" * 16, ctransid=345, ctime=_t("2006-03-15")
        ),
        fd=-1,
    )
    snap1_1 = SubvolInfo(
        id=source1.info.id + 1,
        uuid=b"\x11" * 16,
        parent_uuid=source1.info.uuid,
        ctransid=source1.info.ctransid - 3,
        ctime=source1.info.ctime - 86400,
    )
    snap1_2 = SubvolInfo(
        id=source1.info.id + 2,
        uuid=b"\x12" * 16,
        parent_uuid=source1.info.uuid,
        ctransid=source1.info.ctransid - 2,
        ctime=source1.info.ctime - 3600,
    )
    snap2_1 = SubvolInfo(
        id=source2.info.id + 3,
        uuid=b"\x21" * 16,
        parent_uuid=source2.info.uuid,
        ctransid=source2.info.ctransid - 3,
        ctime=source2.info.ctime - 86400,
    )
    snap2_2 = SubvolInfo(
        id=source2.info.id + 4,
        uuid=b"\x22" * 16,
        parent_uuid=source2.info.uuid,
        ctransid=source2.info.ctransid - 2,
        ctime=source2.info.ctime - 3600,
    )
    snap3_1 = SubvolInfo(
        id=source3.info.id + 5,
        uuid=b"\x31" * 16,
        parent_uuid=source3.info.uuid,
        ctransid=source3.info.ctransid - 3,
        ctime=source3.info.ctime - 86400,
    )
    snap3_2 = SubvolInfo(
        id=source3.info.id + 6,
        uuid=b"\x32" * 16,
        parent_uuid=source3.info.uuid,
        ctransid=source3.info.ctransid - 2,
        ctime=source3.info.ctime - 3600,
    )

    snapshot_dir1 = SnapshotDir(
        path=Path("/path/to/snapshot_dir1"),
        dir_fd=-1,
        snapshots=[("snap1_1", snap1_1), ("snap1_2", snap1_2)],
    )
    snapshot_dir2 = SnapshotDir(
        path=Path("/path/to/snapshot_dir2"),
        dir_fd=-1,
        snapshots=[
            ("snap2_1", snap2_1),
            ("snap2_2", snap2_2),
            ("snap3_1", snap3_1),
            ("snap3_2", snap3_2),
        ],
    )

    backup1_1 = backup_of_snapshot(snap1_1)
    backup1_2 = backup_of_snapshot(snap1_2, send_parent=backup1_1)
    backup2_1 = backup_of_snapshot(snap2_1)
    backup2_2 = backup_of_snapshot(snap2_2, send_parent=backup2_1)
    backup3_1 = backup_of_snapshot(snap3_1)
    backup3_2 = backup_of_snapshot(snap3_2, send_parent=backup3_1)

    obj1_1 = BackupObject(
        key=source1.get_backup_key(backup1_1),
        info=backup1_1,
        stat=ObjectStat(size=1 * 2**30, storage_class="DEEP_ARCHIVE"),
    )
    obj1_2 = BackupObject(
        key=source1.get_backup_key(backup1_2),
        info=backup1_2,
        stat=ObjectStat(size=2 * 2**30, storage_class="STANDARD"),
    )
    obj2_1 = BackupObject(
        key=source2.get_backup_key(backup2_1),
        info=backup2_1,
        stat=ObjectStat(size=3 * 2**30, storage_class="DEEP_ARCHIVE"),
    )
    obj2_2 = BackupObject(
        key=source2.get_backup_key(backup2_2),
        info=backup2_2,
        stat=ObjectStat(size=4 * 2**30),
    )
    obj3_1 = BackupObject(
        key=source3.get_backup_key(backup3_1),
        info=backup3_1,
        stat=ObjectStat(storage_class="DEEP_ARCHIVE"),
    )
    obj3_2 = BackupObject(
        key=source3.get_backup_key(backup3_2), info=backup3_2, stat=ObjectStat()
    )

    remote1 = Remote(
        name="remote1",
        s3=s3,
        bucket="bucket1",
        objects=[obj1_1, obj1_2, obj2_1, obj2_2],
    )
    remote2 = Remote(name="remote2", s3=s3, bucket="bucket2", objects=[obj3_1, obj3_2])

    meta = KeepMeta(reasons=Reasons.Preserved, time_spans={(_t("2006"), _t("2007"))})

    return Assessment(
        snapshots={
            snap1_1.uuid: AssessedSnapshot(
                source=source1, info=snap1_1, snapshot_dir=snapshot_dir1, meta=meta
            ),
            snap1_2.uuid: AssessedSnapshot(
                source=source1, info=snap1_2, snapshot_dir=snapshot_dir1, meta=meta
            ),
            snap2_1.uuid: AssessedSnapshot(
                source=source2, info=snap2_1, snapshot_dir=snapshot_dir2, meta=meta
            ),
            snap2_2.uuid: AssessedSnapshot(
                source=source2, info=snap2_2, snapshot_dir=snapshot_dir2, meta=meta
            ),
            snap3_1.uuid: AssessedSnapshot(
                source=source3, info=snap3_1, snapshot_dir=snapshot_dir2, meta=meta
            ),
            snap3_2.uuid: AssessedSnapshot(
                source=source3, info=snap3_2, snapshot_dir=snapshot_dir2, meta=meta
            ),
        },
        backups={
            (remote1, backup1_1.uuid): AssessedBackup(
                source=source1,
                remote=remote1,
                info=backup1_1,
                stat=obj1_1.stat,
                key=obj1_1.key,
                meta=meta,
                create_pipe=noop_pipe,
            ),
            (remote1, backup1_2.uuid): AssessedBackup(
                source=source1,
                remote=remote1,
                info=backup1_2,
                stat=obj1_2.stat,
                key=obj1_2.key,
                meta=meta,
                create_pipe=noop_pipe,
            ),
            (remote1, backup2_1.uuid): AssessedBackup(
                source=source2,
                remote=remote1,
                info=backup2_1,
                stat=obj2_1.stat,
                key=obj2_1.key,
                meta=meta,
                create_pipe=noop_pipe,
            ),
            (remote1, backup2_2.uuid): AssessedBackup(
                source=source2,
                remote=remote1,
                info=backup2_2,
                stat=obj2_2.stat,
                key=obj2_2.key,
                meta=meta,
                create_pipe=noop_pipe,
            ),
            (remote2, backup3_1.uuid): AssessedBackup(
                source=source3,
                remote=remote2,
                info=backup3_1,
                stat=obj3_1.stat,
                key=obj3_1.key,
                meta=meta,
                create_pipe=noop_pipe,
            ),
            (remote2, backup3_2.uuid): AssessedBackup(
                source=source3,
                remote=remote2,
                info=backup3_2,
                stat=obj3_2.stat,
                key=obj3_2.key,
                meta=meta,
                create_pipe=noop_pipe,
            ),
        },
    )


def test_print_plan(golden_asmt: Assessment, goldifyconsole: Console) -> None:
    print_plan(
        console=goldifyconsole,
        assessment=golden_asmt,
        actions=assessment_to_actions(golden_asmt),
    )


@pytest.mark.parametrize(
    "meta",
    [
        KeepMeta(),
        KeepMeta(reasons=Reasons.MostRecent),
        KeepMeta(reasons=Reasons.MostRecent, flags=Flags.New),
    ],
    ids=["delete", "mostrecent", "mostrecentnew"],
)
def test_alternate_keep_meta(
    golden_asmt: Assessment, goldifyconsole: Console, meta: KeepMeta
) -> None:
    # Pick an arbitrary, stable snapshot
    uuid, snap = sorted(golden_asmt.snapshots.items())[0]
    # Mark the snapshot
    golden_asmt.snapshots[uuid] = snap._replace(meta=meta)
    # And all associated backups
    for (remote, backup_uuid), backup in list(golden_asmt.backups.items()):
        if backup_uuid == uuid:
            golden_asmt.backups[(remote, uuid)] = backup._replace(meta=meta)

    print_plan(
        console=goldifyconsole,
        assessment=golden_asmt,
        actions=assessment_to_actions(golden_asmt),
    )


def test_send_ancestor(golden_asmt: Assessment, goldifyconsole: Console) -> None:
    # Pick an arbitrary, stable backup which has a send-parent
    (remote, child_uuid), child_backup = sorted(
        golden_asmt.backups.items(),
        key=lambda i: (i[1].info.send_parent_uuid is None, i[0][1], i[0][0].name),
    )[0]
    # Get its parent snapshot and backup
    parent_uuid = child_backup.info.send_parent_uuid
    assert parent_uuid is not None
    parent_snap = golden_asmt.snapshots[parent_uuid]
    parent_backup = golden_asmt.backups[(remote, parent_uuid)]
    # Snapshot is not kept
    golden_asmt.snapshots[parent_uuid] = parent_snap._replace(meta=KeepMeta())
    # Backup must be kept because it's the send-parent of another backup
    golden_asmt.backups[(remote, parent_uuid)] = parent_backup._replace(
        meta=KeepMeta(reasons=Reasons.SendAncestor, other_uuids={child_uuid})
    )

    print_plan(
        console=goldifyconsole,
        assessment=golden_asmt,
        actions=assessment_to_actions(golden_asmt),
    )
