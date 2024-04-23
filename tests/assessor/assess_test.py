from __future__ import annotations

import time
from typing import TYPE_CHECKING
from uuid import uuid4

from botocore.exceptions import ClientError
from btrfs2s3._internal.util import backup_of_snapshot
from btrfs2s3._internal.util import mkretained
from btrfs2s3._internal.util import NULL_UUID
from btrfs2s3._internal.util import SubvolumeFlags
from btrfs2s3.action import Actions
from btrfs2s3.assessor import assess
from btrfs2s3.assessor import assessment_to_actions
from btrfs2s3.backups import BackupInfo
from btrfs2s3.resolver import Reason
from btrfs2s3.resolver import ReasonCode
from btrfs2s3.s3 import iter_backups
import btrfsutil
import pytest

if TYPE_CHECKING:
    from pathlib import Path

    from mypy_boto3_s3.client import S3Client

    from tests.conftest import DownloadAndPipe


def test_create_and_backup_new_snapshot(
    btrfs_mountpoint: Path,
    s3: S3Client,
    bucket: str,
    download_and_pipe: DownloadAndPipe,
) -> None:
    # Create a subvolume
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    # Snapshot dir, but no snapshots
    snapshot_dir = btrfs_mountpoint / "snapshots"
    snapshot_dir.mkdir()
    # Modify some data in the source
    (source / "dummy-file").write_bytes(b"dummy")
    btrfsutil.sync(source)

    # No retention, just keep most recent snapshot and backup
    assessment = assess(
        snapshot_dir=snapshot_dir,
        sources=(source,),
        s3=s3,
        bucket=bucket,
        iter_time_spans=lambda _: iter(()),
        is_time_span_retained=lambda _: False,
    )

    (source_asmt,) = list(assessment.sources.values())
    # We should only keep one proposed snapshot
    (snapshot_asmt,) = list(source_asmt.snapshots.values())
    assert snapshot_asmt.new
    proposed_info = snapshot_asmt.info
    assert proposed_info.uuid != NULL_UUID
    assert proposed_info.parent_uuid == btrfsutil.subvolume_info(source).uuid
    want_flags = SubvolumeFlags.Proposed | SubvolumeFlags.ReadOnly
    assert proposed_info.flags & want_flags == want_flags
    assert snapshot_asmt.keep_reasons == {Reason(code=ReasonCode.MostRecent)}

    # We should only keep one proposed full backup
    ((backup_uuid, backup_asmt),) = list(source_asmt.backups.items())
    assert backup_uuid == proposed_info.uuid
    assert backup_asmt.keep_reasons == {
        Reason(code=ReasonCode.MostRecent | ReasonCode.New)
    }

    actions = Actions()
    assessment_to_actions(assessment, actions)

    (create_snapshot_intent,) = list(actions.iter_create_snapshot_intents())
    assert create_snapshot_intent.source.peek() == source
    assert create_snapshot_intent.path().is_relative_to(snapshot_dir)
    assert create_snapshot_intent.path().name.startswith(source.name)
    (rename_snapshot_intent,) = list(actions.iter_rename_snapshot_intents())
    assert rename_snapshot_intent.source.peek() == create_snapshot_intent.path.peek()
    (create_backup_intent,) = list(actions.iter_create_backup_intents())
    assert create_backup_intent.source.peek() == source

    actions.execute(s3, bucket)

    (snapshot,) = list(snapshot_dir.iterdir())
    assert snapshot.name.startswith(source.name)
    snapshot_info = btrfsutil.subvolume_info(snapshot)
    assert snapshot_info.parent_uuid == btrfsutil.subvolume_info(source).uuid

    ((obj, backup),) = list(iter_backups(s3, bucket))
    assert backup.uuid == snapshot_info.uuid
    assert backup.parent_uuid == snapshot_info.parent_uuid
    assert backup.send_parent_uuid is None

    download_and_pipe(obj["Key"], ["btrfs", "receive", "--dump"])


def test_create_and_backup_with_parent(
    btrfs_mountpoint: Path,
    s3: S3Client,
    bucket: str,
    download_and_pipe: DownloadAndPipe,
) -> None:
    # Create a subvolume
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    # Snapshot dir
    snapshot_dir = btrfs_mountpoint / "snapshots"
    snapshot_dir.mkdir()
    # Modify some data in the source
    (source / "dummy-file").write_bytes(b"dummy")
    # Create an initial snapshot
    snapshot1 = snapshot_dir / "snapshot1"
    btrfsutil.create_snapshot(source, snapshot1, read_only=True)
    # Modify the source again
    (source / "dummy-file").write_bytes(b"dummy2")
    btrfsutil.sync(source)

    # This isn't guaranteed to work at year boundaries. Can't think of a better
    # way to do it right now.
    iter_time_spans, is_time_span_retained = mkretained(now=time.time(), years=(0,))
    assessment = assess(
        snapshot_dir=snapshot_dir,
        sources=(source,),
        s3=s3,
        bucket=bucket,
        iter_time_spans=iter_time_spans,
        is_time_span_retained=is_time_span_retained,
    )

    (source_asmt,) = list(assessment.sources.values())
    (old_asmt, new_asmt) = sorted(source_asmt.snapshots.values(), key=lambda a: a.new)
    # One assessment for the new snapshot
    assert new_asmt.new
    proposed_info = new_asmt.info
    assert proposed_info.uuid != NULL_UUID
    assert proposed_info.parent_uuid == btrfsutil.subvolume_info(source).uuid
    want_flags = SubvolumeFlags.Proposed | SubvolumeFlags.ReadOnly
    assert proposed_info.flags & want_flags == want_flags
    assert new_asmt.keep_reasons == {Reason(code=ReasonCode.MostRecent)}
    # One assessment for the existing snapshot
    assert not old_asmt.new
    assert old_asmt.info.uuid == btrfsutil.subvolume_info(snapshot1).uuid
    (reason,) = old_asmt.keep_reasons
    assert reason.code & ReasonCode.Retained

    # One backup assessment will match the uuid of the existing snapshot, but
    # the other will match a proposed uuid
    full_asmt = source_asmt.backups[old_asmt.info.uuid]
    delta_asmt = source_asmt.backups[new_asmt.info.uuid]
    assert full_asmt.backup.check().uuid == old_asmt.info.uuid
    assert full_asmt.backup.check().send_parent_uuid is None
    (full_reason,) = full_asmt.keep_reasons
    assert full_reason.code & ReasonCode.Retained
    assert delta_asmt.keep_reasons == {
        Reason(code=ReasonCode.MostRecent | ReasonCode.New)
    }

    actions = Actions()
    assessment_to_actions(assessment, actions)

    actions.execute(s3, bucket)

    (snapshot1, snapshot2) = sorted(snapshot_dir.iterdir())
    assert snapshot1.name.startswith(source.name)
    assert snapshot2.name.startswith(source.name)
    snapshot1_info = btrfsutil.subvolume_info(snapshot1)
    snapshot2_info = btrfsutil.subvolume_info(snapshot2)
    assert snapshot1_info.parent_uuid == btrfsutil.subvolume_info(source).uuid
    assert snapshot2_info.parent_uuid == btrfsutil.subvolume_info(source).uuid

    ((full_obj, full_backup), (delta_obj, delta_backup)) = sorted(
        iter_backups(s3, bucket), key=lambda x: x[1].ctransid
    )
    assert full_backup.uuid == snapshot1_info.uuid
    assert full_backup.send_parent_uuid is None
    assert delta_backup.uuid == snapshot2_info.uuid
    assert delta_backup.send_parent_uuid == full_backup.uuid

    download_and_pipe(full_obj["Key"], ["btrfs", "receive", "--dump"])
    download_and_pipe(delta_obj["Key"], ["btrfs", "receive", "--dump"])


def test_rename_snapshot(
    btrfs_mountpoint: Path,
    s3: S3Client,
    bucket: str,
    download_and_pipe: DownloadAndPipe,
) -> None:
    # Create a subvolume
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    # Modify some data in the source
    (source / "dummy-file").write_bytes(b"dummy")
    # Snapshot dir
    snapshot_dir = btrfs_mountpoint / "snapshots"
    snapshot_dir.mkdir()
    # One snapshot, but wrongly-named
    snapshot = snapshot_dir / "initial-snapshot-name-that-needs-to-be-updated"
    btrfsutil.create_snapshot(source, snapshot, read_only=True)
    btrfsutil.sync(source)

    # No retention, just keep most recent snapshot and backup
    assessment = assess(
        snapshot_dir=snapshot_dir,
        sources=(source,),
        s3=s3,
        bucket=bucket,
        iter_time_spans=lambda _: iter(()),
        is_time_span_retained=lambda _: False,
    )

    (source_asmt,) = list(assessment.sources.values())
    # Keep only the existing snapshot
    (snapshot_asmt,) = list(source_asmt.snapshots.values())
    assert not snapshot_asmt.new
    assert snapshot_asmt.info.uuid == btrfsutil.subvolume_info(snapshot).uuid
    assert snapshot_asmt.info.parent_uuid == btrfsutil.subvolume_info(source).uuid
    check_flags = SubvolumeFlags.Proposed | SubvolumeFlags.ReadOnly
    want_flags = SubvolumeFlags.ReadOnly
    assert snapshot_asmt.info.flags & check_flags == want_flags
    assert snapshot_asmt.target_path.check().name.startswith(source.name)
    assert snapshot_asmt.keep_reasons == {Reason(code=ReasonCode.MostRecent)}

    # We should only keep one full backup, of the existing snapshot
    ((backup_uuid, backup_asmt),) = list(source_asmt.backups.items())
    assert backup_uuid == snapshot_asmt.info.uuid
    assert backup_asmt.keep_reasons == {
        Reason(code=ReasonCode.MostRecent | ReasonCode.New)
    }

    actions = Actions()
    assessment_to_actions(assessment, actions)

    (rename_snapshot_intent,) = list(actions.iter_rename_snapshot_intents())
    assert rename_snapshot_intent.source.peek() == snapshot
    (create_backup_intent,) = list(actions.iter_create_backup_intents())
    assert create_backup_intent.source.peek() == source

    actions.execute(s3, bucket)

    (snapshot,) = list(snapshot_dir.iterdir())
    assert snapshot.name.startswith(source.name)
    snapshot_info = btrfsutil.subvolume_info(snapshot)
    assert snapshot_info.parent_uuid == btrfsutil.subvolume_info(source).uuid

    ((obj, backup),) = list(iter_backups(s3, bucket))
    assert backup.uuid == snapshot_info.uuid
    assert backup.parent_uuid == snapshot_info.parent_uuid
    assert backup.send_parent_uuid is None

    download_and_pipe(obj["Key"], ["btrfs", "receive", "--dump"])


def test_delete_only_snapshot_because_proposed_would_be_newer(
    btrfs_mountpoint: Path,
    s3: S3Client,
    bucket: str,
    download_and_pipe: DownloadAndPipe,
) -> None:
    # Create a subvolume
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    # Snapshot dir, but no snapshots
    snapshot_dir = btrfs_mountpoint / "snapshots"
    snapshot_dir.mkdir()
    # Modify some data in the source
    (source / "dummy-file").write_bytes(b"dummy")
    # Initial snapshot, name doesn't matter
    initial_snapshot = snapshot_dir / "initial-snapshot"
    btrfsutil.create_snapshot(source, initial_snapshot, read_only=True)
    # Modify the source again
    (source / "dummy-file").write_bytes(b"dummy2")
    btrfsutil.sync(source)

    # No retention, just keep most recent snapshot and backup
    assessment = assess(
        snapshot_dir=snapshot_dir,
        sources=(source,),
        s3=s3,
        bucket=bucket,
        iter_time_spans=lambda _: iter(()),
        is_time_span_retained=lambda _: False,
    )

    (source_asmt,) = list(assessment.sources.values())
    # Two assessments: one for the existing snapshot, one proposed
    (old_asmt, new_asmt) = sorted(source_asmt.snapshots.values(), key=lambda a: a.new)
    assert new_asmt.new
    proposed_info = new_asmt.info
    assert proposed_info.uuid != NULL_UUID
    assert proposed_info.parent_uuid == btrfsutil.subvolume_info(source).uuid
    want_flags = SubvolumeFlags.Proposed | SubvolumeFlags.ReadOnly
    assert proposed_info.flags & want_flags == want_flags
    assert new_asmt.keep_reasons == {Reason(code=ReasonCode.MostRecent)}
    # Assessment of the existing snapshot
    assert not old_asmt.new
    assert old_asmt.info.uuid == btrfsutil.subvolume_info(initial_snapshot).uuid
    assert old_asmt.initial_path == initial_snapshot
    assert old_asmt.keep_reasons == set()

    # We should only keep one proposed full backup
    ((backup_uuid, backup_asmt),) = list(source_asmt.backups.items())
    assert backup_uuid == proposed_info.uuid
    assert backup_asmt.keep_reasons == {
        Reason(code=ReasonCode.MostRecent | ReasonCode.New)
    }

    actions = Actions()
    assessment_to_actions(assessment, actions)

    (create_snapshot_intent,) = list(actions.iter_create_snapshot_intents())
    assert create_snapshot_intent.source.peek() == source
    assert create_snapshot_intent.path().is_relative_to(snapshot_dir)
    assert create_snapshot_intent.path().name.startswith(source.name)
    (rename_snapshot_intent,) = list(actions.iter_rename_snapshot_intents())
    assert rename_snapshot_intent.source.peek() == create_snapshot_intent.path.peek()
    (create_backup_intent,) = list(actions.iter_create_backup_intents())
    assert create_backup_intent.source.peek() == source
    (delete_snapshot_intent,) = list(actions.iter_delete_snapshot_intents())
    assert delete_snapshot_intent.path.peek() == initial_snapshot

    actions.execute(s3, bucket)

    (snapshot,) = list(snapshot_dir.iterdir())
    assert snapshot.name.startswith(source.name)
    snapshot_info = btrfsutil.subvolume_info(snapshot)
    assert snapshot_info.parent_uuid == btrfsutil.subvolume_info(source).uuid

    ((obj, backup),) = list(iter_backups(s3, bucket))
    assert backup.uuid == snapshot_info.uuid
    assert backup.parent_uuid == snapshot_info.parent_uuid
    assert backup.send_parent_uuid is None

    download_and_pipe(obj["Key"], ["btrfs", "receive", "--dump"])


def test_ignore_read_write_snapshot(
    btrfs_mountpoint: Path, s3: S3Client, bucket: str
) -> None:
    # Create a subvolume
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    # Snapshot dir, but no snapshots
    snapshot_dir = btrfs_mountpoint / "snapshots"
    snapshot_dir.mkdir()
    # Modify some data in the source
    (source / "dummy-file").write_bytes(b"dummy")
    # Initial snapshot, name doesn't matter
    initial_snapshot = snapshot_dir / "initial-snapshot"
    btrfsutil.create_snapshot(source, initial_snapshot)
    btrfsutil.sync(source)

    # No retention, just keep most recent snapshot and backup
    assessment = assess(
        snapshot_dir=snapshot_dir,
        sources=(source,),
        s3=s3,
        bucket=bucket,
        iter_time_spans=lambda _: iter(()),
        is_time_span_retained=lambda _: False,
    )
    actions = Actions()
    assessment_to_actions(assessment, actions)
    actions.execute(s3, bucket)

    (snap1, snap2) = sorted(snapshot_dir.iterdir(), key=lambda p: p == initial_snapshot)
    assert snap2 == initial_snapshot
    info = btrfsutil.subvolume_info(snap1)
    assert info.flags & SubvolumeFlags.ReadOnly


def test_fail_if_snapshot_dir_not_on_btrfs(
    btrfs_mountpoint: Path, ext4_mountpoint: Path, s3: S3Client, bucket: str
) -> None:
    with pytest.raises(RuntimeError):
        # No retention, just keep most recent snapshot and backup
        assess(
            snapshot_dir=ext4_mountpoint,
            sources=(btrfs_mountpoint,),
            s3=s3,
            bucket=bucket,
            iter_time_spans=lambda _: iter(()),  # pragma: no cover
            is_time_span_retained=lambda _: False,  # pragma: no cover
        )


def test_ignore_snapshots_from_unrelated_sources(
    btrfs_mountpoint: Path, s3: S3Client, bucket: str
) -> None:
    # Create subvolumes
    source1 = btrfs_mountpoint / "source1"
    btrfsutil.create_subvolume(source1)
    source2 = btrfs_mountpoint / "source2"
    btrfsutil.create_subvolume(source2)
    # Snapshot dir, but no snapshots
    snapshot_dir = btrfs_mountpoint / "snapshots"
    snapshot_dir.mkdir()
    # Modify some data in the sources
    (source1 / "dummy-file").write_bytes(b"dummy")
    (source2 / "dummy-file").write_bytes(b"dummy")
    # Initial snapshot, name doesn't matter
    ignore_snapshot = snapshot_dir / "ignore-snapshot"
    btrfsutil.create_snapshot(source2, ignore_snapshot, read_only=True)
    btrfsutil.sync(btrfs_mountpoint)

    # No retention, just keep most recent snapshot and backup
    assessment = assess(
        snapshot_dir=snapshot_dir,
        sources=(source1,),
        s3=s3,
        bucket=bucket,
        iter_time_spans=lambda _: iter(()),
        is_time_span_retained=lambda _: False,
    )
    actions = Actions()
    assessment_to_actions(assessment, actions)
    actions.execute(s3, bucket)

    (snap1, snap2) = sorted(snapshot_dir.iterdir(), key=lambda p: p == ignore_snapshot)
    assert snap2 == ignore_snapshot
    info = btrfsutil.subvolume_info(snap1)
    assert info.flags & SubvolumeFlags.ReadOnly


def test_ignore_unrelated_s3_objects(
    btrfs_mountpoint: Path, s3: S3Client, bucket: str
) -> None:
    key = "not-a-backup-name"
    s3.put_object(Bucket=bucket, Key=key, Body=b"dummy")

    # No retention, just keep most recent snapshot and backup
    assessment = assess(
        snapshot_dir=btrfs_mountpoint,
        sources=(btrfs_mountpoint,),
        s3=s3,
        bucket=bucket,
        iter_time_spans=lambda _: iter(()),
        is_time_span_retained=lambda _: False,
    )
    actions = Actions()
    assessment_to_actions(assessment, actions)
    actions.execute(s3, bucket)

    assert s3.get_object(Bucket=bucket, Key=key)["Body"].read() == b"dummy"


def test_ignore_unrelated_backups(
    btrfs_mountpoint: Path, s3: S3Client, bucket: str
) -> None:
    info = BackupInfo(
        uuid=uuid4().bytes,
        parent_uuid=uuid4().bytes,
        send_parent_uuid=None,
        ctransid=1234,
        ctime=time.time(),
    )
    key = f"base{''.join(info.get_path_suffixes())}"
    s3.put_object(Bucket=bucket, Key=key, Body=b"dummy")

    # No retention, just keep most recent snapshot and backup
    assessment = assess(
        snapshot_dir=btrfs_mountpoint,
        sources=(btrfs_mountpoint,),
        s3=s3,
        bucket=bucket,
        iter_time_spans=lambda _: iter(()),
        is_time_span_retained=lambda _: False,
    )
    actions = Actions()
    assessment_to_actions(assessment, actions)
    actions.execute(s3, bucket)

    assert s3.get_object(Bucket=bucket, Key=key)["Body"].read() == b"dummy"


def test_delete_old_backups(btrfs_mountpoint: Path, s3: S3Client, bucket: str) -> None:
    # Create subvolume
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    # Snapshot dir
    snapshot_dir = btrfs_mountpoint / "snapshots"
    snapshot_dir.mkdir()
    # Initial snapshot, name doesn't matter
    dummy_snapshot = snapshot_dir / "dummy-snapshot"
    btrfsutil.create_snapshot(source, dummy_snapshot, read_only=True)
    # Create a dummy backup to match the snapshot
    dummy_backup = backup_of_snapshot(btrfsutil.subvolume_info(dummy_snapshot))
    dummy_key = f"base{''.join(dummy_backup.get_path_suffixes())}"
    s3.put_object(Bucket=bucket, Key=dummy_key, Body=b"dummy")
    # Modify some data in the source
    (source / "dummy-file").write_bytes(b"dummy")
    btrfsutil.sync(btrfs_mountpoint)

    # No retention, just keep most recent snapshot and backup
    assessment = assess(
        snapshot_dir=snapshot_dir,
        sources=(source,),
        s3=s3,
        bucket=bucket,
        iter_time_spans=lambda _: iter(()),
        is_time_span_retained=lambda _: False,
    )
    actions = Actions()
    assessment_to_actions(assessment, actions)
    actions.execute(s3, bucket)

    # Ensure dummy backup was deleted
    with pytest.raises(ClientError):
        s3.head_object(Bucket=bucket, Key=dummy_key)


def test_second_run_is_a_no_op(
    btrfs_mountpoint: Path, s3: S3Client, bucket: str
) -> None:
    # Create subvolume
    source = btrfs_mountpoint / "source"
    btrfsutil.create_subvolume(source)
    # Snapshot dir
    snapshot_dir = btrfs_mountpoint / "snapshots"
    snapshot_dir.mkdir()
    # Modify some data in the source
    (source / "dummy-file").write_bytes(b"dummy")
    btrfsutil.sync(btrfs_mountpoint)

    # No retention, just keep most recent snapshot and backup
    assessment = assess(
        snapshot_dir=snapshot_dir,
        sources=(source,),
        s3=s3,
        bucket=bucket,
        iter_time_spans=lambda _: iter(()),
        is_time_span_retained=lambda _: False,
    )
    actions = Actions()
    assessment_to_actions(assessment, actions)
    actions.execute(s3, bucket)

    # Second run

    # No retention, just keep most recent snapshot and backup
    assessment = assess(
        snapshot_dir=snapshot_dir,
        sources=(source,),
        s3=s3,
        bucket=bucket,
        iter_time_spans=lambda _: iter(()),
        is_time_span_retained=lambda _: False,
    )
    actions = Actions()
    assessment_to_actions(assessment, actions)
    assert list(actions.iter_create_snapshot_intents()) == []
    assert list(actions.iter_delete_snapshot_intents()) == []
    assert list(actions.iter_rename_snapshot_intents()) == []
    assert list(actions.iter_create_backup_intents()) == []
    assert list(actions.iter_delete_backup_intents()) == []
