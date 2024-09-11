from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from functools import partial
import os
import time
from typing import TYPE_CHECKING
from uuid import uuid4

import arrow
import btrfsutil
from btrfsutil import SubvolumeInfo

from btrfs2s3._internal.resolver import Flags
from btrfs2s3._internal.resolver import KeepMeta
from btrfs2s3._internal.resolver import resolve
from btrfs2s3._internal.s3 import iter_backups
from btrfs2s3._internal.thunk import Thunk
from btrfs2s3._internal.thunk import ThunkArg
from btrfs2s3._internal.util import mksubvol
from btrfs2s3._internal.util import SubvolumeFlags

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Sequence

    from mypy_boto3_s3.client import S3Client

    from btrfs2s3._internal import resolver
    from btrfs2s3._internal.action import Actions
    from btrfs2s3._internal.backups import BackupInfo
    from btrfs2s3._internal.preservation import Policy


@dataclass
class SnapshotAssessment:
    initial_path: Path
    info: SubvolumeInfo
    target_path: Thunk[Path]
    real_info: Thunk[SubvolumeInfo]
    keep_meta: KeepMeta = field(default_factory=KeepMeta)

    @property
    def new(self) -> bool:
        return bool(self.keep_meta.flags & Flags.New)


@dataclass
class BackupAssessment:
    backup: Thunk[BackupInfo]
    key: Thunk[str]
    keep_meta: KeepMeta = field(default_factory=KeepMeta)

    @property
    def new(self) -> bool:
        return bool(self.keep_meta.flags & Flags.New)


@dataclass
class SourceAssessment:
    path: Path
    info: SubvolumeInfo
    snapshots: dict[bytes, SnapshotAssessment] = field(default_factory=dict)
    backups: dict[bytes, BackupAssessment] = field(default_factory=dict)


@dataclass(frozen=True)
class Assessment:
    sources: dict[bytes, SourceAssessment] = field(default_factory=dict)


def _get_snapshot_path_for_backup_thunk(
    source: SourceAssessment, backup: Thunk[BackupInfo]
) -> Path:
    return source.snapshots[backup().uuid].target_path()


def _get_send_parent_path_for_backup_thunk(
    source: SourceAssessment, backup: Thunk[BackupInfo]
) -> Path | None:
    info = backup()
    if info.send_parent_uuid is None:
        return None
    return source.snapshots[info.send_parent_uuid].target_path()


def _backup_assessment_to_actions(
    source: SourceAssessment, backup: BackupAssessment, actions: Actions
) -> None:
    if not backup.keep_meta.reasons:
        actions.delete_backup(backup.key)
    elif backup.new:
        snapshot = partial(_get_snapshot_path_for_backup_thunk, source, backup.backup)
        send_parent = partial(
            _get_send_parent_path_for_backup_thunk, source, backup.backup
        )
        actions.create_backup(
            source=source.path,
            snapshot=snapshot,
            send_parent=send_parent,
            key=backup.key,
        )


def _snapshot_assessment_to_actions(
    source: SourceAssessment, snapshot: SnapshotAssessment, actions: Actions
) -> None:
    if not snapshot.keep_meta.reasons:
        actions.delete_snapshot(snapshot.initial_path)
    else:
        if snapshot.new:
            actions.create_snapshot(source=source.path, path=snapshot.initial_path)
        if snapshot.initial_path != snapshot.target_path.peek():
            actions.rename_snapshot(
                source=snapshot.initial_path, target=snapshot.target_path
            )


def assessment_to_actions(assessment: Assessment, actions: Actions) -> None:
    for source in assessment.sources.values():
        for snapshot in source.snapshots.values():
            _snapshot_assessment_to_actions(source, snapshot, actions)
        for backup in source.backups.values():
            _backup_assessment_to_actions(source, backup, actions)


@dataclass
class _SourceAssessor:
    assessment: SourceAssessment
    snapshot_dir: Path
    policy: Policy

    def _make_snapshot_path(self, info: SubvolumeInfo) -> Path:
        ctime = arrow.get(info.ctime, tzinfo=self.policy.tzinfo)
        ctime_str = ctime.isoformat(timespec="seconds")
        name = f"{self.assessment.path.name}.{ctime_str}.{info.ctransid}"
        return self.snapshot_dir / name

    def _make_new_snapshot_path(self) -> Path:
        return self.snapshot_dir / f"{self.assessment.path.name}.NEW.{os.getpid()}"

    def _make_backup_key(self, backup: BackupInfo) -> str:
        suffixes = backup.get_path_suffixes(tzinfo=self.policy.tzinfo)
        return f"{self.assessment.path.name}{''.join(suffixes)}"

    def _is_new_snapshot_needed(self) -> bool:
        if not self.assessment.snapshots:
            return True
        return self.assessment.info.ctransid > max(
            s.info.ctransid for s in self.assessment.snapshots.values()
        )

    def _maybe_propose_new_snapshot(self) -> None:
        if not self._is_new_snapshot_needed():
            return

        proposed_uuid = uuid4().bytes
        now = time.time()
        proposed_info = mksubvol(
            parent_id=self.assessment.info.id,
            flags=SubvolumeFlags.ReadOnly | SubvolumeFlags.Proposed,
            uuid=proposed_uuid,
            parent_uuid=self.assessment.info.uuid,
            generation=self.assessment.info.generation,
            ctransid=self.assessment.info.ctransid,
            otransid=self.assessment.info.generation,
            ctime=now,
            otime=now,
        )
        initial_path = self._make_new_snapshot_path()

        def get_real_info() -> SubvolumeInfo:
            return btrfsutil.subvolume_info(initial_path)

        self.assessment.snapshots[proposed_uuid] = SnapshotAssessment(
            initial_path=initial_path,
            info=proposed_info,
            target_path=Thunk(initial_path),
            real_info=Thunk(get_real_info),
            keep_meta=KeepMeta(flags=Flags.New),
        )

    def _get_target_path(self, snapshot: SnapshotAssessment) -> ThunkArg[Path]:
        if snapshot.real_info.is_tbd():
            return lambda: self._make_snapshot_path(snapshot.real_info())
        return self._make_snapshot_path(snapshot.real_info())

    def _do_resolve(self, *, include_proposed: bool = True) -> resolver.Result:
        snapshots = []
        for snapshot in self.assessment.snapshots.values():
            if snapshot.info.flags & SubvolumeFlags.Proposed and not include_proposed:
                continue
            snapshots.append(snapshot.info)
        backups = [
            b.backup()
            for b in self.assessment.backups.values()
            if not b.backup.is_tbd()
        ]

        return resolve(snapshots=snapshots, backups=backups, policy=self.policy)

    def _resolve(self) -> None:
        # Run resolve including proposed snapshots.
        result = self._do_resolve(include_proposed=True)

        # Mark snapshots as kept, and rename these if necessary
        for uuid, keep_snapshot in result.keep_snapshots.items():
            snapshot = self.assessment.snapshots[uuid]
            snapshot.keep_meta |= keep_snapshot.meta
            # Update the target name, only for snapshots we'll keep
            snapshot.target_path = Thunk(self._get_target_path(snapshot))
        # Mark backups as kept, possibly creating new entries
        for uuid, keep_backup in result.keep_backups.items():
            # NB: if resolve() would ever change BackupInfo in-place, do that
            # here
            if uuid not in self.assessment.backups:
                snapshot = self.assessment.snapshots[uuid]
                if snapshot.real_info.is_tbd():
                    backup = Thunk(
                        partial(
                            lambda p, i: self._get_real_backup(p(), i()),
                            snapshot.target_path,
                            snapshot.real_info,
                        )
                    )
                    key = Thunk(partial(lambda b: self._make_backup_key(b()), backup))
                else:
                    backup = Thunk(keep_backup.item)
                    key = Thunk(self._make_backup_key(keep_backup.item))
                self.assessment.backups[uuid] = BackupAssessment(backup=backup, key=key)
            self.assessment.backups[uuid].keep_meta |= keep_backup.meta

    def _get_real_backup(
        self, target_path: Path, real_info: SubvolumeInfo
    ) -> BackupInfo:
        self.assessment.snapshots[real_info.uuid] = SnapshotAssessment(
            initial_path=target_path,
            info=real_info,
            real_info=Thunk(real_info),
            target_path=Thunk(target_path),
        )
        result = self._do_resolve(include_proposed=False)
        return result.keep_backups[real_info.uuid].item

    def assess(self) -> None:
        self._maybe_propose_new_snapshot()
        self._resolve()


@dataclass
class _Assessor:
    snapshot_dir: Path
    sources: Sequence[Path]
    policy: Policy

    _assessment: Assessment = field(init=False, default_factory=Assessment)

    def _collect_sources(self) -> None:
        for source in self.sources:
            info = btrfsutil.subvolume_info(source)
            self._assessment.sources[info.uuid] = SourceAssessment(
                path=source, info=info
            )

    def _collect_snapshots(self) -> None:
        # SubvolumeIterator only works at a subvolume boundary, so search for
        # one
        search_base = self.snapshot_dir
        while (
            not btrfsutil.is_subvolume(search_base)
            and search_base != search_base.parent
        ):
            search_base = search_base.parent
        if search_base == search_base.parent:
            msg = f"no subvolume found. is {self.snapshot_dir} on a btrfs filesystem?"
            raise RuntimeError(msg)
        for name, info in btrfsutil.SubvolumeIterator(search_base, info=True):
            path = search_base / name
            if self.snapshot_dir not in path.parents:
                continue
            if info.parent_uuid not in self._assessment.sources:
                continue
            if not info.flags & SubvolumeFlags.ReadOnly:
                continue
            self._assessment.sources[info.parent_uuid].snapshots[info.uuid] = (
                SnapshotAssessment(
                    initial_path=path,
                    info=info,
                    real_info=Thunk(info),
                    target_path=Thunk(path),
                )
            )

    def _collect_backups(self, s3: S3Client, bucket: str) -> None:
        for obj, backup in iter_backups(s3, bucket):
            if backup.parent_uuid not in self._assessment.sources:
                continue
            self._assessment.sources[backup.parent_uuid].backups[backup.uuid] = (
                BackupAssessment(backup=Thunk(backup), key=Thunk(obj["Key"]))
            )

    def _assess_for_all_sources(self) -> None:
        for source in self._assessment.sources.values():
            assessor = _SourceAssessor(
                assessment=source, snapshot_dir=self.snapshot_dir, policy=self.policy
            )
            assessor.assess()

    def assess(self, s3: S3Client, bucket: str) -> None:
        self._collect_sources()
        self._collect_snapshots()
        self._collect_backups(s3, bucket)
        self._assess_for_all_sources()

    def get_assessment(self) -> Assessment:
        return self._assessment


def assess(
    *,
    snapshot_dir: Path,
    sources: Sequence[Path],
    s3: S3Client,
    bucket: str,
    policy: Policy,
) -> Assessment:
    assessor = _Assessor(snapshot_dir=snapshot_dir, sources=sources, policy=policy)
    assessor.assess(s3, bucket)
    return assessor.get_assessment()
