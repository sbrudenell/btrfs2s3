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

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import logging
import os
from typing import NamedTuple
from typing import Protocol
from typing import TYPE_CHECKING

import arrow

from btrfs2s3._internal.backups import BackupInfo
from btrfs2s3._internal.btrfsioctl import create_snap
from btrfs2s3._internal.btrfsioctl import destroy_snap
from btrfs2s3._internal.btrfsioctl import FIRST_FREE_OBJECTID
from btrfs2s3._internal.btrfsioctl import opendir
from btrfs2s3._internal.btrfsioctl import send
from btrfs2s3._internal.btrfsioctl import subvol_info
from btrfs2s3._internal.btrfsioctl import SubvolFlag
from btrfs2s3._internal.btrfsioctl import SubvolInfo
from btrfs2s3._internal.cvar import TZINFO
from btrfs2s3._internal.resolver import Flags
from btrfs2s3._internal.resolver import KeepMeta
from btrfs2s3._internal.resolver import resolve
from btrfs2s3._internal.s3 import iter_backups
from btrfs2s3._internal.stream_uploader import upload_non_seekable_stream_via_tempfile
from btrfs2s3._internal.util import backup_of_snapshot

if TYPE_CHECKING:
    from collections.abc import Collection
    from collections.abc import Iterator
    from collections.abc import Mapping
    from contextlib import AbstractContextManager
    from pathlib import Path
    from typing import IO

    from types_boto3_s3.client import S3Client
    from types_boto3_s3.type_defs import ObjectTypeDef
    from typing_extensions import Self

    from btrfs2s3._internal.preservation import Policy

_LOG = logging.getLogger(__name__)


def _check_is_subvol(path: Path, fd: int) -> None:
    if not _is_subvol(fd):
        msg = f"{path} is not a subvolume boundary"
        raise ValueError(msg)


class Source:
    @classmethod
    def create(cls, path: Path) -> Self:
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            _check_is_subvol(path, fd)
            info = subvol_info(fd)
            return cls(path=path, fd=fd, info=info)
        except Exception:
            os.close(fd)
            raise

    def __init__(self, *, path: Path, info: SubvolInfo, fd: int) -> None:
        self._path = path
        self._info = info
        self._fd = fd

    @property
    def fd(self) -> int:
        return self._fd

    @property
    def info(self) -> SubvolInfo:
        return self._info

    @property
    def path(self) -> Path:
        return self._path

    def get_new_snapshot_name(self) -> str:
        return f"{self.path.name}.NEW.{os.getpid()}"

    def get_snapshot_name(self, info: SubvolInfo) -> str:
        ctime = arrow.get(info.ctime, tzinfo=TZINFO.get())
        ctime_str = ctime.isoformat(timespec="seconds")
        return f"{self.path.name}.{ctime_str}.{info.ctransid}"

    def get_backup_key(self, info: BackupInfo) -> str:
        suffixes = info.get_path_suffixes()
        return f"{self.path.name}{''.join(suffixes)}"

    def close(self) -> None:
        os.close(self._fd)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


def _is_subvol(fd: int) -> bool:
    return os.stat(fd).st_ino == FIRST_FREE_OBJECTID  # noqa: PTH116


def _iter_snapshots(dir_fd: int) -> Iterator[tuple[str, SubvolInfo]]:
    for name in os.listdir(dir_fd):
        with opendir(name, dir_fd=dir_fd) as fd:
            if not _is_subvol(fd):
                continue
            info = subvol_info(fd)
            if info.parent_uuid is None:
                continue
            if not info.flags & SubvolFlag.ReadOnly:
                continue
            yield (name, info)


class SnapshotDir:
    @classmethod
    def create(cls, path: Path) -> Self:
        dir_fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            # raise an error if the dir isn't on a btrfs mountpoint, even if
            # it's empty
            subvol_info(dir_fd)
            snapshots = list(_iter_snapshots(dir_fd))
            return cls(path=path, dir_fd=dir_fd, snapshots=snapshots)
        except Exception:
            os.close(dir_fd)
            raise

    def __init__(
        self, *, path: Path, dir_fd: int, snapshots: Collection[tuple[str, SubvolInfo]]
    ) -> None:
        self._path = path
        self._dir_fd = dir_fd
        self._id_to_snapshot: dict[int, SubvolInfo] = {}
        self._id_to_name: dict[int, str] = {}
        self._p_to_u_to_snapshot: dict[bytes, dict[bytes, SubvolInfo]] = defaultdict(
            dict
        )
        for name, snap in snapshots:
            self._add_snapshot(name, snap)

    def _add_snapshot(self, name: str, snap: SubvolInfo) -> None:
        id_ = snap.id
        u = snap.uuid
        p = snap.parent_uuid
        assert p is not None
        assert id_ not in self._id_to_name
        self._id_to_name[id_] = name
        assert id_ not in self._id_to_snapshot
        self._id_to_snapshot[id_] = snap
        assert u not in self._p_to_u_to_snapshot[p]
        self._p_to_u_to_snapshot[p][u] = snap

    def _remove_snapshot(self, snapshot_id: int) -> None:
        snap = self._id_to_snapshot[snapshot_id]
        u = snap.uuid
        p = snap.parent_uuid
        assert p is not None
        del self._id_to_name[snapshot_id]
        del self._id_to_snapshot[snapshot_id]
        del self._p_to_u_to_snapshot[p][u]

    def get_name(self, snapshot_id: int) -> str:
        return self._id_to_name[snapshot_id]

    def get_path(self, snapshot_id: int) -> Path:
        return self.path / self.get_name(snapshot_id)

    @property
    def path(self) -> Path:
        return self._path

    def get_snapshots(self, source: Source) -> Mapping[bytes, SubvolInfo]:
        return self._p_to_u_to_snapshot.get(source.info.uuid, {})

    def create_snapshot(self, source: Source, name: str) -> SubvolInfo:
        _LOG.info(
            "creating read-only snapshot of %s at %s", source.path, self.path / name
        )
        create_snap(src=source.fd, dst=name, dst_dir_fd=self._dir_fd, read_only=True)
        snap = subvol_info(name, dir_fd=self._dir_fd)
        self._add_snapshot(name, snap)
        return snap

    def destroy_snapshot(self, snapshot_id: int) -> None:
        self._id_to_snapshot[snapshot_id]
        _LOG.info("destroying read-only snapshot %s", self.get_path(snapshot_id))
        destroy_snap(dir_fd=self._dir_fd, snapshot_id=snapshot_id)
        self._remove_snapshot(snapshot_id)

    def rename_snapshot(self, snapshot_id: int, target: str) -> None:
        name = self.get_name(snapshot_id)
        _LOG.info("renaming %s -> %s", self.path / name, self.path / target)
        os.rename(name, target, src_dir_fd=self._dir_fd, dst_dir_fd=self._dir_fd)  # noqa: PTH104
        self._id_to_name[snapshot_id] = target

    def send(
        self,
        *,
        dst: int | IO[bytes],
        snapshot_id: int,
        parent_id: int = 0,
        proto: int | None = None,
        flags: int = 0,
    ) -> None:
        name = self.get_name(snapshot_id)
        with opendir(name, dir_fd=self._dir_fd) as fd:
            snap = subvol_info(fd)
            if snap.id != snapshot_id:
                msg = (
                    "snapshot moved or renamed since we started: "
                    f"{self.get_path(snapshot_id)} was subvol {snapshot_id}, "
                    f"now it's {snap.id}"
                )
                raise RuntimeError(msg)
            send(src=fd, dst=dst, parent_id=parent_id, proto=proto, flags=flags)

    def close(self) -> None:
        os.close(self._dir_fd)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


DEFAULT_PART_SIZE = 5 * 2**30


class CreatePipe(Protocol):
    def __call__(self) -> AbstractContextManager[tuple[IO[bytes], IO[bytes]]]: ...


class ObjectStat(NamedTuple):
    size: int | None = None
    storage_class: str | None = None

    @classmethod
    def from_obj(cls, obj: ObjectTypeDef) -> Self:
        return cls(size=obj.get("Size"), storage_class=obj.get("StorageClass"))


class BackupObject(NamedTuple):
    key: str
    info: BackupInfo
    stat: ObjectStat


class Remote:
    @classmethod
    def create(cls, *, name: str, s3: S3Client, bucket: str) -> Self:
        return cls(
            name=name,
            s3=s3,
            bucket=bucket,
            objects=[
                BackupObject(key=obj["Key"], info=info, stat=ObjectStat.from_obj(obj))
                for obj, info in iter_backups(s3, bucket)
            ],
        )

    def __init__(
        self, *, name: str, s3: S3Client, bucket: str, objects: Collection[BackupObject]
    ) -> None:
        self._name = name
        self._s3 = s3
        self._bucket = bucket
        self._p_to_u_to_obj: dict[bytes, dict[bytes, BackupObject]] = defaultdict(dict)
        self._u_to_obj: dict[bytes, BackupObject] = {}
        for obj in objects:
            self._add_object(obj)

    def _add_object(self, obj: BackupObject) -> None:
        u = obj.info.uuid
        p = obj.info.parent_uuid
        assert u not in self._p_to_u_to_obj[p]
        self._p_to_u_to_obj[p][u] = obj
        assert u not in self._u_to_obj
        self._u_to_obj[u] = obj

    def _remove_object(self, u: bytes) -> None:
        obj = self._u_to_obj[u]
        del self._u_to_obj[u]
        del self._p_to_u_to_obj[obj.info.parent_uuid][u]

    @property
    def name(self) -> str:
        return self._name

    @property
    def s3(self) -> S3Client:
        return self._s3

    @property
    def bucket(self) -> str:
        return self._bucket

    def get_objects(self, source: Source) -> Mapping[bytes, BackupObject]:
        return self._p_to_u_to_obj.get(source.info.uuid, {})

    def upload(
        self,
        *,
        snapshot_dir: SnapshotDir,
        snapshot_id: int,
        send_parent_id: int | None,
        key: str,
        create_pipe: CreatePipe,
    ) -> ObjectStat:
        _LOG.info(
            "creating backup of %s (%s) on %s",
            snapshot_dir.get_path(snapshot_id),
            f"differential from {snapshot_dir.get_path(send_parent_id)}"
            if send_parent_id
            else "full",
            self.name,
        )

        try:
            with ExitStack() as stack:
                # The stack order is important. On an error, we want to close
                # the pipe first. This will allow the send() and upload to
                # fail.
                executor = stack.enter_context(ThreadPoolExecutor())
                r, w = stack.enter_context(create_pipe())

                def send() -> None:
                    # close the pipe when the send completes
                    with w:
                        snapshot_dir.send(
                            dst=w,
                            snapshot_id=snapshot_id,
                            parent_id=send_parent_id or 0,
                        )

                send_future = executor.submit(send)
                upload_non_seekable_stream_via_tempfile(
                    client=self._s3,
                    bucket=self._bucket,
                    key=key,
                    stream=r,
                    part_size=DEFAULT_PART_SIZE,
                )
                send_future.result()
        except BaseException:
            # Assume the backup is corrupted
            self._s3.delete_objects(
                Bucket=self._bucket, Delete={"Quiet": True, "Objects": [{"Key": key}]}
            )
            raise

        stat = ObjectStat()
        self._add_object(
            BackupObject(key=key, info=BackupInfo.from_path(key), stat=stat)
        )
        return stat

    def delete(self, *keys: str) -> None:
        for i in range(0, len(keys), 1000):
            batch = keys[i : i + 1000]
            for key in batch:
                _LOG.info("deleting backup %s", key)
            # Do we need to inspect the response for individual errors, or will we
            # raise an exception in this case? The docs are thousands of words long
            # but don't explain this
            self._s3.delete_objects(
                Bucket=self._bucket,
                Delete={"Quiet": True, "Objects": [{"Key": key} for key in batch]},
            )
            for key in batch:
                self._remove_object(BackupInfo.from_path(key).uuid)


class ConfigTuple(NamedTuple):
    source: Source
    snapshot_dir: SnapshotDir
    remote: Remote
    policy: Policy
    create_pipe: CreatePipe


class AssessedSnapshot(NamedTuple):
    source: Source
    snapshot_dir: SnapshotDir
    info: SubvolInfo
    meta: KeepMeta


class AssessedBackup(NamedTuple):
    source: Source
    remote: Remote
    info: BackupInfo
    stat: ObjectStat | None
    key: str
    meta: KeepMeta
    create_pipe: CreatePipe


class Assessment(NamedTuple):
    snapshots: dict[bytes, AssessedSnapshot]
    backups: dict[tuple[Remote, bytes], AssessedBackup]


def _maybe_create_snapshot(
    source: Source, snapshot_dir: SnapshotDir
) -> SubvolInfo | None:
    snaps = snapshot_dir.get_snapshots(source)
    if snaps:
        max_ctransid = max(snap.ctransid for snap in snaps.values())
        if source.info.ctransid <= max_ctransid:
            return None
    return snapshot_dir.create_snapshot(source, source.get_new_snapshot_name())


def destroy_new_snapshots(asmt: Assessment) -> None:
    for snap in asmt.snapshots.values():
        if snap.meta.flags & Flags.New:
            snap.snapshot_dir.destroy_snapshot(snap.info.id)


def assess(*cfg_tuples: ConfigTuple) -> Assessment:
    snap_meta: dict[bytes, KeepMeta] = {}
    snaps: dict[bytes, AssessedSnapshot] = {}
    backups: dict[tuple[Remote, bytes], AssessedBackup] = {}

    for cfg_tuple in cfg_tuples:
        created_info = _maybe_create_snapshot(cfg_tuple.source, cfg_tuple.snapshot_dir)
        if created_info is not None:
            snap_meta[created_info.uuid] = KeepMeta(flags=Flags.New)

        tuple_snaps = cfg_tuple.snapshot_dir.get_snapshots(cfg_tuple.source)

        for uuid, info in tuple_snaps.items():
            snap = snaps.get(uuid)
            if not snap:
                snaps[uuid] = AssessedSnapshot(
                    source=cfg_tuple.source,
                    snapshot_dir=cfg_tuple.snapshot_dir,
                    info=info,
                    meta=snap_meta.get(uuid) or KeepMeta(),
                )
            else:
                assert snap.snapshot_dir is cfg_tuple.snapshot_dir

        tuple_objects = cfg_tuple.remote.get_objects(cfg_tuple.source)
        for uuid, obj in tuple_objects.items():
            assert (cfg_tuple.remote, uuid) not in backups
            backups[(cfg_tuple.remote, uuid)] = AssessedBackup(
                source=cfg_tuple.source,
                remote=cfg_tuple.remote,
                info=obj.info,
                stat=obj.stat,
                key=obj.key,
                meta=KeepMeta(),
                create_pipe=cfg_tuple.create_pipe,
            )

        result = resolve(
            snapshots=tuple_snaps.values(),
            backups=[obj.info for obj in tuple_objects.values()],
            policy=cfg_tuple.policy,
            mk_backup=backup_of_snapshot,
        )

        for uuid, result_snap in result.keep_snapshots.items():
            snaps[uuid] = snaps[uuid]._replace(meta=snaps[uuid].meta | result_snap.meta)

        for uuid, result_backup in result.keep_backups.items():
            backup = backups.get((cfg_tuple.remote, uuid))
            if backup:
                # keep an existing backup
                backups[(cfg_tuple.remote, uuid)] = backup._replace(
                    meta=backup.meta | result_backup.meta
                )
            else:
                # keep a new backup
                backups[(cfg_tuple.remote, uuid)] = AssessedBackup(
                    source=cfg_tuple.source,
                    remote=cfg_tuple.remote,
                    info=result_backup.item,
                    stat=None,
                    key=cfg_tuple.source.get_backup_key(result_backup.item),
                    meta=result_backup.meta,
                    create_pipe=cfg_tuple.create_pipe,
                )

    return Assessment(snapshots=snaps, backups=backups)


def assessment_to_actions(asmt: Assessment) -> Actions:
    rename_snapshots = []
    destroy_snapshots = []
    upload_backups = []
    delete_backups = []

    for snap in asmt.snapshots.values():
        if snap.meta.reasons:
            name = snap.snapshot_dir.get_name(snap.info.id)
            target_name = snap.source.get_snapshot_name(snap.info)
            if name != target_name:
                rename_snapshots.append(
                    RenameSnapshot(
                        snapshot_dir=snap.snapshot_dir,
                        info=snap.info,
                        target_name=target_name,
                    )
                )
        else:
            destroy_snapshots.append(
                DestroySnapshot(snapshot_dir=snap.snapshot_dir, info=snap.info)
            )

    for (_, uuid), backup in asmt.backups.items():
        if backup.meta.reasons:
            if backup.meta.flags & Flags.New:
                if backup.info.send_parent_uuid:
                    send_parent = asmt.snapshots[backup.info.send_parent_uuid].info
                else:
                    send_parent = None
                upload_backups.append(
                    UploadBackup(
                        remote=backup.remote,
                        key=backup.key,
                        snapshot_dir=asmt.snapshots[uuid].snapshot_dir,
                        info=asmt.snapshots[uuid].info,
                        send_parent=send_parent,
                        create_pipe=backup.create_pipe,
                    )
                )
        else:
            assert backup.stat is not None
            delete_backups.append(
                DeleteBackup(
                    remote=backup.remote,
                    key=backup.key,
                    info=backup.info,
                    stat=backup.stat,
                )
            )

    rename_snapshots.sort(key=lambda args: args.snapshot_dir.get_path(args.info.id))
    upload_backups.sort(key=lambda args: args.snapshot_dir.get_path(args.info.id))
    destroy_snapshots.sort(key=lambda args: args.snapshot_dir.get_path(args.info.id))
    delete_backups.sort(key=lambda args: (args.remote.name, args.key))

    return Actions(
        rename_snapshots=rename_snapshots,
        upload_backups=upload_backups,
        destroy_snapshots=destroy_snapshots,
        delete_backups=delete_backups,
    )


class RenameSnapshot(NamedTuple):
    snapshot_dir: SnapshotDir
    info: SubvolInfo
    target_name: str

    def __call__(self) -> None:
        self.snapshot_dir.rename_snapshot(self.info.id, self.target_name)


class DestroySnapshot(NamedTuple):
    snapshot_dir: SnapshotDir
    info: SubvolInfo

    def __call__(self) -> None:
        self.snapshot_dir.destroy_snapshot(self.info.id)


class UploadBackup(NamedTuple):
    remote: Remote
    key: str
    snapshot_dir: SnapshotDir
    info: SubvolInfo
    send_parent: SubvolInfo | None
    create_pipe: CreatePipe

    def __call__(self) -> None:
        self.remote.upload(
            snapshot_dir=self.snapshot_dir,
            snapshot_id=self.info.id,
            send_parent_id=self.send_parent.id if self.send_parent else None,
            key=self.key,
            create_pipe=self.create_pipe,
        )


class DeleteBackup(NamedTuple):
    remote: Remote
    key: str
    info: BackupInfo
    stat: ObjectStat


def delete_backups(*delete_backups: DeleteBackup) -> None:
    remote_to_delete_keys: dict[Remote, list[str]] = defaultdict(list)
    for delete_backup_intent in delete_backups:
        remote_to_delete_keys[delete_backup_intent.remote].append(
            delete_backup_intent.key
        )
    for remote, delete_keys in remote_to_delete_keys.items():
        remote.delete(*delete_keys)


class Actions(NamedTuple):
    rename_snapshots: list[RenameSnapshot]
    upload_backups: list[UploadBackup]
    destroy_snapshots: list[DestroySnapshot]
    delete_backups: list[DeleteBackup]

    def any_actions(self) -> bool:
        return bool(
            self.rename_snapshots
            or self.upload_backups
            or self.destroy_snapshots
            or self.delete_backups
        )

    def execute(self) -> None:
        for rename_snapshot in self.rename_snapshots:
            rename_snapshot()
        for upload_backup in self.upload_backups:
            upload_backup()
        for destroy_snapshot in self.destroy_snapshots:
            destroy_snapshot()

        delete_backups(*self.delete_backups)
