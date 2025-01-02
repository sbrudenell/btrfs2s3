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
from contextlib import contextmanager
from contextlib import ExitStack
import logging
import os
from typing import NamedTuple
from typing import Protocol
from typing import TYPE_CHECKING

import arrow

from btrfs2s3._internal import resolver
from btrfs2s3._internal.backups import BackupInfo
from btrfs2s3._internal.btrfsioctl import create_snap
from btrfs2s3._internal.btrfsioctl import destroy_snap
from btrfs2s3._internal.btrfsioctl import FIRST_FREE_OBJECTID
from btrfs2s3._internal.btrfsioctl import opendir
from btrfs2s3._internal.btrfsioctl import send
from btrfs2s3._internal.btrfsioctl import subvol_info
from btrfs2s3._internal.btrfsioctl import SubvolFlag
from btrfs2s3._internal.btrfsioctl import SubvolInfo
from btrfs2s3._internal.resolver import KeepMeta
from btrfs2s3._internal.resolver import resolve
from btrfs2s3._internal.s3 import iter_backups
from btrfs2s3._internal.stream_uploader import upload_non_seekable_stream_via_tempfile
from btrfs2s3._internal.util import backup_of_snap
from btrfs2s3._internal.util import TZINFO

if TYPE_CHECKING:
    from collections.abc import Collection
    from collections.abc import Iterator
    from collections.abc import Mapping
    from contextlib import AbstractContextManager
    from pathlib import Path
    from typing import IO

    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_s3.type_defs import ObjectTypeDef
    from typing_extensions import Self
    from typing_extensions import TypeAlias

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

    def get_snapshot_name(self, snapshot: SubvolInfo) -> str:
        ctime = arrow.get(snapshot.ctime, tzinfo=TZINFO.get())
        ctime_str = ctime.isoformat(timespec="seconds")
        return f"{self.path.name}.{ctime_str}.{snapshot.ctransid}"

    def get_backup_key(self, info: BackupInfo) -> str:
        suffixes = info.get_path_suffixes(tzinfo=TZINFO.get())
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

    def get_snapshots(self, *, parent_uuid: bytes) -> Mapping[bytes, SubvolInfo]:
        return self._p_to_u_to_snapshot.get(parent_uuid, {})

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
    size: int | None
    storage_class: str | None

    @classmethod
    def create(
        cls, *, size: int | None = None, storage_class: str | None = None
    ) -> Self:
        return cls(size=size, storage_class=storage_class)

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

    def get_objects(self, *, parent_uuid: bytes) -> Mapping[bytes, BackupObject]:
        return self._p_to_u_to_obj.get(parent_uuid, {})

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

        stat = ObjectStat.create()
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


class KeepSnapshotArgs(NamedTuple):
    source: Source
    snapshot_dir: SnapshotDir
    snapshot: SubvolInfo
    meta: KeepMeta


class KeepBackupArgs(NamedTuple):
    source: Source
    remote: Remote
    info: BackupInfo
    stat: ObjectStat | None
    key: str
    meta: KeepMeta


class CreatedSnapshotArgs(NamedTuple):
    source: Source
    snapshot_dir: SnapshotDir
    snapshot: SubvolInfo


class RenameSnapshotArgs(NamedTuple):
    snapshot_dir: SnapshotDir
    snapshot: SubvolInfo
    target_name: str


def rename_snapshot(
    snapshot_dir: SnapshotDir, snapshot: SubvolInfo, target_name: str
) -> None:
    snapshot_dir.rename_snapshot(snapshot.id, target_name)


class DestroySnapshotArgs(NamedTuple):
    snapshot_dir: SnapshotDir
    snapshot: SubvolInfo


def destroy_snapshot(snapshot_dir: SnapshotDir, snapshot: SubvolInfo) -> None:
    snapshot_dir.destroy_snapshot(snapshot.id)


class UploadBackupArgs(NamedTuple):
    remote: Remote
    key: str
    snapshot_dir: SnapshotDir
    snapshot: SubvolInfo
    send_parent: SubvolInfo | None
    create_pipe: CreatePipe


def upload_backup(
    remote: Remote,
    key: str,
    snapshot_dir: SnapshotDir,
    snapshot: SubvolInfo,
    send_parent: SubvolInfo | None,
    create_pipe: CreatePipe,
) -> None:
    remote.upload(
        snapshot_dir=snapshot_dir,
        snapshot_id=snapshot.id,
        send_parent_id=send_parent.id if send_parent else None,
        key=key,
        create_pipe=create_pipe,
    )


class DeleteBackupArgs(NamedTuple):
    remote: Remote
    key: str
    info: BackupInfo
    stat: ObjectStat


Result: TypeAlias = resolver.Result[SubvolInfo, BackupInfo]


class UpdateArgs(NamedTuple):
    source: Source
    snapshot_dir: SnapshotDir
    remote: Remote
    policy: Policy
    create_pipe: CreatePipe

    def get_snaps(self) -> Mapping[bytes, SubvolInfo]:
        return self.snapshot_dir.get_snapshots(parent_uuid=self.source.info.uuid)

    def get_objects(self) -> Mapping[bytes, BackupObject]:
        return self.remote.get_objects(parent_uuid=self.source.info.uuid)


class Update(Protocol):
    def __call__(
        self,
        *,
        source: Source,
        snapshot_dir: SnapshotDir,
        remote: Remote,
        policy: Policy,
        create_pipe: CreatePipe,
    ) -> None: ...


class Plan(NamedTuple):
    @classmethod
    def create(
        cls,
        *,
        keep_snapshots: dict[bytes, KeepSnapshotArgs] | None = None,
        keep_backups: dict[bytes, KeepBackupArgs] | None = None,
        created_snapshots: dict[bytes, CreatedSnapshotArgs] | None = None,
        rename_snapshots: list[RenameSnapshotArgs] | None = None,
        upload_backups: list[UploadBackupArgs] | None = None,
        delete_backups: list[DeleteBackupArgs] | None = None,
        destroy_snapshots: list[DestroySnapshotArgs] | None = None,
    ) -> Self:
        return cls(
            keep_snapshots=keep_snapshots or {},
            keep_backups=keep_backups or {},
            created_snapshots=created_snapshots or {},
            rename_snapshots=rename_snapshots or [],
            upload_backups=upload_backups or [],
            delete_backups=delete_backups or [],
            destroy_snapshots=destroy_snapshots or [],
        )

    keep_snapshots: dict[bytes, KeepSnapshotArgs]
    keep_backups: dict[bytes, KeepBackupArgs]
    created_snapshots: dict[bytes, CreatedSnapshotArgs]
    rename_snapshots: list[RenameSnapshotArgs]
    upload_backups: list[UploadBackupArgs]
    delete_backups: list[DeleteBackupArgs]
    destroy_snapshots: list[DestroySnapshotArgs]

    @contextmanager
    def update(self) -> Iterator[Update]:
        self.rename_snapshots.clear()
        self.destroy_snapshots.clear()

        yield self._update

        for keep_snap in self.keep_snapshots.values():
            if keep_snap.meta.reasons:
                name = keep_snap.snapshot_dir.get_name(keep_snap.snapshot.id)
                target_name = keep_snap.source.get_snapshot_name(keep_snap.snapshot)
                if name != target_name:
                    self.rename_snapshots.append(
                        RenameSnapshotArgs(
                            snapshot_dir=keep_snap.snapshot_dir,
                            snapshot=keep_snap.snapshot,
                            target_name=target_name,
                        )
                    )
            else:
                self.destroy_snapshots.append(
                    DestroySnapshotArgs(
                        snapshot_dir=keep_snap.snapshot_dir, snapshot=keep_snap.snapshot
                    )
                )

        self.rename_snapshots.sort(
            key=lambda args: args.snapshot_dir.get_path(args.snapshot.id)
        )
        self.upload_backups.sort(
            key=lambda args: args.snapshot_dir.get_path(args.snapshot.id)
        )
        self.delete_backups.sort(key=lambda args: (args.remote.name, args.key))
        self.destroy_snapshots.sort(
            key=lambda args: args.snapshot_dir.get_path(args.snapshot.id)
        )

    def _update(
        self,
        *,
        source: Source,
        snapshot_dir: SnapshotDir,
        remote: Remote,
        policy: Policy,
        create_pipe: CreatePipe,
    ) -> None:
        args = UpdateArgs(
            source=source,
            snapshot_dir=snapshot_dir,
            remote=remote,
            policy=policy,
            create_pipe=create_pipe,
        )

        self._maybe_create_new_snapshot(args)

        result = resolve(
            snapshots=args.get_snaps().values(),
            backups=[obj.info for obj in args.get_objects().values()],
            policy=policy,
            mk_backup=backup_of_snap,
        )

        self._update_snapshots(args, result)
        self._update_backups(args, result)

    def _maybe_create_new_snapshot(self, args: UpdateArgs) -> None:
        snaps = args.get_snaps()

        if snaps:
            max_ctransid = max(snap.ctransid for snap in snaps.values())
            if args.source.info.ctransid <= max_ctransid:
                return

        snapshot = args.snapshot_dir.create_snapshot(
            args.source, args.source.get_new_snapshot_name()
        )
        self.created_snapshots[snapshot.uuid] = CreatedSnapshotArgs(
            source=args.source, snapshot_dir=args.snapshot_dir, snapshot=snapshot
        )

    def _update_snapshots(self, args: UpdateArgs, result: Result) -> None:
        snaps = args.get_snaps()

        for uuid, snap in snaps.items():
            if uuid not in self.keep_snapshots:
                self.keep_snapshots[uuid] = KeepSnapshotArgs(
                    source=args.source,
                    snapshot_dir=args.snapshot_dir,
                    snapshot=snap,
                    meta=KeepMeta(),
                )

        for uuid, keep_snap in result.keep_snapshots.items():
            self.keep_snapshots[uuid] = self.keep_snapshots[uuid]._replace(
                meta=self.keep_snapshots[uuid].meta | keep_snap.meta
            )

    def _update_backups(self, args: UpdateArgs, result: Result) -> None:
        snaps = args.get_snaps()
        objects = args.get_objects()

        for uuid, keep_backup in result.keep_backups.items():
            obj = objects.get(uuid)
            if obj:
                info = obj.info
                stat = obj.stat
                key = obj.key
            else:
                info = keep_backup.item
                stat = None
                key = args.source.get_backup_key(info)
                if info.send_parent_uuid:
                    send_parent = snaps[info.send_parent_uuid]
                else:
                    send_parent = None
                self.upload_backups.append(
                    UploadBackupArgs(
                        remote=args.remote,
                        key=key,
                        snapshot_dir=args.snapshot_dir,
                        snapshot=snaps[uuid],
                        send_parent=send_parent,
                        create_pipe=args.create_pipe,
                    )
                )
            self.keep_backups[info.uuid] = KeepBackupArgs(
                source=args.source,
                remote=args.remote,
                info=info,
                stat=stat,
                key=key,
                meta=keep_backup.meta,
            )

        for uuid in objects.keys() - result.keep_backups.keys():
            obj = objects[uuid]
            self.delete_backups.append(
                DeleteBackupArgs(
                    remote=args.remote, key=obj.key, info=obj.info, stat=obj.stat
                )
            )

    def any_actions(self) -> bool:
        return bool(
            self.rename_snapshots
            or self.upload_backups
            or self.destroy_snapshots
            or self.delete_backups
        )

    def execute(self) -> None:
        for rename_snapshot_intent in self.rename_snapshots:
            rename_snapshot(**rename_snapshot_intent._asdict())
        for upload_backup_intent in self.upload_backups:
            upload_backup(**upload_backup_intent._asdict())
        for destroy_snapshot_intent in self.destroy_snapshots:
            destroy_snapshot(**destroy_snapshot_intent._asdict())

        remote_to_delete_keys: dict[Remote, list[str]] = defaultdict(list)
        for delete_backup_intent in self.delete_backups:
            remote_to_delete_keys[delete_backup_intent.remote].append(
                delete_backup_intent.key
            )
        for remote, delete_keys in remote_to_delete_keys.items():
            remote.delete(*delete_keys)

    def undo_created_snapshots(self) -> None:
        for created_snapshot in self.created_snapshots.values():
            destroy_snapshot(
                snapshot_dir=created_snapshot.snapshot_dir,
                snapshot=created_snapshot.snapshot,
            )
