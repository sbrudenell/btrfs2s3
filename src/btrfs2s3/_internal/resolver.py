# btrfs2s3 - maintains a tree of differential backups in object storage.
#
# Copyright (C) 2024-2025 Steven Brudenell and other contributors.
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

import collections
import contextlib
from contextvars import ContextVar
import dataclasses
from dataclasses import field
import enum
from queue import SimpleQueue
from typing import Generic
from typing import Protocol
from typing import TYPE_CHECKING
from typing import TypeVar
import uuid
import warnings

from typing_extensions import Self

if TYPE_CHECKING:
    from collections.abc import Collection
    from collections.abc import Iterator

    from btrfs2s3._internal.preservation import Policy
    from btrfs2s3._internal.preservation import TS


class InfoLike(Protocol):
    @property
    def uuid(self) -> bytes: ...
    @property
    def ctime(self) -> float: ...
    @property
    def ctransid(self) -> int: ...


class BackupLike(InfoLike, Protocol):
    @property
    def send_parent_uuid(self) -> bytes | None: ...


_I = TypeVar("_I", bound=InfoLike)
_S_contra = TypeVar("_S_contra", bound=InfoLike, contravariant=True)
_B_co = TypeVar("_B_co", bound=BackupLike, covariant=True)


class _Index(Generic[_I]):
    def __init__(self, *, items: Collection[_I], policy: Policy) -> None:
        self._item_by_uuid = {}
        self._items_by_time_span = collections.defaultdict(list)

        for item in items:
            self._item_by_uuid[item.uuid] = item
            for time_span in policy.iter_time_spans(item.ctime):
                self._items_by_time_span[time_span].append(item)

    def get_nominal(self, time_span: TS) -> _I | None:
        items = self._items_by_time_span.get(time_span)
        if items:
            return min(items, key=lambda i: i.ctransid)
        return None

    def get(self, uuid: bytes) -> _I | None:
        return self._item_by_uuid.get(uuid)

    def get_most_recent(self) -> _I | None:
        if self._item_by_uuid:
            return max(self._item_by_uuid.values(), key=lambda i: i.ctransid)
        return None

    def get_all_time_spans(self) -> Collection[TS]:
        return self._items_by_time_span.keys()


class Reasons(enum.Flag):
    Empty = 0
    Preserved = enum.auto()
    MostRecent = enum.auto()
    SendAncestor = enum.auto()


class Flags(enum.Flag):
    Empty = 0
    New = enum.auto()
    ReplacingNewer = enum.auto()
    NoSnapshot = enum.auto()
    SnapshotIsNewer = enum.auto()


@dataclasses.dataclass
class KeepMeta:
    reasons: Reasons = Reasons.Empty
    flags: Flags = Flags.Empty
    time_spans: set[TS] = field(default_factory=set)
    other_uuids: set[bytes] = field(default_factory=set)

    def __or__(self, other: Self) -> Self:
        return self.__class__(
            reasons=self.reasons | other.reasons,
            flags=self.flags | other.flags,
            time_spans=self.time_spans | other.time_spans,
            other_uuids=self.other_uuids | other.other_uuids,
        )


@dataclasses.dataclass
class Item(Generic[_I]):
    item: _I
    meta: KeepMeta = field(default_factory=KeepMeta)


class _Marker(Generic[_I]):
    def __init__(self) -> None:
        self._result: dict[bytes, Item[_I]] = {}
        self._reasons_ctx: ContextVar[Reasons] = ContextVar(
            "reasons", default=Reasons.Empty
        )
        self._flags_ctx: ContextVar[Flags] = ContextVar("flags", default=Flags.Empty)
        self._time_span_ctx: ContextVar[frozenset[TS]] = ContextVar(
            "time_span", default=frozenset()
        )

    @contextlib.contextmanager
    def with_reasons(self, reasons: Reasons) -> Iterator[None]:
        existing = self._reasons_ctx.get()
        token = self._reasons_ctx.set(reasons | existing)
        try:
            yield
        finally:
            self._reasons_ctx.reset(token)

    @contextlib.contextmanager
    def with_time_span(self, time_span: TS) -> Iterator[None]:
        existing = self._time_span_ctx.get()
        token = self._time_span_ctx.set(existing | {time_span})
        try:
            yield
        finally:
            self._time_span_ctx.reset(token)

    def get_result(self) -> dict[bytes, Item[_I]]:
        return self._result

    def mark(
        self, item: _I, *, flags: Flags = Flags.Empty, other_uuid: bytes | None = None
    ) -> None:
        if item.uuid not in self._result:
            self._result[item.uuid] = Item(item=item)
        marked = self._result[item.uuid]
        update = KeepMeta(
            reasons=self._reasons_ctx.get(),
            flags=flags | self._flags_ctx.get(),
            time_spans=set(self._time_span_ctx.get()),
            other_uuids={other_uuid} if other_uuid is not None else set(),
        )
        if not update.reasons:
            raise AssertionError
        marked.meta |= update


@dataclasses.dataclass(frozen=True)
class Result(Generic[_S_contra, _B_co]):
    keep_snapshots: dict[bytes, Item[_S_contra]] = field(default_factory=dict)
    keep_backups: dict[bytes, Item[_B_co]] = field(default_factory=dict)


class MkBackup(Protocol, Generic[_S_contra, _B_co]):
    def __call__(
        self, snapshot: _S_contra, /, send_parent: _S_contra | None = None
    ) -> _B_co: ...


class _Resolver(Generic[_S_contra, _B_co]):
    def __init__(
        self,
        *,
        snapshots: Collection[_S_contra],
        backups: Collection[_B_co],
        policy: Policy,
        mk_backup: MkBackup[_S_contra, _B_co],
    ) -> None:
        self._snapshots = _Index(items=snapshots, policy=policy)
        self._backups = _Index(items=backups, policy=policy)
        self._policy = policy
        self._mk_backup = mk_backup

        self._keep_snapshots: _Marker[_S_contra] = _Marker()
        self._keep_backups: _Marker[_B_co] = _Marker()

    @contextlib.contextmanager
    def _with_time_span(self, time_span: TS) -> Iterator[None]:
        with self._keep_snapshots.with_time_span(time_span):
            with self._keep_backups.with_time_span(time_span):
                yield

    @contextlib.contextmanager
    def _with_reasons(self, reasons: Reasons) -> Iterator[None]:
        with self._keep_snapshots.with_reasons(reasons):
            with self._keep_backups.with_reasons(reasons):
                yield

    def get_result(self) -> Result[_S_contra, _B_co]:
        return Result(
            keep_snapshots=self._keep_snapshots.get_result(),
            keep_backups=self._keep_backups.get_result(),
        )

    def _keep_backup_of_snapshot(
        self,
        snapshot: _S_contra,
        *,
        flags: Flags = Flags.Empty,
        other_uuid: bytes | None = None,
    ) -> _B_co:
        backup: _B_co | None = None
        # Use an existing backup when available
        if snapshot.uuid in self._keep_backups.get_result():
            backup = self._keep_backups.get_result()[snapshot.uuid].item
        if backup is None:
            backup = self._backups.get(snapshot.uuid)
        if backup is None:
            flags |= Flags.New
            # Determine send-parent for a new backup
            send_parent: _S_contra | None = None
            for snapshot_time_span in self._policy.iter_time_spans(snapshot.ctime):
                nominal_snapshot = self._snapshots.get_nominal(snapshot_time_span)
                if nominal_snapshot is None:
                    raise AssertionError
                if nominal_snapshot.uuid == snapshot.uuid:
                    break
                send_parent = nominal_snapshot

            backup = self._mk_backup(snapshot, send_parent=send_parent)

        self._keep_backups.mark(backup, flags=flags, other_uuid=other_uuid)
        return backup

    def _keep_snapshot_and_backup_for_time_span(self, time_span: TS) -> None:
        nominal_snapshot = self._snapshots.get_nominal(time_span)
        nominal_backup = self._backups.get_nominal(time_span)

        # NB: we could have a nominal backup older than our nominal snapshot.
        # If so, we keep the existing backup, but don't back up the snapshot.
        # There's some duplication in the following logic, but the duplicated
        # cases are expected to be rare and this makes it easier to follow
        if nominal_snapshot:
            self._keep_snapshots.mark(nominal_snapshot)
            if nominal_backup is None:
                self._keep_backup_of_snapshot(nominal_snapshot, flags=Flags.New)
            elif nominal_backup.ctransid > nominal_snapshot.ctransid:
                self._keep_backup_of_snapshot(
                    nominal_snapshot, flags=Flags.ReplacingNewer
                )
        if nominal_backup:
            if nominal_snapshot is None:
                self._keep_backups.mark(nominal_backup, flags=Flags.NoSnapshot)
            elif nominal_backup.ctransid < nominal_snapshot.ctransid:
                self._keep_backups.mark(nominal_backup, flags=Flags.SnapshotIsNewer)
            elif nominal_backup.ctransid == nominal_snapshot.ctransid:
                self._keep_backups.mark(nominal_backup)

    def keep_snapshots_and_backups_for_preserved_time_spans(self) -> None:
        with self._with_reasons(Reasons.Preserved):
            all_time_spans = set(self._snapshots.get_all_time_spans()) | set(
                self._backups.get_all_time_spans()
            )
            for time_span in all_time_spans:
                if not self._policy.should_preserve_for_time_span(time_span):
                    continue
                with self._with_time_span(time_span):
                    self._keep_snapshot_and_backup_for_time_span(time_span)

    def _keep_most_recent_snapshot(self) -> None:
        most_recent_snapshot = self._snapshots.get_most_recent()
        if most_recent_snapshot:
            self._keep_snapshots.mark(most_recent_snapshot)
            self._keep_backup_of_snapshot(most_recent_snapshot)

    def keep_most_recent_snapshot(self) -> None:
        with self._with_reasons(Reasons.MostRecent):
            self._keep_most_recent_snapshot()

    def _keep_send_ancestors_of_backups(self) -> None:
        # Ensure the send-parent ancestors of any kept backups are also kept
        backups_to_check: SimpleQueue[_B_co] = SimpleQueue()
        for marked_item in self._keep_backups.get_result().values():
            backups_to_check.put(marked_item.item)
        while not backups_to_check.empty():
            backup = backups_to_check.get()
            if not backup.send_parent_uuid:
                continue
            if backup.send_parent_uuid in self._keep_backups.get_result():
                continue
            parent_backup = self._backups.get(backup.send_parent_uuid)
            if parent_backup:
                # This can be common. For example in January, if December's monthly
                # backup should be preserved, but the December backup's send-parent
                # is last year's yearly backup
                self._keep_backups.mark(parent_backup, other_uuid=backup.uuid)
            else:
                parent_snapshot = self._snapshots.get(backup.send_parent_uuid)
                if parent_snapshot:
                    parent_backup = self._keep_backup_of_snapshot(
                        parent_snapshot, flags=Flags.New, other_uuid=backup.uuid
                    )
                else:
                    warnings.warn(
                        f"Backup chain is broken: {uuid.UUID(bytes=backup.uuid)} "
                        f"has parent {uuid.UUID(bytes=backup.send_parent_uuid)}, "
                        "which is missing",
                        stacklevel=1,
                    )
            if parent_backup:
                backups_to_check.put(parent_backup)

    def keep_send_ancestors_of_backups(self) -> None:
        with self._with_reasons(Reasons.SendAncestor):
            self._keep_send_ancestors_of_backups()


def resolve(
    *,
    snapshots: Collection[_S_contra],
    backups: Collection[_B_co],
    policy: Policy,
    mk_backup: MkBackup[_S_contra, _B_co],
) -> Result[_S_contra, _B_co]:
    resolver = _Resolver(
        snapshots=snapshots, backups=backups, policy=policy, mk_backup=mk_backup
    )
    resolver.keep_snapshots_and_backups_for_preserved_time_spans()

    resolver.keep_most_recent_snapshot()

    # Future: is there a case where we need to keep a snapshot because it'll be
    # used as the send-parent of a future backup, but *isn't* otherwise kept?

    resolver.keep_send_ancestors_of_backups()
    return resolver.get_result()
