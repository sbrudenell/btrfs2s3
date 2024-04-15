from __future__ import annotations

import collections
import contextlib
from contextvars import ContextVar
import enum
from queue import SimpleQueue
from typing import Collection
from typing import Generic
from typing import Hashable
from typing import Iterator
from typing import Protocol
from typing import TypeAlias
import uuid
import warnings

from btrfsutil import SubvolumeInfo
from typing_extensions import NamedTuple
from typing_extensions import TypeVar

from btrfs2s3._internal.util import backup_of_snapshot
from btrfs2s3._internal.util import (
    IsTimeSpanRetained as IsTimeSpanRetained,  # noqa: PLC0414
)
from btrfs2s3._internal.util import IterTimeSpans as IterTimeSpans  # noqa: PLC0414
from btrfs2s3.backups import BackupInfo


class _Info(Protocol):
    @property
    def uuid(self) -> bytes: ...
    @property
    def ctime(self) -> float: ...
    @property
    def ctransid(self) -> int: ...


_TS = TypeVar("_TS", bound=Hashable)
_I = TypeVar("_I", bound=_Info)


class _Index(Generic[_I, _TS]):
    def __init__(
        self, *, items: Collection[_I], iter_time_spans: IterTimeSpans[_TS]
    ) -> None:
        self.iter_time_spans = iter_time_spans
        self._item_by_uuid = {}
        self._items_by_time_span = collections.defaultdict(list)

        for item in items:
            self._item_by_uuid[item.uuid] = item
            for time_span in self.iter_time_spans(item.ctime):
                self._items_by_time_span[time_span].append(item)

    def get_nominal(self, time_span: _TS) -> _I | None:
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

    def get_all_time_spans(self) -> Collection[_TS]:
        return self._items_by_time_span.keys()


class ReasonCode(enum.Flag):
    Retained = enum.auto()
    New = enum.auto()
    ReplacingNewer = enum.auto()
    NoSnapshot = enum.auto()
    SnapshotIsNewer = enum.auto()
    MostRecent = enum.auto()
    SendAncestor = enum.auto()


_EMPTY_REASON_CODE = ReasonCode(0)


class Reason(NamedTuple, Generic[_TS]):
    code: ReasonCode = _EMPTY_REASON_CODE
    time_span: _TS | None = None
    other: bytes | None = None


class _MarkedItem(NamedTuple, Generic[_I, _TS]):
    item: _I
    reasons: set[Reason[_TS]]


class _Marker(Generic[_I, _TS]):
    def __init__(self) -> None:
        self._result: dict[bytes, _MarkedItem[_I, _TS]] = {}
        self._reason_code_ctx: ContextVar[ReasonCode] = ContextVar(
            "reason_code", default=_EMPTY_REASON_CODE
        )
        self._time_span_ctx: ContextVar[_TS | None] = ContextVar(
            "time_span", default=None
        )

    @contextlib.contextmanager
    def with_code(self, code: ReasonCode) -> Iterator[None]:
        existing = self._reason_code_ctx.get()
        token = self._reason_code_ctx.set(code | existing)
        try:
            yield
        finally:
            self._reason_code_ctx.reset(token)

    @contextlib.contextmanager
    def with_time_span(self, time_span: _TS) -> Iterator[None]:
        token = self._time_span_ctx.set(time_span)
        try:
            yield
        finally:
            self._time_span_ctx.reset(token)

    def _mk_reason(
        self,
        *,
        code: ReasonCode = _EMPTY_REASON_CODE,
        time_span: _TS | None = None,
        other: bytes | None = None,
    ) -> Reason[_TS]:
        code |= self._reason_code_ctx.get()
        if code == _EMPTY_REASON_CODE:
            raise AssertionError
        time_span = time_span or self._time_span_ctx.get()
        return Reason(code=code, time_span=time_span, other=other)

    def get_result(self) -> dict[bytes, _MarkedItem[_I, _TS]]:
        return self._result

    def mark(
        self,
        item: _I,
        *,
        code: ReasonCode = _EMPTY_REASON_CODE,
        time_span: _TS | None = None,
        other: bytes | None = None,
    ) -> None:
        if item.uuid not in self._result:
            self._result[item.uuid] = _MarkedItem(item=item, reasons=set())
        self._result[item.uuid].reasons.add(
            self._mk_reason(code=code, time_span=time_span, other=other)
        )


KeepSnapshot: TypeAlias = _MarkedItem[SubvolumeInfo, _TS]
KeepBackup: TypeAlias = _MarkedItem[BackupInfo, _TS]


class Result(NamedTuple, Generic[_TS]):
    keep_snapshots: dict[bytes, KeepSnapshot[_TS]]
    keep_backups: dict[bytes, KeepBackup[_TS]]


class _Resolver(Generic[_TS]):
    def __init__(
        self,
        *,
        snapshots: Collection[SubvolumeInfo],
        backups: Collection[BackupInfo],
        iter_time_spans: IterTimeSpans[_TS],
    ) -> None:
        self._snapshots = _Index(items=snapshots, iter_time_spans=iter_time_spans)
        self._backups = _Index(items=backups, iter_time_spans=iter_time_spans)

        self._keep_snapshots: _Marker[SubvolumeInfo, _TS] = _Marker()
        self._keep_backups: _Marker[BackupInfo, _TS] = _Marker()

    @contextlib.contextmanager
    def _with_time_span(self, time_span: _TS) -> Iterator[None]:
        with self._keep_snapshots.with_time_span(
            time_span
        ), self._keep_backups.with_time_span(time_span):
            yield

    @contextlib.contextmanager
    def _with_code(self, code: ReasonCode) -> Iterator[None]:
        with self._keep_snapshots.with_code(code), self._keep_backups.with_code(code):
            yield

    def get_result(self) -> Result[_TS]:
        return Result(
            keep_snapshots=self._keep_snapshots.get_result(),
            keep_backups=self._keep_backups.get_result(),
        )

    def _keep_backup_of_snapshot(
        self,
        snapshot: SubvolumeInfo,
        *,
        code: ReasonCode = _EMPTY_REASON_CODE,
        time_span: _TS | None = None,
        other: bytes | None = None,
    ) -> BackupInfo:
        backup: BackupInfo | None = None
        # Use an existing backup when available
        if snapshot.uuid in self._keep_backups.get_result():
            backup = self._keep_backups.get_result()[snapshot.uuid].item
        if backup is None:
            backup = self._backups.get(snapshot.uuid)
        if backup is None:
            # Determine send-parent for a new backup
            send_parent: SubvolumeInfo | None = None
            for snapshot_time_span in self._snapshots.iter_time_spans(snapshot.ctime):
                nominal_snapshot = self._snapshots.get_nominal(snapshot_time_span)
                if nominal_snapshot is None:
                    raise AssertionError
                if nominal_snapshot.uuid == snapshot.uuid:
                    break
                send_parent = nominal_snapshot

            backup = backup_of_snapshot(snapshot, send_parent=send_parent)

        self._keep_backups.mark(backup, code=code, time_span=time_span, other=other)
        return backup

    def _keep_snapshot_and_backup_for_time_span(self, time_span: _TS) -> None:
        nominal_snapshot = self._snapshots.get_nominal(time_span)
        nominal_backup = self._backups.get_nominal(time_span)

        # NB: we could have a nominal backup older than our nominal snapshot.
        # If so, we keep the existing backup, but don't back up the snapshot.
        # There's some duplication in the following logic, but the duplicated
        # cases are expected to be rare and this makes it easier to follow
        if nominal_snapshot:
            self._keep_snapshots.mark(nominal_snapshot)
            if nominal_backup is None:
                self._keep_backup_of_snapshot(nominal_snapshot, code=ReasonCode.New)
            elif nominal_backup.ctransid > nominal_snapshot.ctransid:
                self._keep_backup_of_snapshot(
                    nominal_snapshot, code=ReasonCode.ReplacingNewer
                )
        if nominal_backup:
            if nominal_snapshot is None:
                self._keep_backups.mark(nominal_backup, code=ReasonCode.NoSnapshot)
            elif nominal_backup.ctransid < nominal_snapshot.ctransid:
                self._keep_backups.mark(nominal_backup, code=ReasonCode.SnapshotIsNewer)
            elif nominal_backup.ctransid == nominal_snapshot.ctransid:
                self._keep_backups.mark(nominal_backup)

    def keep_snapshots_and_backups_for_retained_time_spans(
        self, is_time_span_retained: IsTimeSpanRetained[_TS]
    ) -> None:
        with self._with_code(ReasonCode.Retained):
            all_time_spans = set(self._snapshots.get_all_time_spans()) | set(
                self._backups.get_all_time_spans()
            )
            for time_span in all_time_spans:
                if not is_time_span_retained(time_span):
                    continue
                with self._with_time_span(time_span):
                    self._keep_snapshot_and_backup_for_time_span(time_span)

    def _keep_most_recent_snapshot(self) -> None:
        most_recent_snapshot = self._snapshots.get_most_recent()
        if most_recent_snapshot:
            self._keep_snapshots.mark(most_recent_snapshot)
            self._keep_backup_of_snapshot(most_recent_snapshot)

    def keep_most_recent_snapshot(self) -> None:
        with self._with_code(ReasonCode.MostRecent):
            self._keep_most_recent_snapshot()

    def _keep_send_ancestors_of_backups(self) -> None:
        # Ensure the send-parent ancestors of any kept backups are also kept
        backups_to_check: SimpleQueue[BackupInfo] = SimpleQueue()
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
                # backup should be retained, but the December backup's send-parent
                # is last year's yearly backup
                self._keep_backups.mark(parent_backup, other=backup.uuid)
            else:
                parent_snapshot = self._snapshots.get(backup.send_parent_uuid)
                if parent_snapshot:
                    parent_backup = self._keep_backup_of_snapshot(
                        parent_snapshot, code=ReasonCode.New, other=backup.uuid
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
        with self._with_code(ReasonCode.SendAncestor):
            self._keep_send_ancestors_of_backups()


def resolve(
    *,
    snapshots: Collection[SubvolumeInfo],
    backups: Collection[BackupInfo],
    iter_time_spans: IterTimeSpans[_TS],
    is_time_span_retained: IsTimeSpanRetained[_TS],
) -> Result[_TS]:
    resolver = _Resolver(
        snapshots=snapshots, backups=backups, iter_time_spans=iter_time_spans
    )
    resolver.keep_snapshots_and_backups_for_retained_time_spans(is_time_span_retained)

    resolver.keep_most_recent_snapshot()

    # Future: is there a case where we need to keep a snapshot because it'll be
    # used as the send-parent of a future backup, but *isn't* otherwise kept?

    resolver.keep_send_ancestors_of_backups()
    return resolver.get_result()
