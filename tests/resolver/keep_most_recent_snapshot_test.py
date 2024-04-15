from __future__ import annotations

import functools
import random
from typing import Iterable
from typing import Protocol
from typing import TYPE_CHECKING

import arrow
from btrfs2s3._internal.arrowutil import iter_intersecting_time_spans
from btrfs2s3._internal.util import backup_of_snapshot
from btrfs2s3._internal.util import mksubvol
from btrfs2s3._internal.util import TS
from btrfs2s3.resolver import _Resolver
from btrfs2s3.resolver import KeepBackup
from btrfs2s3.resolver import KeepSnapshot
from btrfs2s3.resolver import Reason
from btrfs2s3.resolver import ReasonCode
from btrfs2s3.resolver import Result
import pytest

if TYPE_CHECKING:
    from btrfsutil import SubvolumeInfo

mkuuid = functools.partial(random.randbytes, 16)


@pytest.fixture()
def parent_uuid() -> bytes:
    return mkuuid()


class MkSnap(Protocol):
    def __call__(self, *, t: str | None = None, i: int = 0) -> SubvolumeInfo: ...


@pytest.fixture()
def mksnap(parent_uuid: bytes) -> MkSnap:
    def inner(*, t: str | None = None, i: int = 0) -> SubvolumeInfo:
        a = arrow.get() if t is None else arrow.get(t)
        return mksubvol(
            uuid=mkuuid(), parent_uuid=parent_uuid, ctime=a.timestamp(), ctransid=i
        )

    return inner


def iter_time_spans(timestamp: float) -> Iterable[TS]:
    return iter_intersecting_time_spans(arrow.get(timestamp), bounds="[]")


def test_noop() -> None:
    resolver = _Resolver(snapshots=(), backups=(), iter_time_spans=iter_time_spans)

    resolver.keep_most_recent_snapshot()

    assert resolver.get_result() == Result[TS](keep_snapshots={}, keep_backups={})


def test_one_snapshot(mksnap: MkSnap) -> None:
    snapshot = mksnap()
    resolver = _Resolver(
        snapshots=(snapshot,), backups=(), iter_time_spans=iter_time_spans
    )

    resolver.keep_most_recent_snapshot()

    expected_backup = backup_of_snapshot(snapshot, send_parent=None)
    assert resolver.get_result() == Result[TS](
        keep_snapshots={
            snapshot.uuid: KeepSnapshot[TS](
                item=snapshot, reasons={Reason(code=ReasonCode.MostRecent)}
            )
        },
        keep_backups={
            expected_backup.uuid: KeepBackup[TS](
                item=expected_backup, reasons={Reason(code=ReasonCode.MostRecent)}
            )
        },
    )


def test_multiple_snapshots_keep_most_recent(mksnap: MkSnap) -> None:
    snapshot1 = mksnap(i=1)
    snapshot2 = mksnap(i=2)
    backup1 = backup_of_snapshot(snapshot1, send_parent=None)
    resolver = _Resolver(
        snapshots=(snapshot1, snapshot2),
        backups=(backup1,),
        iter_time_spans=iter_time_spans,
    )

    resolver.keep_most_recent_snapshot()

    expected_backup = backup_of_snapshot(snapshot2, send_parent=snapshot1)
    assert resolver.get_result() == Result[TS](
        keep_snapshots={
            snapshot2.uuid: KeepSnapshot[TS](
                item=snapshot2, reasons={Reason(code=ReasonCode.MostRecent)}
            )
        },
        keep_backups={
            expected_backup.uuid: KeepBackup[TS](
                item=expected_backup, reasons={Reason(code=ReasonCode.MostRecent)}
            )
        },
    )
