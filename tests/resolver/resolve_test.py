from __future__ import annotations

import functools
import random
from typing import Protocol
from typing import TYPE_CHECKING

import arrow
from btrfs2s3._internal.util import backup_of_snapshot
from btrfs2s3._internal.util import mkretained
from btrfs2s3._internal.util import mksubvol
from btrfs2s3._internal.util import TS
from btrfs2s3.resolver import KeepBackup
from btrfs2s3.resolver import KeepSnapshot
from btrfs2s3.resolver import Reason
from btrfs2s3.resolver import ReasonCode
from btrfs2s3.resolver import resolve
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


def test_noop() -> None:
    iter_time_spans, is_time_span_retained = mkretained(now="2006-01-01", years=(0,))
    result = resolve(
        snapshots=(),
        backups=(),
        iter_time_spans=iter_time_spans,
        is_time_span_retained=is_time_span_retained,
    )

    assert result == Result[TS](keep_snapshots={}, keep_backups={})


def test_one_snapshot_retained(mksnap: MkSnap) -> None:
    iter_time_spans, is_time_span_retained = mkretained(now="2006-01-01", years=(0,))
    snapshot = mksnap(t="2006-01-01", i=1)
    result = resolve(
        snapshots=(snapshot,),
        backups=(),
        iter_time_spans=iter_time_spans,
        is_time_span_retained=is_time_span_retained,
    )

    expected_backup = backup_of_snapshot(snapshot, send_parent=None)
    assert result == Result[TS](
        keep_snapshots={
            snapshot.uuid: KeepSnapshot[TS](
                item=snapshot,
                reasons={
                    Reason(
                        code=ReasonCode.Retained,
                        time_span=(arrow.get("2006-01-01"), arrow.get("2007-01-01")),
                    ),
                    Reason(code=ReasonCode.MostRecent),
                },
            )
        },
        keep_backups={
            expected_backup.uuid: KeepBackup[TS](
                item=expected_backup,
                reasons={
                    Reason(
                        code=ReasonCode.Retained | ReasonCode.New,
                        time_span=(arrow.get("2006-01-01"), arrow.get("2007-01-01")),
                    ),
                    Reason(code=ReasonCode.MostRecent),
                },
            )
        },
    )


def test_multiple_snapshots_and_time_spans(mksnap: MkSnap) -> None:
    iter_time_spans, is_time_span_retained = mkretained(
        now="2006-01-02", years=(0,), months=(0,)
    )
    snapshot1 = mksnap(t="2006-01-01", i=1)
    snapshot2 = mksnap(t="2006-01-02", i=2)
    snapshot3 = mksnap(t="2006-01-02", i=3)
    result = resolve(
        snapshots=(snapshot1, snapshot2, snapshot3),
        backups=(),
        iter_time_spans=iter_time_spans,
        is_time_span_retained=is_time_span_retained,
    )

    expected_backup1 = backup_of_snapshot(snapshot1, send_parent=None)
    expected_backup3 = backup_of_snapshot(snapshot3, send_parent=snapshot1)
    assert result == Result[TS](
        keep_snapshots={
            snapshot1.uuid: KeepSnapshot[TS](
                item=snapshot1,
                reasons={
                    Reason(
                        code=ReasonCode.Retained,
                        time_span=(arrow.get("2006-01-01"), arrow.get("2007-01-01")),
                    ),
                    Reason(
                        code=ReasonCode.Retained,
                        time_span=(arrow.get("2006-01-01"), arrow.get("2006-02-01")),
                    ),
                },
            ),
            snapshot3.uuid: KeepSnapshot[TS](
                item=snapshot3, reasons={Reason(code=ReasonCode.MostRecent)}
            ),
        },
        keep_backups={
            expected_backup1.uuid: KeepBackup[TS](
                item=expected_backup1,
                reasons={
                    Reason(
                        code=ReasonCode.Retained | ReasonCode.New,
                        time_span=(arrow.get("2006-01-01"), arrow.get("2007-01-01")),
                    ),
                    Reason(
                        code=ReasonCode.Retained | ReasonCode.New,
                        time_span=(arrow.get("2006-01-01"), arrow.get("2006-02-01")),
                    ),
                },
            ),
            expected_backup3.uuid: KeepBackup[TS](
                item=expected_backup3, reasons={Reason(code=ReasonCode.MostRecent)}
            ),
        },
    )


def test_keep_send_ancestor_on_year_change(mksnap: MkSnap) -> None:
    iter_time_spans, is_time_span_retained = mkretained(
        now="2007-01-01", years=(0,), months=(-1, 0)
    )
    snapshot1 = mksnap(t="2006-01-01", i=1)
    snapshot2 = mksnap(t="2006-12-01", i=2)
    snapshot3 = mksnap(t="2007-01-01", i=3)
    backup1 = backup_of_snapshot(snapshot1, send_parent=None)
    backup2 = backup_of_snapshot(snapshot2, send_parent=snapshot1)
    result = resolve(
        snapshots=(snapshot1, snapshot2, snapshot3),
        backups=(backup1, backup2),
        iter_time_spans=iter_time_spans,
        is_time_span_retained=is_time_span_retained,
    )

    expected_backup3 = backup_of_snapshot(snapshot3, send_parent=None)
    assert result == Result[TS](
        keep_snapshots={
            snapshot2.uuid: KeepSnapshot[TS](
                item=snapshot2,
                reasons={
                    Reason(
                        code=ReasonCode.Retained,
                        time_span=(arrow.get("2006-12-01"), arrow.get("2007-01-01")),
                    )
                },
            ),
            snapshot3.uuid: KeepSnapshot[TS](
                item=snapshot3,
                reasons={
                    Reason(
                        code=ReasonCode.Retained,
                        time_span=(arrow.get("2007-01-01"), arrow.get("2008-01-01")),
                    ),
                    Reason(
                        code=ReasonCode.Retained,
                        time_span=(arrow.get("2007-01-01"), arrow.get("2007-02-01")),
                    ),
                    Reason(code=ReasonCode.MostRecent),
                },
            ),
        },
        keep_backups={
            backup1.uuid: KeepBackup[TS](
                item=backup1,
                reasons={Reason(code=ReasonCode.SendAncestor, other=backup2.uuid)},
            ),
            backup2.uuid: KeepBackup[TS](
                item=backup2,
                reasons={
                    Reason(
                        code=ReasonCode.Retained,
                        time_span=(arrow.get("2006-12-01"), arrow.get("2007-01-01")),
                    )
                },
            ),
            expected_backup3.uuid: KeepBackup[TS](
                item=expected_backup3,
                reasons={
                    Reason(
                        code=ReasonCode.Retained | ReasonCode.New,
                        time_span=(arrow.get("2007-01-01"), arrow.get("2008-01-01")),
                    ),
                    Reason(
                        code=ReasonCode.Retained | ReasonCode.New,
                        time_span=(arrow.get("2007-01-01"), arrow.get("2007-02-01")),
                    ),
                    Reason(code=ReasonCode.MostRecent),
                },
            ),
        },
    )


def test_backup_chain_broken(mksnap: MkSnap) -> None:
    iter_time_spans, is_time_span_retained = mkretained(now="2006-01-01", years=(0,))
    snapshot1 = mksnap(t="2005-01-01", i=1)
    snapshot2 = mksnap(t="2006-01-01", i=2)
    backup2 = backup_of_snapshot(snapshot2, send_parent=snapshot1)

    with pytest.warns(UserWarning, match="Backup chain is broken"):
        result = resolve(
            snapshots=(),
            backups=(backup2,),
            iter_time_spans=iter_time_spans,
            is_time_span_retained=is_time_span_retained,
        )

    assert result == Result[TS](
        keep_snapshots={},
        keep_backups={
            backup2.uuid: KeepBackup[TS](
                item=backup2,
                reasons={
                    Reason(
                        code=ReasonCode.Retained | ReasonCode.NoSnapshot,
                        time_span=(arrow.get("2006-01-01"), arrow.get("2007-01-01")),
                    )
                },
            )
        },
    )
