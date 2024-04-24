from __future__ import annotations

import functools
import random
from typing import Protocol
from typing import TYPE_CHECKING

import arrow
from btrfs2s3._internal.util import backup_of_snapshot
from btrfs2s3._internal.util import iter_all_time_spans
from btrfs2s3._internal.util import mksubvol
from btrfs2s3.resolver import _Resolver
from btrfs2s3.resolver import KeepBackup
from btrfs2s3.resolver import Reason
from btrfs2s3.resolver import ReasonCode
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


def test_simple_backup_of_snapshot(mksnap: MkSnap) -> None:
    snapshot = mksnap()
    resolver = _Resolver(
        snapshots=(snapshot,), backups=(), iter_time_spans=iter_all_time_spans
    )

    with resolver._with_code(ReasonCode.Retained):
        got = resolver._keep_backup_of_snapshot(snapshot)

    expected = backup_of_snapshot(snapshot, send_parent=None)
    assert got == expected

    assert resolver.get_result().keep_backups == {
        got.uuid: KeepBackup(
            item=got, reasons={Reason(code=ReasonCode.Retained | ReasonCode.New)}
        )
    }


def test_backup_done_twice(mksnap: MkSnap) -> None:
    snapshot = mksnap()
    resolver = _Resolver(
        snapshots=(snapshot,), backups=(), iter_time_spans=iter_all_time_spans
    )

    with resolver._with_code(ReasonCode.Retained):
        got1 = resolver._keep_backup_of_snapshot(snapshot)
    with resolver._with_code(ReasonCode.Retained | ReasonCode.New):
        got2 = resolver._keep_backup_of_snapshot(snapshot)

    assert got1 == got2

    assert resolver.get_result().keep_backups == {
        got1.uuid: KeepBackup(
            item=got1, reasons={Reason(code=ReasonCode.Retained | ReasonCode.New)}
        )
    }


def test_choose_correct_parents(mksnap: MkSnap) -> None:
    snapshot1 = mksnap(t="2006-01-01", i=1)
    snapshot2 = mksnap(t="2006-02-01", i=2)
    snapshot3 = mksnap(t="2006-02-01", i=3)
    snapshot4 = mksnap(t="2006-02-02", i=4)
    resolver = _Resolver(
        snapshots=(snapshot1, snapshot2, snapshot3, snapshot4),
        backups=(),
        iter_time_spans=iter_all_time_spans,
    )

    with resolver._with_code(ReasonCode.Retained):
        backup1 = resolver._keep_backup_of_snapshot(snapshot1)
        backup2 = resolver._keep_backup_of_snapshot(snapshot2)
        backup3 = resolver._keep_backup_of_snapshot(snapshot3)
        backup4 = resolver._keep_backup_of_snapshot(snapshot4)

    expected1 = backup_of_snapshot(snapshot1, send_parent=None)
    assert backup1 == expected1
    expected2 = backup_of_snapshot(snapshot2, send_parent=snapshot1)
    assert backup2 == expected2
    expected3 = backup_of_snapshot(snapshot3, send_parent=snapshot2)
    assert backup3 == expected3
    expected4 = backup_of_snapshot(snapshot4, send_parent=snapshot2)
    assert backup4 == expected4

    assert resolver.get_result().keep_backups == {
        backup1.uuid: KeepBackup(
            item=backup1, reasons={Reason(code=ReasonCode.Retained | ReasonCode.New)}
        ),
        backup2.uuid: KeepBackup(
            item=backup2, reasons={Reason(code=ReasonCode.Retained | ReasonCode.New)}
        ),
        backup3.uuid: KeepBackup(
            item=backup3, reasons={Reason(code=ReasonCode.Retained | ReasonCode.New)}
        ),
        backup4.uuid: KeepBackup(
            item=backup4, reasons={Reason(code=ReasonCode.Retained | ReasonCode.New)}
        ),
    }


def test_existing_backup(mksnap: MkSnap) -> None:
    snapshot1 = mksnap(t="2006-01-01", i=1)
    snapshot2 = mksnap(t="2006-02-01", i=2)
    # full backup, as normal
    backup1 = backup_of_snapshot(snapshot1, send_parent=None)
    # full backup wouldn't normally be done here, but ensure we utilize this
    # existing backup if we need to backup the snapshot
    backup2 = backup_of_snapshot(snapshot2, send_parent=None)
    resolver = _Resolver(
        snapshots=(snapshot1, snapshot2),
        backups=(backup1, backup2),
        iter_time_spans=iter_all_time_spans,
    )

    with resolver._with_code(ReasonCode.Retained):
        got_backup1 = resolver._keep_backup_of_snapshot(snapshot1)
        got_backup2 = resolver._keep_backup_of_snapshot(snapshot2)

    assert got_backup1 == backup1
    assert got_backup2 == backup2

    assert resolver.get_result().keep_backups == {
        backup1.uuid: KeepBackup(
            item=backup1, reasons={Reason(code=ReasonCode.Retained)}
        ),
        backup2.uuid: KeepBackup(
            item=backup2, reasons={Reason(code=ReasonCode.Retained)}
        ),
    }
