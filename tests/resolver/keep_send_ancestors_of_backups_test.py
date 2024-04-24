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
from btrfs2s3.resolver import Flags
from btrfs2s3.resolver import KeepBackup
from btrfs2s3.resolver import KeepMeta
from btrfs2s3.resolver import Reasons
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
    resolver = _Resolver(snapshots=(), backups=(), iter_time_spans=iter_all_time_spans)

    resolver.keep_send_ancestors_of_backups()

    assert resolver.get_result() == Result(keep_snapshots={}, keep_backups={})


def test_backup_with_no_parent(mksnap: MkSnap) -> None:
    snapshot1 = mksnap()
    backup1 = backup_of_snapshot(snapshot1, send_parent=None)
    resolver = _Resolver(
        snapshots=(), backups=(backup1,), iter_time_spans=iter_all_time_spans
    )
    with resolver._keep_backups.with_reasons(Reasons.Retained):
        resolver._keep_backups.mark(backup1)

    resolver.keep_send_ancestors_of_backups()

    assert resolver.get_result() == Result(
        keep_snapshots={},
        keep_backups={
            backup1.uuid: KeepBackup(backup1, KeepMeta(reasons=Reasons.Retained))
        },
    )


def test_send_ancestors_already_kept(mksnap: MkSnap) -> None:
    snapshot1 = mksnap()
    snapshot2 = mksnap()
    snapshot3 = mksnap()
    backup1 = backup_of_snapshot(snapshot1, send_parent=None)
    backup2 = backup_of_snapshot(snapshot2, send_parent=snapshot1)
    backup3 = backup_of_snapshot(snapshot3, send_parent=snapshot2)
    resolver = _Resolver(
        snapshots=(),
        backups=(backup1, backup2, backup3),
        iter_time_spans=iter_all_time_spans,
    )
    with resolver._keep_backups.with_reasons(Reasons.Retained):
        resolver._keep_backups.mark(backup1)
        resolver._keep_backups.mark(backup2)
        resolver._keep_backups.mark(backup3)

    resolver.keep_send_ancestors_of_backups()

    assert resolver.get_result() == Result(
        keep_snapshots={},
        keep_backups={
            backup1.uuid: KeepBackup(backup1, KeepMeta(reasons=Reasons.Retained)),
            backup2.uuid: KeepBackup(backup2, KeepMeta(reasons=Reasons.Retained)),
            backup3.uuid: KeepBackup(backup3, KeepMeta(reasons=Reasons.Retained)),
        },
    )


def test_send_ancestors_created_but_not_yet_kept(mksnap: MkSnap) -> None:
    snapshot1 = mksnap()
    snapshot2 = mksnap()
    snapshot3 = mksnap()
    backup1 = backup_of_snapshot(snapshot1, send_parent=None)
    backup2 = backup_of_snapshot(snapshot2, send_parent=snapshot1)
    backup3 = backup_of_snapshot(snapshot3, send_parent=snapshot2)
    resolver = _Resolver(
        snapshots=(snapshot1, snapshot2, snapshot3),
        backups=(backup1, backup2, backup3),
        iter_time_spans=iter_all_time_spans,
    )
    with resolver._keep_backups.with_reasons(Reasons.Retained):
        resolver._keep_backups.mark(backup3)

    resolver.keep_send_ancestors_of_backups()

    assert resolver.get_result() == Result(
        keep_snapshots={},
        keep_backups={
            backup1.uuid: KeepBackup(
                backup1,
                KeepMeta(reasons=Reasons.SendAncestor, other_uuids={backup2.uuid}),
            ),
            backup2.uuid: KeepBackup(
                backup2,
                KeepMeta(reasons=Reasons.SendAncestor, other_uuids={backup3.uuid}),
            ),
            backup3.uuid: KeepBackup(backup3, KeepMeta(reasons=Reasons.Retained)),
        },
    )


def test_send_ancestors_not_yet_created(mksnap: MkSnap) -> None:
    snapshot1 = mksnap()
    snapshot2 = mksnap()
    snapshot3 = mksnap()
    backup3 = backup_of_snapshot(snapshot3, send_parent=snapshot2)
    resolver = _Resolver(
        snapshots=(snapshot1, snapshot2, snapshot3),
        backups=(backup3,),
        iter_time_spans=iter_all_time_spans,
    )
    with resolver._keep_backups.with_reasons(Reasons.Retained):
        resolver._keep_backups.mark(backup3)

    resolver.keep_send_ancestors_of_backups()

    expected_backup1 = backup_of_snapshot(snapshot1, send_parent=None)
    expected_backup2 = backup_of_snapshot(snapshot2, send_parent=snapshot1)
    assert resolver.get_result() == Result(
        keep_snapshots={},
        keep_backups={
            expected_backup1.uuid: KeepBackup(
                expected_backup1,
                KeepMeta(
                    reasons=Reasons.SendAncestor,
                    flags=Flags.New,
                    other_uuids={expected_backup2.uuid},
                ),
            ),
            expected_backup2.uuid: KeepBackup(
                expected_backup2,
                KeepMeta(
                    reasons=Reasons.SendAncestor,
                    flags=Flags.New,
                    other_uuids={backup3.uuid},
                ),
            ),
            backup3.uuid: KeepBackup(backup3, KeepMeta(reasons=Reasons.Retained)),
        },
    )


def test_backup_chain_broken(mksnap: MkSnap) -> None:
    snapshot1 = mksnap()
    snapshot2 = mksnap()
    backup2 = backup_of_snapshot(snapshot2, send_parent=snapshot1)
    resolver = _Resolver(
        snapshots=(), backups=(backup2,), iter_time_spans=iter_all_time_spans
    )
    with resolver._keep_backups.with_reasons(Reasons.Retained):
        resolver._keep_backups.mark(backup2)

    with pytest.warns(UserWarning, match="Backup chain is broken"):
        resolver.keep_send_ancestors_of_backups()

    assert resolver.get_result() == Result(
        keep_snapshots={},
        keep_backups={
            backup2.uuid: KeepBackup(backup2, KeepMeta(reasons=Reasons.Retained))
        },
    )
