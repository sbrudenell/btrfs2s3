from __future__ import annotations

import functools
import random
from typing import Protocol
from typing import TYPE_CHECKING

import arrow
from btrfs2s3._internal.util import backup_of_snapshot
from btrfs2s3._internal.util import mksubvol
from btrfs2s3.preservation import Policy
from btrfs2s3.resolver import _Resolver
from btrfs2s3.resolver import Flags
from btrfs2s3.resolver import KeepBackup
from btrfs2s3.resolver import KeepMeta
from btrfs2s3.resolver import KeepSnapshot
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
    resolver = _Resolver(snapshots=(), backups=(), policy=Policy())

    resolver.keep_most_recent_snapshot()

    assert resolver.get_result() == Result(keep_snapshots={}, keep_backups={})


def test_one_snapshot(mksnap: MkSnap) -> None:
    snapshot = mksnap()
    resolver = _Resolver(snapshots=(snapshot,), backups=(), policy=Policy())

    resolver.keep_most_recent_snapshot()

    expected_backup = backup_of_snapshot(snapshot, send_parent=None)
    assert resolver.get_result() == Result(
        keep_snapshots={
            snapshot.uuid: KeepSnapshot(snapshot, KeepMeta(reasons=Reasons.MostRecent))
        },
        keep_backups={
            expected_backup.uuid: KeepBackup(
                expected_backup, KeepMeta(reasons=Reasons.MostRecent, flags=Flags.New)
            )
        },
    )


def test_multiple_snapshots_keep_most_recent(mksnap: MkSnap) -> None:
    snapshot1 = mksnap(i=1)
    snapshot2 = mksnap(i=2)
    backup1 = backup_of_snapshot(snapshot1, send_parent=None)
    resolver = _Resolver(
        snapshots=(snapshot1, snapshot2), backups=(backup1,), policy=Policy.all()
    )

    resolver.keep_most_recent_snapshot()

    expected_backup = backup_of_snapshot(snapshot2, send_parent=snapshot1)
    assert resolver.get_result() == Result(
        keep_snapshots={
            snapshot2.uuid: KeepSnapshot(
                snapshot2, KeepMeta(reasons=Reasons.MostRecent)
            )
        },
        keep_backups={
            expected_backup.uuid: KeepBackup(
                expected_backup, KeepMeta(reasons=Reasons.MostRecent, flags=Flags.New)
            )
        },
    )
