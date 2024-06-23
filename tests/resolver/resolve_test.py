from __future__ import annotations

from typing import Protocol
from typing import TYPE_CHECKING

import arrow
from btrfs2s3._internal.util import backup_of_snapshot
from btrfs2s3._internal.util import mksubvol
from btrfs2s3.preservation import Params
from btrfs2s3.preservation import Policy
from btrfs2s3.resolver import Flags
from btrfs2s3.resolver import KeepBackup
from btrfs2s3.resolver import KeepMeta
from btrfs2s3.resolver import KeepSnapshot
from btrfs2s3.resolver import Reasons
from btrfs2s3.resolver import resolve
from btrfs2s3.resolver import Result
import pytest

if TYPE_CHECKING:
    from btrfsutil import SubvolumeInfo

from uuid import uuid4


@pytest.fixture()
def parent_uuid() -> bytes:
    return uuid4().bytes


class MkSnap(Protocol):
    def __call__(self, *, t: str | None = None, i: int = 0) -> SubvolumeInfo: ...


@pytest.fixture()
def mksnap(parent_uuid: bytes) -> MkSnap:
    def inner(*, t: str | None = None, i: int = 0) -> SubvolumeInfo:
        a = arrow.get() if t is None else arrow.get(t)
        return mksubvol(
            uuid=uuid4().bytes, parent_uuid=parent_uuid, ctime=a.timestamp(), ctransid=i
        )

    return inner


def _t(t: str) -> float:
    return arrow.get(t).timestamp()


def test_noop() -> None:
    result = resolve(snapshots=(), backups=(), policy=Policy())

    assert result == Result(keep_snapshots={}, keep_backups={})


def test_one_snapshot_preserved(mksnap: MkSnap) -> None:
    snapshot = mksnap(t="2006-01-01", i=1)
    result = resolve(
        snapshots=(snapshot,),
        backups=(),
        policy=Policy(now=arrow.get("2006-01-01").timestamp(), params=Params(years=1)),
    )

    expected_backup = backup_of_snapshot(snapshot, send_parent=None)
    assert result == Result(
        keep_snapshots={
            snapshot.uuid: KeepSnapshot(
                snapshot,
                KeepMeta(
                    reasons=Reasons.Preserved | Reasons.MostRecent,
                    time_spans={(_t("2006-01-01"), _t("2007-01-01"))},
                ),
            )
        },
        keep_backups={
            expected_backup.uuid: KeepBackup(
                expected_backup,
                KeepMeta(
                    reasons=Reasons.Preserved | Reasons.MostRecent,
                    flags=Flags.New,
                    time_spans={(_t("2006-01-01"), _t("2007-01-01"))},
                ),
            )
        },
    )


def test_multiple_snapshots_and_time_spans(mksnap: MkSnap) -> None:
    snapshot1 = mksnap(t="2006-01-01", i=1)
    snapshot2 = mksnap(t="2006-01-02", i=2)
    snapshot3 = mksnap(t="2006-01-02", i=3)
    result = resolve(
        snapshots=(snapshot1, snapshot2, snapshot3),
        backups=(),
        policy=Policy(
            now=arrow.get("2006-01-02").timestamp(), params=Params(years=1, months=1)
        ),
    )

    expected_backup1 = backup_of_snapshot(snapshot1, send_parent=None)
    expected_backup3 = backup_of_snapshot(snapshot3, send_parent=snapshot1)
    assert result == Result(
        keep_snapshots={
            snapshot1.uuid: KeepSnapshot(
                snapshot1,
                KeepMeta(
                    reasons=Reasons.Preserved,
                    time_spans={
                        (_t("2006-01-01"), _t("2007-01-01")),
                        (_t("2006-01-01"), _t("2006-02-01")),
                    },
                ),
            ),
            snapshot3.uuid: KeepSnapshot(
                snapshot3, KeepMeta(reasons=Reasons.MostRecent)
            ),
        },
        keep_backups={
            expected_backup1.uuid: KeepBackup(
                expected_backup1,
                KeepMeta(
                    reasons=Reasons.Preserved,
                    flags=Flags.New,
                    time_spans={
                        (_t("2006-01-01"), _t("2007-01-01")),
                        (_t("2006-01-01"), _t("2006-02-01")),
                    },
                ),
            ),
            expected_backup3.uuid: KeepBackup(
                expected_backup3, KeepMeta(reasons=Reasons.MostRecent, flags=Flags.New)
            ),
        },
    )


def test_keep_send_ancestor_on_year_change(mksnap: MkSnap) -> None:
    snapshot1 = mksnap(t="2006-01-01", i=1)
    snapshot2 = mksnap(t="2006-12-01", i=2)
    snapshot3 = mksnap(t="2007-01-01", i=3)
    backup1 = backup_of_snapshot(snapshot1, send_parent=None)
    backup2 = backup_of_snapshot(snapshot2, send_parent=snapshot1)
    result = resolve(
        snapshots=(snapshot1, snapshot2, snapshot3),
        backups=(backup1, backup2),
        policy=Policy(
            now=arrow.get("2007-01-01").timestamp(), params=Params(years=1, months=2)
        ),
    )

    expected_backup3 = backup_of_snapshot(snapshot3, send_parent=None)
    assert result == Result(
        keep_snapshots={
            snapshot2.uuid: KeepSnapshot(
                snapshot2,
                KeepMeta(
                    reasons=Reasons.Preserved,
                    time_spans={(_t("2006-12-01"), _t("2007-01-01"))},
                ),
            ),
            snapshot3.uuid: KeepSnapshot(
                snapshot3,
                KeepMeta(
                    reasons=Reasons.Preserved | Reasons.MostRecent,
                    time_spans={
                        (_t("2007-01-01"), _t("2008-01-01")),
                        (_t("2007-01-01"), _t("2007-02-01")),
                    },
                ),
            ),
        },
        keep_backups={
            backup1.uuid: KeepBackup(
                backup1,
                KeepMeta(reasons=Reasons.SendAncestor, other_uuids={backup2.uuid}),
            ),
            backup2.uuid: KeepBackup(
                backup2,
                KeepMeta(
                    reasons=Reasons.Preserved,
                    time_spans={(_t("2006-12-01"), _t("2007-01-01"))},
                ),
            ),
            expected_backup3.uuid: KeepBackup(
                expected_backup3,
                KeepMeta(
                    reasons=Reasons.Preserved | Reasons.MostRecent,
                    flags=Flags.New,
                    time_spans={
                        (_t("2007-01-01"), _t("2008-01-01")),
                        (_t("2007-01-01"), _t("2007-02-01")),
                    },
                ),
            ),
        },
    )


def test_backup_chain_broken(mksnap: MkSnap) -> None:
    snapshot1 = mksnap(t="2005-01-01", i=1)
    snapshot2 = mksnap(t="2006-01-01", i=2)
    backup2 = backup_of_snapshot(snapshot2, send_parent=snapshot1)

    with pytest.warns(UserWarning, match="Backup chain is broken"):
        result = resolve(
            snapshots=(),
            backups=(backup2,),
            policy=Policy(
                now=arrow.get("2006-01-01").timestamp(), params=Params(years=1)
            ),
        )

    assert result == Result(
        keep_snapshots={},
        keep_backups={
            backup2.uuid: KeepBackup(
                backup2,
                KeepMeta(
                    reasons=Reasons.Preserved,
                    flags=Flags.NoSnapshot,
                    time_spans={(_t("2006-01-01"), _t("2007-01-01"))},
                ),
            )
        },
    )
