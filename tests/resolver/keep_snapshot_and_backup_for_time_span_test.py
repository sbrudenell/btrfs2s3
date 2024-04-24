from __future__ import annotations

import functools
import random
from typing import Protocol
from typing import TYPE_CHECKING

import arrow
from btrfs2s3._internal.util import backup_of_snapshot
from btrfs2s3._internal.util import mkretained
from btrfs2s3._internal.util import mksubvol
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
    iter_time_spans, _ = mkretained(0, years=(0,))
    resolver = _Resolver(snapshots=(), backups=(), iter_time_spans=iter_time_spans)

    resolver.keep_snapshots_and_backups_for_retained_time_spans(
        lambda _: True  # pragma: no cover
    )

    assert resolver.get_result() == Result(keep_snapshots={}, keep_backups={})


def _t(t: str) -> float:
    return arrow.get(t).timestamp()


def test_one_snapshot_multiple_time_spans(mksnap: MkSnap) -> None:
    # One snapshot on Jan 1st
    snapshot = mksnap(t="2006-01-01")
    # Retain one yearly and one monthly backup. Current day is Jan 1st.
    iter_time_spans, is_time_span_retained = mkretained(
        "2006-01-01", years=(0,), months=(0,)
    )
    resolver = _Resolver(
        snapshots=(snapshot,), backups=(), iter_time_spans=iter_time_spans
    )

    resolver.keep_snapshots_and_backups_for_retained_time_spans(is_time_span_retained)

    expected_backup = backup_of_snapshot(snapshot, send_parent=None)
    assert resolver.get_result() == Result(
        keep_snapshots={
            snapshot.uuid: KeepSnapshot(
                snapshot,
                KeepMeta(
                    reasons=Reasons.Retained,
                    time_spans={
                        (_t("2006-01-01"), _t("2007-01-01")),
                        (_t("2006-01-01"), _t("2006-02-01")),
                    },
                ),
            )
        },
        keep_backups={
            expected_backup.uuid: KeepBackup(
                expected_backup,
                KeepMeta(
                    reasons=Reasons.Retained,
                    flags=Flags.New,
                    time_spans={
                        (_t("2006-01-01"), _t("2007-01-01")),
                        (_t("2006-01-01"), _t("2006-02-01")),
                    },
                ),
            )
        },
    )


def test_one_snapshot_with_existing_backup(mksnap: MkSnap) -> None:
    # One snapshot on Jan 1st
    snapshot = mksnap(t="2006-01-01")
    backup = backup_of_snapshot(snapshot, send_parent=None)
    # Retain one yearly backup. Current day is Jan 1st.
    iter_time_spans, is_time_span_retained = mkretained("2006-01-01", years=(0,))
    resolver = _Resolver(
        snapshots=(snapshot,), backups=(backup,), iter_time_spans=iter_time_spans
    )

    resolver.keep_snapshots_and_backups_for_retained_time_spans(is_time_span_retained)

    assert resolver.get_result() == Result(
        keep_snapshots={
            snapshot.uuid: KeepSnapshot(
                snapshot,
                KeepMeta(
                    reasons=Reasons.Retained,
                    time_spans={(_t("2006-01-01"), _t("2007-01-01"))},
                ),
            )
        },
        keep_backups={
            backup.uuid: KeepBackup(
                backup,
                KeepMeta(
                    reasons=Reasons.Retained,
                    time_spans={(_t("2006-01-01"), _t("2007-01-01"))},
                ),
            )
        },
    )


def test_one_existing_backup_and_no_snapshot(mksnap: MkSnap) -> None:
    # One snapshot on Jan 1st
    snapshot = mksnap(t="2006-01-01")
    backup = backup_of_snapshot(snapshot, send_parent=None)
    # Retain one yearly backup. Current day is Jan 1st.
    iter_time_spans, is_time_span_retained = mkretained("2006-01-01", years=(0,))
    # Don't include the snapshot
    resolver = _Resolver(
        snapshots=(), backups=(backup,), iter_time_spans=iter_time_spans
    )

    resolver.keep_snapshots_and_backups_for_retained_time_spans(is_time_span_retained)

    assert resolver.get_result() == Result(
        keep_snapshots={},
        keep_backups={
            backup.uuid: KeepBackup(
                backup,
                KeepMeta(
                    reasons=Reasons.Retained,
                    flags=Flags.NoSnapshot,
                    time_spans={(_t("2006-01-01"), _t("2007-01-01"))},
                ),
            )
        },
    )


def test_one_existing_backup_and_newer_snapshot(mksnap: MkSnap) -> None:
    # Two snapshots on Jan 1st, one newer by transid
    snapshot1 = mksnap(t="2006-01-01", i=1)
    snapshot2 = mksnap(t="2006-01-01", i=2)
    # One backup of the earlier snapshot
    backup1 = backup_of_snapshot(snapshot1, send_parent=None)
    # Retain one yearly backup. Current day is Jan 1st.
    iter_time_spans, is_time_span_retained = mkretained("2006-01-01", years=(0,))
    # Don't include the older snapshot
    resolver = _Resolver(
        snapshots=(snapshot2,), backups=(backup1,), iter_time_spans=iter_time_spans
    )

    resolver.keep_snapshots_and_backups_for_retained_time_spans(is_time_span_retained)

    # Note that keep_most_recent_snapshot() would add a backup of the newer
    # snapshot
    assert resolver.get_result() == Result(
        keep_snapshots={
            snapshot2.uuid: KeepSnapshot(
                snapshot2,
                KeepMeta(
                    reasons=Reasons.Retained,
                    time_spans={(_t("2006-01-01"), _t("2007-01-01"))},
                ),
            )
        },
        keep_backups={
            backup1.uuid: KeepBackup(
                backup1,
                KeepMeta(
                    reasons=Reasons.Retained,
                    flags=Flags.SnapshotIsNewer,
                    time_spans={(_t("2006-01-01"), _t("2007-01-01"))},
                ),
            )
        },
    )


def test_one_existing_backup_and_older_snapshot(mksnap: MkSnap) -> None:
    # Two snapshots on Jan 1st, one newer by transid
    snapshot1 = mksnap(t="2006-01-01", i=1)
    snapshot2 = mksnap(t="2006-01-01", i=2)
    # One backup of the newer snapshot
    backup2 = backup_of_snapshot(snapshot2, send_parent=None)
    # Retain one yearly backup. Current day is Jan 1st.
    iter_time_spans, is_time_span_retained = mkretained("2006-01-01", years=(0,))
    resolver = _Resolver(
        snapshots=(snapshot1, snapshot2),
        backups=(backup2,),
        iter_time_spans=iter_time_spans,
    )

    resolver.keep_snapshots_and_backups_for_retained_time_spans(is_time_span_retained)

    # Note that keep_most_recent_snapshot() would add a backup of the newer
    # snapshot
    expected_backup = backup_of_snapshot(snapshot1, send_parent=None)
    assert resolver.get_result() == Result(
        keep_snapshots={
            snapshot1.uuid: KeepSnapshot(
                snapshot1,
                KeepMeta(
                    reasons=Reasons.Retained,
                    time_spans={(_t("2006-01-01"), _t("2007-01-01"))},
                ),
            )
        },
        keep_backups={
            expected_backup.uuid: KeepBackup(
                expected_backup,
                KeepMeta(
                    reasons=Reasons.Retained,
                    flags=Flags.New | Flags.ReplacingNewer,
                    time_spans={(_t("2006-01-01"), _t("2007-01-01"))},
                ),
            )
        },
    )
