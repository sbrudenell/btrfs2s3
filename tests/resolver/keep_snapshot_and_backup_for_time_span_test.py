# btrfs2s3 - maintains a tree of differential backups in object storage.
#
# Copyright (C) 2024 Steven Brudenell and other contributors.
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

from uuid import uuid4

import arrow

from btrfs2s3._internal.btrfsioctl import SubvolInfo
from btrfs2s3._internal.preservation import Params
from btrfs2s3._internal.preservation import Policy
from btrfs2s3._internal.resolver import _Resolver
from btrfs2s3._internal.resolver import Flags
from btrfs2s3._internal.resolver import Item
from btrfs2s3._internal.resolver import KeepMeta
from btrfs2s3._internal.resolver import Reasons
from btrfs2s3._internal.resolver import Result
from btrfs2s3._internal.util import backup_of_snapshot


def _u() -> bytes:
    return uuid4().bytes


def test_noop() -> None:
    resolver = _Resolver(
        snapshots=(), backups=(), policy=Policy(), mk_backup=backup_of_snapshot
    )

    resolver.keep_snapshots_and_backups_for_preserved_time_spans()

    assert resolver.get_result() == Result(keep_snapshots={}, keep_backups={})


def _t(t: str) -> float:
    return arrow.get(t).timestamp()


def test_one_snapshot_multiple_time_spans() -> None:
    # One snapshot on Jan 1st
    snapshot = SubvolInfo(uuid=_u(), parent_uuid=_u(), ctime=_t("2006-01-01"))
    resolver = _Resolver(
        snapshots=(snapshot,),
        backups=(),
        policy=Policy(now=_t("2006-01-01"), params=Params(years=1, months=1)),
        mk_backup=backup_of_snapshot,
    )

    resolver.keep_snapshots_and_backups_for_preserved_time_spans()

    expected_backup = backup_of_snapshot(snapshot, send_parent=None)
    assert resolver.get_result() == Result(
        keep_snapshots={
            snapshot.uuid: Item(
                snapshot,
                KeepMeta(
                    reasons=Reasons.Preserved,
                    time_spans={
                        (_t("2006-01-01"), _t("2007-01-01")),
                        (_t("2006-01-01"), _t("2006-02-01")),
                    },
                ),
            )
        },
        keep_backups={
            expected_backup.uuid: Item(
                expected_backup,
                KeepMeta(
                    reasons=Reasons.Preserved,
                    flags=Flags.New,
                    time_spans={
                        (_t("2006-01-01"), _t("2007-01-01")),
                        (_t("2006-01-01"), _t("2006-02-01")),
                    },
                ),
            )
        },
    )


def test_one_snapshot_with_existing_backup() -> None:
    # One snapshot on Jan 1st
    snapshot = SubvolInfo(uuid=_u(), parent_uuid=_u(), ctime=_t("2006-01-01"))
    backup = backup_of_snapshot(snapshot, send_parent=None)
    resolver = _Resolver(
        snapshots=(snapshot,),
        backups=(backup,),
        policy=Policy(now=_t("2006-01-01"), params=Params(years=1)),
        mk_backup=backup_of_snapshot,
    )

    resolver.keep_snapshots_and_backups_for_preserved_time_spans()

    assert resolver.get_result() == Result(
        keep_snapshots={
            snapshot.uuid: Item(
                snapshot,
                KeepMeta(
                    reasons=Reasons.Preserved,
                    time_spans={(_t("2006-01-01"), _t("2007-01-01"))},
                ),
            )
        },
        keep_backups={
            backup.uuid: Item(
                backup,
                KeepMeta(
                    reasons=Reasons.Preserved,
                    time_spans={(_t("2006-01-01"), _t("2007-01-01"))},
                ),
            )
        },
    )


def test_one_existing_backup_and_no_snapshot() -> None:
    # One snapshot on Jan 1st
    snapshot = SubvolInfo(uuid=_u(), parent_uuid=_u(), ctime=_t("2006-01-01"))
    backup = backup_of_snapshot(snapshot, send_parent=None)
    # Don't include the snapshot
    resolver = _Resolver(
        snapshots=(),
        backups=(backup,),
        policy=Policy(now=_t("2006-01-01"), params=Params(years=1)),
        mk_backup=backup_of_snapshot,
    )

    resolver.keep_snapshots_and_backups_for_preserved_time_spans()

    assert resolver.get_result() == Result(
        keep_snapshots={},
        keep_backups={
            backup.uuid: Item(
                backup,
                KeepMeta(
                    reasons=Reasons.Preserved,
                    flags=Flags.NoSnapshot,
                    time_spans={(_t("2006-01-01"), _t("2007-01-01"))},
                ),
            )
        },
    )


def test_one_existing_backup_and_newer_snapshot() -> None:
    # Two snapshots on Jan 1st, one newer by transid
    snapshot1 = SubvolInfo(
        uuid=_u(), parent_uuid=_u(), ctime=_t("2006-01-01"), ctransid=1
    )
    snapshot2 = SubvolInfo(
        uuid=_u(), parent_uuid=_u(), ctime=_t("2006-01-01"), ctransid=2
    )
    # One backup of the earlier snapshot
    backup1 = backup_of_snapshot(snapshot1, send_parent=None)
    # Don't include the older snapshot
    resolver = _Resolver(
        snapshots=(snapshot2,),
        backups=(backup1,),
        policy=Policy(now=_t("2006-01-01"), params=Params(years=1)),
        mk_backup=backup_of_snapshot,
    )

    resolver.keep_snapshots_and_backups_for_preserved_time_spans()

    # Note that keep_most_recent_snapshot() would add a backup of the newer
    # snapshot
    assert resolver.get_result() == Result(
        keep_snapshots={
            snapshot2.uuid: Item(
                snapshot2,
                KeepMeta(
                    reasons=Reasons.Preserved,
                    time_spans={(_t("2006-01-01"), _t("2007-01-01"))},
                ),
            )
        },
        keep_backups={
            backup1.uuid: Item(
                backup1,
                KeepMeta(
                    reasons=Reasons.Preserved,
                    flags=Flags.SnapshotIsNewer,
                    time_spans={(_t("2006-01-01"), _t("2007-01-01"))},
                ),
            )
        },
    )


def test_one_existing_backup_and_older_snapshot() -> None:
    # Two snapshots on Jan 1st, one newer by transid
    snapshot1 = SubvolInfo(
        uuid=_u(), parent_uuid=_u(), ctime=_t("2006-01-01"), ctransid=1
    )
    snapshot2 = SubvolInfo(
        uuid=_u(), parent_uuid=_u(), ctime=_t("2006-01-01"), ctransid=2
    )
    # One backup of the newer snapshot
    backup2 = backup_of_snapshot(snapshot2, send_parent=None)
    resolver = _Resolver(
        snapshots=(snapshot1, snapshot2),
        backups=(backup2,),
        policy=Policy(now=_t("2006-01-01"), params=Params(years=1)),
        mk_backup=backup_of_snapshot,
    )

    resolver.keep_snapshots_and_backups_for_preserved_time_spans()

    # Note that keep_most_recent_snapshot() would add a backup of the newer
    # snapshot
    expected_backup = backup_of_snapshot(snapshot1, send_parent=None)
    assert resolver.get_result() == Result(
        keep_snapshots={
            snapshot1.uuid: Item(
                snapshot1,
                KeepMeta(
                    reasons=Reasons.Preserved,
                    time_spans={(_t("2006-01-01"), _t("2007-01-01"))},
                ),
            )
        },
        keep_backups={
            expected_backup.uuid: Item(
                expected_backup,
                KeepMeta(
                    reasons=Reasons.Preserved,
                    flags=Flags.New | Flags.ReplacingNewer,
                    time_spans={(_t("2006-01-01"), _t("2007-01-01"))},
                ),
            )
        },
    )
