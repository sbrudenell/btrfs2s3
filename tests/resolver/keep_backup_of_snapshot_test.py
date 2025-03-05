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
from btrfs2s3._internal.preservation import Policy
from btrfs2s3._internal.resolver import _Resolver
from btrfs2s3._internal.resolver import Flags
from btrfs2s3._internal.resolver import Item
from btrfs2s3._internal.resolver import KeepMeta
from btrfs2s3._internal.resolver import Reasons
from btrfs2s3._internal.util import backup_of_snapshot


def _u() -> bytes:
    return uuid4().bytes


def _t(t: str) -> float:
    return arrow.get(t).timestamp()


def test_simple_backup_of_snapshot() -> None:
    snapshot = SubvolInfo(parent_uuid=_u())
    resolver = _Resolver(
        snapshots=(snapshot,), backups=(), policy=Policy(), mk_backup=backup_of_snapshot
    )

    with resolver._with_reasons(Reasons.Preserved):
        got = resolver._keep_backup_of_snapshot(snapshot)

    expected = backup_of_snapshot(snapshot, send_parent=None)
    assert got == expected

    assert resolver.get_result().keep_backups == {
        got.uuid: Item(got, KeepMeta(reasons=Reasons.Preserved, flags=Flags.New))
    }


def test_backup_done_twice() -> None:
    snapshot = SubvolInfo(parent_uuid=_u())
    resolver = _Resolver(
        snapshots=(snapshot,), backups=(), policy=Policy(), mk_backup=backup_of_snapshot
    )

    with resolver._with_reasons(Reasons.Preserved):
        got1 = resolver._keep_backup_of_snapshot(snapshot)
    with resolver._with_reasons(Reasons.Preserved):
        got2 = resolver._keep_backup_of_snapshot(snapshot, flags=Flags.New)

    assert got1 == got2

    assert resolver.get_result().keep_backups == {
        got1.uuid: Item(got1, KeepMeta(reasons=Reasons.Preserved, flags=Flags.New))
    }


def test_choose_correct_parents() -> None:
    snapshot1 = SubvolInfo(
        uuid=_u(), parent_uuid=_u(), ctime=_t("2006-01-01"), ctransid=1
    )
    snapshot2 = SubvolInfo(
        uuid=_u(), parent_uuid=_u(), ctime=_t("2006-02-01"), ctransid=2
    )
    snapshot3 = SubvolInfo(
        uuid=_u(), parent_uuid=_u(), ctime=_t("2006-02-01"), ctransid=3
    )
    snapshot4 = SubvolInfo(
        uuid=_u(), parent_uuid=_u(), ctime=_t("2006-02-02"), ctransid=4
    )
    resolver = _Resolver(
        snapshots=(snapshot1, snapshot2, snapshot3, snapshot4),
        backups=(),
        policy=Policy.all(),
        mk_backup=backup_of_snapshot,
    )

    with resolver._with_reasons(Reasons.Preserved):
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
        backup1.uuid: Item(
            backup1, KeepMeta(reasons=Reasons.Preserved, flags=Flags.New)
        ),
        backup2.uuid: Item(
            backup2, KeepMeta(reasons=Reasons.Preserved, flags=Flags.New)
        ),
        backup3.uuid: Item(
            backup3, KeepMeta(reasons=Reasons.Preserved, flags=Flags.New)
        ),
        backup4.uuid: Item(
            backup4, KeepMeta(reasons=Reasons.Preserved, flags=Flags.New)
        ),
    }


def test_existing_backup() -> None:
    snapshot1 = SubvolInfo(
        uuid=_u(), parent_uuid=_u(), ctime=_t("2006-01-01"), ctransid=1
    )
    snapshot2 = SubvolInfo(
        uuid=_u(), parent_uuid=_u(), ctime=_t("2006-02-01"), ctransid=2
    )
    # full backup, as normal
    backup1 = backup_of_snapshot(snapshot1, send_parent=None)
    # full backup wouldn't normally be done here, but ensure we utilize this
    # existing backup if we need to backup the snapshot
    backup2 = backup_of_snapshot(snapshot2, send_parent=None)
    resolver = _Resolver(
        snapshots=(snapshot1, snapshot2),
        backups=(backup1, backup2),
        policy=Policy(),
        mk_backup=backup_of_snapshot,
    )

    with resolver._with_reasons(Reasons.Preserved):
        got_backup1 = resolver._keep_backup_of_snapshot(snapshot1)
        got_backup2 = resolver._keep_backup_of_snapshot(snapshot2)

    assert got_backup1 == backup1
    assert got_backup2 == backup2

    assert resolver.get_result().keep_backups == {
        backup1.uuid: Item(backup1, KeepMeta(reasons=Reasons.Preserved)),
        backup2.uuid: Item(backup2, KeepMeta(reasons=Reasons.Preserved)),
    }
