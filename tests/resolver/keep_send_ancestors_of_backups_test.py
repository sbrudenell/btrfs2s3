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

from typing import Protocol
from typing import TYPE_CHECKING
from uuid import uuid4

import arrow
import pytest

from btrfs2s3._internal.preservation import Policy
from btrfs2s3._internal.resolver import _Resolver
from btrfs2s3._internal.resolver import Flags
from btrfs2s3._internal.resolver import Item
from btrfs2s3._internal.resolver import KeepMeta
from btrfs2s3._internal.resolver import Reasons
from btrfs2s3._internal.resolver import Result
from btrfs2s3._internal.util import backup_of_snapshot
from btrfs2s3._internal.util import mksubvol

if TYPE_CHECKING:
    from btrfsutil import SubvolumeInfo


@pytest.fixture
def parent_uuid() -> bytes:
    return uuid4().bytes


class MkSnap(Protocol):
    def __call__(self, *, t: str | None = None, i: int = 0) -> SubvolumeInfo: ...


@pytest.fixture
def mksnap(parent_uuid: bytes) -> MkSnap:
    def inner(*, t: str | None = None, i: int = 0) -> SubvolumeInfo:
        a = arrow.get() if t is None else arrow.get(t)
        return mksubvol(
            uuid=uuid4().bytes, parent_uuid=parent_uuid, ctime=a.timestamp(), ctransid=i
        )

    return inner


def test_noop() -> None:
    resolver = _Resolver(
        snapshots=(), backups=(), policy=Policy(), mk_backup=backup_of_snapshot
    )

    resolver.keep_send_ancestors_of_backups()

    assert resolver.get_result() == Result(keep_snapshots={}, keep_backups={})


def test_backup_with_no_parent(mksnap: MkSnap) -> None:
    snapshot1 = mksnap()
    backup1 = backup_of_snapshot(snapshot1, send_parent=None)
    resolver = _Resolver(
        snapshots=(), backups=(backup1,), policy=Policy(), mk_backup=backup_of_snapshot
    )
    with resolver._keep_backups.with_reasons(Reasons.Preserved):
        resolver._keep_backups.mark(backup1)

    resolver.keep_send_ancestors_of_backups()

    assert resolver.get_result() == Result(
        keep_snapshots={},
        keep_backups={backup1.uuid: Item(backup1, KeepMeta(reasons=Reasons.Preserved))},
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
        policy=Policy(),
        mk_backup=backup_of_snapshot,
    )
    with resolver._keep_backups.with_reasons(Reasons.Preserved):
        resolver._keep_backups.mark(backup1)
        resolver._keep_backups.mark(backup2)
        resolver._keep_backups.mark(backup3)

    resolver.keep_send_ancestors_of_backups()

    assert resolver.get_result() == Result(
        keep_snapshots={},
        keep_backups={
            backup1.uuid: Item(backup1, KeepMeta(reasons=Reasons.Preserved)),
            backup2.uuid: Item(backup2, KeepMeta(reasons=Reasons.Preserved)),
            backup3.uuid: Item(backup3, KeepMeta(reasons=Reasons.Preserved)),
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
        policy=Policy(),
        mk_backup=backup_of_snapshot,
    )
    with resolver._keep_backups.with_reasons(Reasons.Preserved):
        resolver._keep_backups.mark(backup3)

    resolver.keep_send_ancestors_of_backups()

    assert resolver.get_result() == Result(
        keep_snapshots={},
        keep_backups={
            backup1.uuid: Item(
                backup1,
                KeepMeta(reasons=Reasons.SendAncestor, other_uuids={backup2.uuid}),
            ),
            backup2.uuid: Item(
                backup2,
                KeepMeta(reasons=Reasons.SendAncestor, other_uuids={backup3.uuid}),
            ),
            backup3.uuid: Item(backup3, KeepMeta(reasons=Reasons.Preserved)),
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
        policy=Policy.all(),
        mk_backup=backup_of_snapshot,
    )
    with resolver._keep_backups.with_reasons(Reasons.Preserved):
        resolver._keep_backups.mark(backup3)

    resolver.keep_send_ancestors_of_backups()

    expected_backup1 = backup_of_snapshot(snapshot1, send_parent=None)
    expected_backup2 = backup_of_snapshot(snapshot2, send_parent=snapshot1)
    assert resolver.get_result() == Result(
        keep_snapshots={},
        keep_backups={
            expected_backup1.uuid: Item(
                expected_backup1,
                KeepMeta(
                    reasons=Reasons.SendAncestor,
                    flags=Flags.New,
                    other_uuids={expected_backup2.uuid},
                ),
            ),
            expected_backup2.uuid: Item(
                expected_backup2,
                KeepMeta(
                    reasons=Reasons.SendAncestor,
                    flags=Flags.New,
                    other_uuids={backup3.uuid},
                ),
            ),
            backup3.uuid: Item(backup3, KeepMeta(reasons=Reasons.Preserved)),
        },
    )


def test_backup_chain_broken(mksnap: MkSnap) -> None:
    snapshot1 = mksnap()
    snapshot2 = mksnap()
    backup2 = backup_of_snapshot(snapshot2, send_parent=snapshot1)
    resolver = _Resolver(
        snapshots=(), backups=(backup2,), policy=Policy(), mk_backup=backup_of_snapshot
    )
    with resolver._keep_backups.with_reasons(Reasons.Preserved):
        resolver._keep_backups.mark(backup2)

    with pytest.warns(UserWarning, match="Backup chain is broken"):
        resolver.keep_send_ancestors_of_backups()

    assert resolver.get_result() == Result(
        keep_snapshots={},
        keep_backups={backup2.uuid: Item(backup2, KeepMeta(reasons=Reasons.Preserved))},
    )
