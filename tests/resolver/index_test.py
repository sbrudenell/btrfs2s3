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

from btrfs2s3._internal.backups import BackupInfo
from btrfs2s3._internal.btrfsioctl import SubvolInfo
from btrfs2s3._internal.preservation import Policy
from btrfs2s3._internal.resolver import _Index


def _u() -> bytes:
    return uuid4().bytes


def test_get_nominal_none() -> None:
    index = _Index(policy=Policy(), items=())
    got = index.get_nominal((0.0, 0.0))
    assert got is None


def test_get_nominal_snapshot_one() -> None:
    snapshot = SubvolInfo()
    index = _Index(policy=Policy.all(), items=(snapshot,))
    for timespan in Policy.all().iter_time_spans(snapshot.ctime):
        got = index.get_nominal(timespan)
        assert got == snapshot


def test_get_nominal_snapshot_one_of_many() -> None:
    snapshot1 = SubvolInfo(ctransid=1)
    snapshot2 = SubvolInfo(ctransid=2)
    index = _Index(policy=Policy.all(), items=(snapshot1, snapshot2))
    for timespan in Policy.all().iter_time_spans(snapshot1.ctime):
        got = index.get_nominal(timespan)
        assert got == snapshot1


def test_get_nominal_backup_one() -> None:
    backup = BackupInfo(
        uuid=_u(), parent_uuid=_u(), send_parent_uuid=None, ctime=0, ctransid=0
    )
    index = _Index(policy=Policy.all(), items=(backup,))
    for timespan in Policy.all().iter_time_spans(backup.ctime):
        got = index.get_nominal(timespan)
        assert got == backup


def test_get_nominal_backup_one_of_many() -> None:
    backup1 = BackupInfo(
        uuid=_u(), parent_uuid=_u(), send_parent_uuid=None, ctime=0, ctransid=0
    )
    backup2 = BackupInfo(
        uuid=_u(),
        parent_uuid=backup1.parent_uuid,
        send_parent_uuid=None,
        ctime=0,
        ctransid=1,
    )
    index = _Index(policy=Policy.all(), items=(backup1, backup2))
    for timespan in Policy.all().iter_time_spans(backup1.ctime):
        got = index.get_nominal(timespan)
        assert got == backup1


def test_get_none() -> None:
    index = _Index(policy=Policy(), items=())
    got = index.get(_u())
    assert got is None


def test_get_snapshot() -> None:
    snapshot = SubvolInfo(uuid=_u())
    index = _Index(policy=Policy(), items=(snapshot,))
    got = index.get(snapshot.uuid)
    assert got == snapshot


def test_get_backup() -> None:
    backup = BackupInfo(
        uuid=_u(), parent_uuid=_u(), send_parent_uuid=None, ctime=0, ctransid=0
    )
    index = _Index(policy=Policy(), items=(backup,))
    got = index.get(backup.uuid)
    assert got == backup


def test_get_most_recent_none() -> None:
    index = _Index(policy=Policy(), items=())
    got = index.get_most_recent()
    assert got is None


def test_get_most_recent_snapshot() -> None:
    snapshot1 = SubvolInfo(uuid=_u(), ctransid=1)
    snapshot2 = SubvolInfo(uuid=_u(), ctransid=2)
    index = _Index(policy=Policy(), items=(snapshot1, snapshot2))
    got = index.get_most_recent()
    assert got == snapshot2


def test_get_all_time_spans_empty() -> None:
    index = _Index(policy=Policy.all(), items=())
    got = index.get_all_time_spans()
    assert set(got) == set()


def test_get_all_time_spans() -> None:
    snapshot = SubvolInfo(uuid=_u(), ctime=2000000000.0)
    index = _Index(policy=Policy.all(), items=(snapshot,))
    got = index.get_all_time_spans()
    assert got == set(Policy.all().iter_time_spans(snapshot.ctime))
