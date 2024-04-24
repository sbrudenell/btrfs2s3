from __future__ import annotations

import functools
import random

from btrfs2s3._internal.util import iter_all_time_spans
from btrfs2s3._internal.util import mksubvol
from btrfs2s3.backups import BackupInfo
from btrfs2s3.resolver import _Index

mkuuid = functools.partial(random.randbytes, 16)


def test_get_nominal_none() -> None:
    index = _Index(iter_time_spans=iter_all_time_spans, items=())
    got = index.get_nominal((0.0, 0.0))
    assert got is None


def test_get_nominal_snapshot_one() -> None:
    snapshot = mksubvol()
    index = _Index(iter_time_spans=iter_all_time_spans, items=(snapshot,))
    for timespan in iter_all_time_spans(snapshot.ctime):
        got = index.get_nominal(timespan)
        assert got == snapshot


def test_get_nominal_snapshot_one_of_many() -> None:
    snapshot1 = mksubvol(ctransid=1)
    snapshot2 = mksubvol(ctransid=2)
    index = _Index(iter_time_spans=iter_all_time_spans, items=(snapshot1, snapshot2))
    for timespan in iter_all_time_spans(snapshot1.ctime):
        got = index.get_nominal(timespan)
        assert got == snapshot1


def test_get_nominal_backup_one() -> None:
    backup = BackupInfo(
        uuid=mkuuid(), parent_uuid=mkuuid(), send_parent_uuid=None, ctime=0, ctransid=0
    )
    index = _Index(iter_time_spans=iter_all_time_spans, items=(backup,))
    for timespan in iter_all_time_spans(backup.ctime):
        got = index.get_nominal(timespan)
        assert got == backup


def test_get_nominal_backup_one_of_many() -> None:
    backup1 = BackupInfo(
        uuid=mkuuid(), parent_uuid=mkuuid(), send_parent_uuid=None, ctime=0, ctransid=0
    )
    backup2 = BackupInfo(
        uuid=mkuuid(),
        parent_uuid=backup1.parent_uuid,
        send_parent_uuid=None,
        ctime=0,
        ctransid=1,
    )
    index = _Index(iter_time_spans=iter_all_time_spans, items=(backup1, backup2))
    for timespan in iter_all_time_spans(backup1.ctime):
        got = index.get_nominal(timespan)
        assert got == backup1


def test_get_none() -> None:
    index = _Index(iter_time_spans=iter_all_time_spans, items=())
    got = index.get(mkuuid())
    assert got is None


def test_get_snapshot() -> None:
    snapshot = mksubvol(uuid=mkuuid())
    index = _Index(iter_time_spans=iter_all_time_spans, items=(snapshot,))
    got = index.get(snapshot.uuid)
    assert got == snapshot


def test_get_backup() -> None:
    backup = BackupInfo(
        uuid=mkuuid(), parent_uuid=mkuuid(), send_parent_uuid=None, ctime=0, ctransid=0
    )
    index = _Index(iter_time_spans=iter_all_time_spans, items=(backup,))
    got = index.get(backup.uuid)
    assert got == backup


def test_get_most_recent_none() -> None:
    index = _Index(iter_time_spans=iter_all_time_spans, items=())
    got = index.get_most_recent()
    assert got is None


def test_get_most_recent_snapshot() -> None:
    snapshot1 = mksubvol(uuid=mkuuid(), ctransid=1)
    snapshot2 = mksubvol(uuid=mkuuid(), ctransid=2)
    index = _Index(iter_time_spans=iter_all_time_spans, items=(snapshot1, snapshot2))
    got = index.get_most_recent()
    assert got == snapshot2


def test_get_all_time_spans_empty() -> None:
    index = _Index(iter_time_spans=iter_all_time_spans, items=())
    got = index.get_all_time_spans()
    assert set(got) == set()


def test_get_all_time_spans() -> None:
    snapshot = mksubvol(uuid=mkuuid(), ctime=2000000000.0)
    index = _Index(iter_time_spans=iter_all_time_spans, items=(snapshot,))
    got = index.get_all_time_spans()
    assert got == set(iter_all_time_spans(snapshot.ctime))
