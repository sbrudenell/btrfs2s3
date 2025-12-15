# btrfs2s3 - maintains a tree of differential backups in object storage.
#
# Copyright (C) 2025 Steven Brudenell and other contributors.
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

from contextlib import contextmanager
from datetime import timezone
from functools import partial
from random import randrange
from typing import Protocol
from typing import TYPE_CHECKING
from uuid import uuid4

import arrow
import pytest

from btrfs2s3._internal.backups import BackupInfo
from btrfs2s3._internal.btrfsioctl import create_subvol
from btrfs2s3._internal.cvar import use_now
from btrfs2s3._internal.durations import Duration
from btrfs2s3._internal.piper import filter_pipe
from btrfs2s3._internal.planner import AssessedBackup
from btrfs2s3._internal.planner import get_best_storage_class
from btrfs2s3._internal.planner import ObjectStat
from btrfs2s3._internal.planner import Remote
from btrfs2s3._internal.planner import Source
from btrfs2s3._internal.resolver import KeepMeta
from btrfs2s3._internal.resolver import Reasons
from btrfs2s3._internal.s3 import Costs
from btrfs2s3._internal.util import backup_of_snapshot
from tests.conftest import AWS_COSTS
from tests.conftest import B2_COSTS

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from types_boto3_s3.client import S3Client


@pytest.fixture
def source_path(btrfs_mountpoint: Path) -> Path:
    path = btrfs_mountpoint / "source"
    create_subvol(path)
    return path


@pytest.fixture
def source(source_path: Path) -> Iterator[Source]:
    with Source.create(source_path) as source:
        yield source


noop_pipe = partial(filter_pipe, [])


class MkBackup(Protocol):
    def __call__(
        self,
        *,
        costs: Costs | None,
        time_span: tuple[str, str] | None,
        stat: ObjectStat | None = None,
    ) -> AssessedBackup: ...


def _u() -> bytes:
    return uuid4().bytes


@pytest.fixture
def mkbackup(source: Source, s3: S3Client, bucket: str) -> MkBackup:
    def inner(
        *,
        costs: Costs | None,
        time_span: tuple[str, str] | None,
        stat: ObjectStat | None = None,
    ) -> AssessedBackup:
        remote = Remote.create(name="test", s3=s3, bucket=bucket, costs=costs)
        info = BackupInfo(
            uuid=_u(),
            parent_uuid=_u(),
            ctransid=randrange(100000),
            ctime=arrow.get().timestamp(),
            send_parent_uuid=_u(),
        )
        meta = KeepMeta(reasons=Reasons.Preserved)
        if time_span is not None:
            start, end = time_span
            meta.time_spans.add(
                (arrow.get(start).timestamp(), arrow.get(end).timestamp())
            )
        return AssessedBackup(
            source=source,
            remote=remote,
            info=info,
            stat=stat,
            key=source.get_backup_key(backup_of_snapshot(info)),
            meta=meta,
            create_pipe=noop_pipe,
        )

    return inner


@contextmanager
def _use_now(value: str) -> Iterator[None]:
    with use_now(arrow.get(value).timestamp()):
        yield


def test_object_already_exists(mkbackup: MkBackup) -> None:
    backup = mkbackup(
        costs=AWS_COSTS, time_span=("2006-01-01", "2007-01-01"), stat=ObjectStat()
    )
    with _use_now("2006-01-01"):
        storage_class = get_best_storage_class(backup)
    assert storage_class is None


def test_empty_costs(mkbackup: MkBackup) -> None:
    backup = mkbackup(costs=None, time_span=("2006-01-01", "2007-01-01"))
    with _use_now("2006-01-01"):
        storage_class = get_best_storage_class(backup)
    assert storage_class is None


def test_costs_with_no_storage_classes(mkbackup: MkBackup) -> None:
    costs = Costs(
        storage_classes=[], billing_period=Duration(months=1), tzinfo=timezone.utc
    )
    backup = mkbackup(costs=costs, time_span=("2006-01-01", "2007-01-01"))
    with _use_now("2006-01-01"):
        storage_class = get_best_storage_class(backup)
    assert storage_class is None


def test_not_preserved(mkbackup: MkBackup) -> None:
    backup = mkbackup(costs=AWS_COSTS, time_span=None)
    storage_class = get_best_storage_class(backup)
    assert storage_class is None


@pytest.mark.parametrize(
    ("costs", "start", "end", "expected_best_storage_class"),
    [
        (AWS_COSTS, "2006-01-01", "2006-01-02", "STANDARD"),
        (AWS_COSTS, "2006-01-01", "2006-01-09", "DEEP_ARCHIVE"),
        (AWS_COSTS, "2006-01-01", "2007-01-01", "DEEP_ARCHIVE"),
        (B2_COSTS, "2006-01-01", "2006-01-02", "STANDARD"),
    ],
)
def test_best_new_storage_class(
    mkbackup: MkBackup,
    costs: Costs,
    start: str,
    end: str,
    expected_best_storage_class: str | None,
) -> None:
    backup = mkbackup(costs=costs, time_span=(start, end))
    with _use_now("2006-01-01"):
        storage_class = get_best_storage_class(backup)
    assert storage_class == expected_best_storage_class
