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

from random import randrange
from typing import TYPE_CHECKING
from uuid import uuid4

import arrow

from btrfs2s3._internal.backups import BackupInfo
from btrfs2s3._internal.s3 import iter_backups

if TYPE_CHECKING:
    from types_boto3_s3.client import S3Client


def _u() -> bytes:
    return uuid4().bytes


def test_empty(s3: S3Client, bucket: str) -> None:
    got = list(iter_backups(s3, bucket))
    assert got == []


def test_objects_but_none_are_backups(s3: S3Client, bucket: str) -> None:
    s3.put_object(Bucket=bucket, Key="not-a-bucket-name.gz", Body=b"dummy")
    got = list(iter_backups(s3, bucket))
    assert got == []


def mkinfo() -> BackupInfo:
    return BackupInfo(
        uuid=_u(),
        parent_uuid=_u(),
        ctransid=randrange(100000),
        ctime=arrow.get().timestamp(),
        send_parent_uuid=_u(),
    )


def test_one_backup(s3: S3Client, bucket: str) -> None:
    info = mkinfo()
    key = f"basename{''.join(info.get_path_suffixes())}"
    s3.put_object(Bucket=bucket, Key=key, Body=b"dummy")

    got = list(iter_backups(s3, bucket))

    assert len(got) == 1
    got_object, got_info = got[0]
    assert got_info == info
    assert got_object["Key"] == key


def test_pagination(s3: S3Client, bucket: str) -> None:
    infos = {mkinfo() for _ in range(1001)}
    for info in infos:
        key = f"basename{''.join(info.get_path_suffixes())}"
        s3.put_object(Bucket=bucket, Key=key, Body=b"dummy")

    got = list(iter_backups(s3, bucket))

    got_infos = {info for _, info in got}
    assert got_infos == infos
