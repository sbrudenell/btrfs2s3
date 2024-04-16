from __future__ import annotations

from random import randrange
from typing import TYPE_CHECKING
from uuid import uuid4

import arrow
from btrfs2s3.backups import BackupInfo
from btrfs2s3.s3 import iter_backups

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


def test_empty(s3: S3Client, bucket: str) -> None:
    got = list(iter_backups(s3, bucket))
    assert got == []


def test_objects_but_none_are_backups(s3: S3Client, bucket: str) -> None:
    s3.put_object(Bucket=bucket, Key="not-a-bucket-name.gz", Body=b"dummy")
    got = list(iter_backups(s3, bucket))
    assert got == []


def mkinfo() -> BackupInfo:
    return BackupInfo(
        uuid=uuid4().bytes,
        parent_uuid=uuid4().bytes,
        ctransid=randrange(100000),
        ctime=arrow.get().timestamp(),
        send_parent_uuid=uuid4().bytes,
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
