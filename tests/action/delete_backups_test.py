from __future__ import annotations

from typing import TYPE_CHECKING

from btrfs2s3._internal.action import delete_backups

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


def test_good_delete_one_key(s3: S3Client, bucket: str) -> None:
    key = "test-backup"
    s3.put_object(Bucket=bucket, Key=key, Body=b"dummy")

    delete_backups(s3, bucket, key)

    assert s3.list_objects_v2(Bucket=bucket).get("Contents", []) == []


def test_delete_key_that_does_not_exist(s3: S3Client, bucket: str) -> None:
    key = "test-backup"

    delete_backups(s3, bucket, key)

    assert s3.list_objects_v2(Bucket=bucket).get("Contents", []) == []


def test_some_keys_exist_and_some_do_not(s3: S3Client, bucket: str) -> None:
    key1 = "test-backup1"
    key2 = "test-backup2"
    s3.put_object(Bucket=bucket, Key=key1, Body=b"dummy")

    delete_backups(s3, bucket, key1, key2)

    assert s3.list_objects_v2(Bucket=bucket).get("Contents", []) == []


def test_large_numbers_of_keys(s3: S3Client, bucket: str) -> None:
    keys = [f"test-backup{i}" for i in range(1001)]
    for key in keys:
        s3.put_object(Bucket=bucket, Key=key, Body=b"dummy")

    delete_backups(s3, bucket, *keys)

    assert s3.list_objects_v2(Bucket=bucket).get("Contents", []) == []
