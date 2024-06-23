"""Functions for interacting with S3."""

from __future__ import annotations

from typing import TYPE_CHECKING

from btrfs2s3.backups import BackupInfo

if TYPE_CHECKING:
    from typing import Iterator

    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_s3.type_defs import ListObjectsV2RequestRequestTypeDef
    from mypy_boto3_s3.type_defs import ObjectTypeDef


def iter_backups(
    client: S3Client, bucket: str
) -> Iterator[tuple[ObjectTypeDef, BackupInfo]]:
    """Find backups in an S3 bucket that were created by btrfs2s3.

    Args:
        client: An S3 client (e.g. created with boto3.client("s3")).
        bucket: The bucket to enumerate.

    Yields:
        Pairs of object info (as returned by ListObjectsV2) and BackupInfo.
    """
    done = False
    continuation_token: str | None = None
    while not done:
        kwargs: ListObjectsV2RequestRequestTypeDef = {"Bucket": bucket}
        if continuation_token is not None:
            kwargs["ContinuationToken"] = continuation_token
        response = client.list_objects_v2(**kwargs)
        for obj in response.get("Contents", []):
            try:
                info = BackupInfo.from_path(obj["Key"])
            except ValueError:
                continue
            yield obj, info
        done = not response["IsTruncated"]
        if not done:
            continuation_token = response["NextContinuationToken"]
