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

"""Functions for interacting with S3."""

from __future__ import annotations

from typing import TYPE_CHECKING

from btrfs2s3._internal.backups import BackupInfo

if TYPE_CHECKING:
    from collections.abc import Iterator

    from types_boto3_s3.client import S3Client
    from types_boto3_s3.type_defs import ListObjectsV2RequestTypeDef
    from types_boto3_s3.type_defs import ObjectTypeDef


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
        kwargs: ListObjectsV2RequestTypeDef = {"Bucket": bucket}
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
