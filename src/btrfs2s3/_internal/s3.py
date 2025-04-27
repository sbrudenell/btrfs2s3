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

from typing import NamedTuple
from typing import TYPE_CHECKING

from arrow import Arrow

from btrfs2s3._internal.backups import BackupInfo

if TYPE_CHECKING:
    from collections.abc import Collection
    from collections.abc import Iterator
    from datetime import tzinfo

    from types_boto3_s3.client import S3Client
    from types_boto3_s3.type_defs import ListObjectsV2RequestTypeDef
    from types_boto3_s3.type_defs import ObjectTypeDef

    from btrfs2s3._internal.durations import Duration


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


class Timespan(NamedTuple):
    start: Arrow
    end: Arrow

    def length(self) -> float:
        return self.end.timestamp() - self.start.timestamp()


def apply_granularity(granularity: Duration, timespan: Timespan) -> Timespan:
    assert list(granularity.values()) == [1]
    start, end = timespan
    key = next(iter(granularity))
    end_floor = end.floor(key)
    return Timespan(
        start=start.floor(key),
        end=end if end == end_floor else end_floor.shift(**granularity.kwargs()),
    )


def apply_min_duration(duration: Duration, timespan: Timespan) -> Timespan:
    return Timespan(
        start=timespan.start,
        end=max(timespan.end, timespan.start.shift(**duration.kwargs())),
    )


def count_periods(duration: Duration, timespan: Timespan) -> float:
    num_periods = 0.0
    assert len(duration) == 1
    for enclosing_start, enclosing_end in Arrow.span_range(
        next(iter(duration)),
        timespan.start.datetime,
        timespan.end.datetime,
        bounds="[]",
    ):
        enclosing = Timespan(start=enclosing_start, end=enclosing_end)
        clamped = Timespan(
            start=max(enclosing.start, timespan.start),
            end=min(enclosing.end, timespan.end),
        )
        num_periods += clamped.length() / enclosing.length()

    return num_periods / next(iter(duration.values()))


class CostPerByteAndTime(NamedTuple):
    cost: float
    per_bytes: int
    per_time: Duration

    def get(self, *, size: int, timespan: Timespan) -> float:
        periods = count_periods(self.per_time, timespan)
        return self.cost * (size / self.per_bytes) * periods


class StorageClassCost(NamedTuple):
    name: str
    storage_cost: CostPerByteAndTime
    min_time: Duration | None

    def get_storage_cost(self, *, size: int, timespan: Timespan) -> float:
        if self.min_time is not None:
            timespan = apply_min_duration(self.min_time, timespan)
        return self.storage_cost.get(size=size, timespan=timespan)


class Costs:
    def __init__(
        self,
        *,
        storage_classes: Collection[StorageClassCost],
        tzinfo: tzinfo,
        storage_time_granularity: Duration,
        billing_period: Duration,
    ) -> None:
        self._storage_class = {c.name: c for c in storage_classes}
        self._tzinfo = tzinfo
        self._storage_time_granularity = storage_time_granularity
        self._billing_period = billing_period

    @property
    def tzinfo(self) -> tzinfo:
        return self._tzinfo

    @property
    def storage_time_granularity(self) -> Duration:
        return self._storage_time_granularity

    @property
    def billing_period(self) -> Duration:
        return self._billing_period

    def storage_classes(self) -> Collection[str]:
        return self._storage_class.keys()

    def get_storage_cost(
        self, *, size: int, storage_class: str, timespan: Timespan
    ) -> float | None:
        storage_class_obj = self._storage_class.get(storage_class)
        if not storage_class_obj:
            return None
        timespan = apply_granularity(self.storage_time_granularity, timespan)
        return storage_class_obj.get_storage_cost(size=size, timespan=timespan)
