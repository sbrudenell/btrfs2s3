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

from types import MappingProxyType
from typing import NamedTuple
from typing import TYPE_CHECKING

from arrow import Arrow

from btrfs2s3._internal.arrow_cache import get_arrow
from btrfs2s3._internal.arrow_cache import get_arrow_span
from btrfs2s3._internal.backups import BackupInfo

if TYPE_CHECKING:
    from collections.abc import Collection
    from collections.abc import Iterator
    from collections.abc import Mapping
    from datetime import tzinfo
    from typing import TypeAlias

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


TS: TypeAlias = tuple[float, float]


def apply_granularity(granularity: Duration, timespan: TS, /, *, tzinfo: tzinfo) -> TS:
    assert list(granularity.values()) == [1]
    start, end = get_arrow_span(timespan, tzinfo=tzinfo)
    key = next(iter(granularity))
    start = start.floor(key)
    end_floor = end.floor(key)
    if end != end_floor:
        end = end_floor.shift(**granularity.kwargs())
    return (start.timestamp(), end.timestamp())


def apply_min_duration(duration: Duration, timespan: TS, /, *, tzinfo: tzinfo) -> TS:
    start, end = get_arrow_span(timespan, tzinfo=tzinfo)
    end = max(end, start.shift(**duration.kwargs()))
    return (timespan[0], end.timestamp())


def count_periods(duration: Duration, timespan: TS, /, *, tzinfo: tzinfo) -> float:
    num_periods = 0.0
    assert len(duration) == 1
    start, end = get_arrow_span(timespan, tzinfo=tzinfo)
    for enclosing_start, enclosing_end in Arrow.span_range(
        next(iter(duration)), start.datetime, end.datetime, bounds="[]"
    ):
        enclosing_length = enclosing_end.timestamp() - enclosing_start.timestamp()
        clamped_start = max(enclosing_start, start)
        clamped_end = min(enclosing_end, end)
        clamped_length = clamped_end.timestamp() - clamped_start.timestamp()
        num_periods += clamped_length / enclosing_length

    return num_periods / next(iter(duration.values()))


class BillingPerTime(NamedTuple):
    period: Duration
    tzinfo: tzinfo
    minimum: Duration | None = None
    granularity: Duration | None = None

    def get_billable_timespan(self, timespan: TS, /) -> TS:
        if self.granularity is not None:
            timespan = apply_granularity(self.granularity, timespan, tzinfo=self.tzinfo)
        if self.minimum is not None:
            timespan = apply_min_duration(self.minimum, timespan, tzinfo=self.tzinfo)
        return timespan

    def count_periods(self, timespan: TS, /) -> float:
        return count_periods(self.period, timespan, tzinfo=self.tzinfo)


class CostPerByteAndTime(NamedTuple):
    cost: float
    per_bytes: int
    per_time: BillingPerTime

    def get(self, *, size: int, timespan: TS) -> float:
        timespan = self.per_time.get_billable_timespan(timespan)
        periods = self.per_time.count_periods(timespan)
        return self.cost * (size / self.per_bytes) * periods


class StorageClassCosts(NamedTuple):
    name: str
    storage_cost: CostPerByteAndTime


class Costs:
    def __init__(
        self,
        *,
        storage_classes: Collection[StorageClassCosts],
        tzinfo: tzinfo,
        billing_period: Duration,
    ) -> None:
        # It's not clear what it would mean for tzinfos to be different.
        assert all(tzinfo == c.storage_cost.per_time.tzinfo for c in storage_classes)
        assert list(billing_period.values()) == [1], billing_period
        self._storage_classes = MappingProxyType({c.name: c for c in storage_classes})
        self._tzinfo = tzinfo
        self._billing_period = billing_period

    @property
    def tzinfo(self) -> tzinfo:
        return self._tzinfo

    @property
    def billing_period(self) -> Duration:
        return self._billing_period

    def storage_classes(self) -> Mapping[str, StorageClassCosts]:
        return self._storage_classes

    def get_billing_timespan(self, timestamp: float) -> tuple[float, float]:
        timestamp_a = get_arrow(timestamp, tzinfo=self.tzinfo)
        start_a, end_a = timestamp_a.span(next(iter(self.billing_period)), bounds="[]")
        return (start_a.timestamp(), end_a.timestamp())
