from __future__ import annotations

from datetime import timezone

from btrfs2s3._internal.durations import Duration
from btrfs2s3._internal.s3 import CostPerByteAndTime
from btrfs2s3._internal.s3 import Costs
from btrfs2s3._internal.s3 import StorageClassCost

STORAGE_COST_RATE_STANDARD = CostPerByteAndTime(
    cost=0.023, per_bytes=10**9, per_time="month"
)
STORAGE_COST_RATE_DEEP_ARCHIVE = CostPerByteAndTime(
    cost=0.00099, per_bytes=10**9, per_time="month"
)
STORAGE_COST_STANDARD = StorageClassCost(
    name="STANDARD", storage_cost=STORAGE_COST_RATE_STANDARD, min_time=None
)
STORAGE_COST_DEEP_ARCHIVE = StorageClassCost(
    name="DEEP_ARCHIVE",
    storage_cost=STORAGE_COST_RATE_DEEP_ARCHIVE,
    min_time=Duration.parse("P180D"),
)
AWS_COSTS = Costs(
    storage_classes=[STORAGE_COST_STANDARD, STORAGE_COST_DEEP_ARCHIVE],
    tzinfo=timezone.utc,
    storage_time_granularity="hour",
    billing_period="month",
)

AWS_COSTS_YAML = (
    "{storage_classes: ["
    "{name: STANDARD, storage: {cost: 0.023}}, "
    "{name: DEEP_ARCHIVE, storage: {cost: 0.00099}, min_time: P180D}]}"
)
