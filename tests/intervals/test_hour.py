from btrfs2s3.intervals import Interval
import datetime

import pytest

def _i(value:str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value)

@pytest.mark.parametrize("input, offset, start, end", [
    (_i("2006-01-02T15:04:05.999+00:00"), 0, _i("2006-01-02T15:00:00+00:00"), _i("2006-01-02T16:00:00+00:00")),
    (_i("2006-01-02T15:04:05.999+00:00"), -1, _i("2006-01-02T14:00:00+00:00"), _i("2006-01-02T15:00:00+00:00")),
    (_i("2006-01-02T15:04:05.999+00:00"), 1, _i("2006-01-02T16:00:00+00:00"), _i("2006-01-02T17:00:00+00:00")),
    (_i("2006-01-02T15:04:05.999+00:00"), 9, _i("2006-01-03T00:00:00+00:00"), _i("2006-01-03T01:00:00+00:00")),
])
def test_hour_interval(input:datetime.datetime, offset:int,
        start:datetime.datetime, end:datetime.datetime) -> None:
    assert Interval.hour(input, offset=offset) == Interval(start, end)
