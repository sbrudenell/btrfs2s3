from btrfs2s3.intervals import Interval
import datetime

import pytest

def _i(value:str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value)

@pytest.mark.parametrize("input, offset, start, end", [
    (_i("2006-01-02T15:04:05.999"), 0, _i("2006-01-02T15:04:00"), _i("2006-01-02T15:05:00")),
    (_i("2006-01-02T15:04:05.999"), -1, _i("2006-01-02T15:03:00"), _i("2006-01-02T15:04:00")),
    (_i("2006-01-02T15:04:05.999"), 1, _i("2006-01-02T15:05:00"), _i("2006-01-02T15:06:00")),
    (_i("2006-01-02T15:04:05.999+00:00"), 0, _i("2006-01-02T15:04:00+00:00"), _i("2006-01-02T15:05:00+00:00")),
    (_i("2006-01-02T15:04:05.999+00:00"), -1, _i("2006-01-02T15:03:00+00:00"), _i("2006-01-02T15:04:00+00:00")),
    (_i("2006-01-02T15:04:05.999+00:00"), 1, _i("2006-01-02T15:05:00+00:00"), _i("2006-01-02T15:06:00+00:00")),
])
def test_minute_interval(input:datetime.datetime, offset:int,
        start:datetime.datetime, end:datetime.datetime) -> None:
    assert Interval.minute(input, offset=offset) == Interval(start, end)
