from btrfs2s3.intervals import Interval
import datetime
import pytest

def _i(value:str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value)

@pytest.mark.parametrize("input, offset, start, end", [
    (_i("2006-01-02T15:04:05.999"), 0, _i("2006-01-02"), _i("2006-01-03")),
    (_i("2006-01-02T15:04:05.999"), -1, _i("2006-01-01"), _i("2006-01-02")),
    (_i("2006-01-02T15:04:05.999"), 1, _i("2006-01-03"), _i("2006-01-04")),
    (_i("2006-01-02T15:04:05.999"), -2, _i("2005-12-31"), _i("2006-01-01")),
    (_i("2006-01-02T15:04:05.999+00:00"), 0, _i("2006-01-02T00:00:00+00:00"), _i("2006-01-03T00:00:00+00:00")),
    (_i("2006-01-02T15:04:05.999+00:00"), -1, _i("2006-01-01T00:00:00+00:00"), _i("2006-01-02T00:00:00+00:00")),
    (_i("2006-01-02T15:04:05.999+00:00"), 1, _i("2006-01-03T00:00:00+00:00"), _i("2006-01-04T00:00:00+00:00")),
    (_i("2006-01-02T15:04:05.999+00:00"), -2, _i("2005-12-31T00:00:00+00:00"), _i("2006-01-01T00:00:00+00:00")),
])
def test_day_interval(input:datetime.datetime, offset:int,
        start:datetime.datetime, end:datetime.datetime) -> None:
    assert Interval.day(input, offset=offset) == Interval(start, end)
