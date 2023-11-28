from btrfs2s3.intervals import Interval
import datetime

import pytest

def _i(value:str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value)

@pytest.mark.parametrize("start, end, expected", [
    (_i("2006-01-01"), _i("2007-01-01"), ""),
    (_i("2006-01-02T15:04:05.999"), _i("2007-01-02T15:04:05.999"), "2006-01-02T15:04:05.999/2007-01-02T15:04:05.999"),
    (_i("2006-01-02T15:04:05.999"), -1, _i("2005-01-01"), _i("2006-01-01")),
    (_i("2006-01-02T15:04:05.999"), 1, _i("2007-01-01"), _i("2008-01-01")),
    (_i("2006-01-02T15:04:05.999+00:00"), 0, _i("2006-01-01T00:00:00+00:00"), _i("2007-01-01T00:00:00+00:00")),
    (_i("2006-01-02T15:04:05.999+00:00"), -1, _i("2005-01-01T00:00:00+00:00"), _i("2006-01-01T00:00:00+00:00")),
    (_i("2006-01-02T15:04:05.999+00:00"), 1, _i("2007-01-01T00:00:00+00:00"), _i("2008-01-01T00:00:00+00:00")),
])
def test_str(start:datetime.datetime, end:datetime.datetime, expected:str) -> None:
    assert str(Interval(start, end)) == str
