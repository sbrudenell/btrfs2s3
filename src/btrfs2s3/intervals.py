from __future__ import annotations

from typing import NamedTuple
from typing import Iterable
from typing import TypeVar
from typing import Type
from . import times
import datetime

_C = TypeVar("_C", bound="Interval")

class Interval(NamedTuple):
    start: datetime.datetime
    end: datetime.datetime

    def __str__(self) -> str:
        if self == self.__class__.year(self.start):
            return f"{self.start.isoformat()}/P1Y"
        if self == self.__class__.month(self.start):
            return f"{self.start.isoformat()}/P1M"
        if self == self.__class__.day(self.start):
            return f"{self.start.isoformat()}/P1D"
        if self == self.__class__.hour(self.start):
            return f"{self.start.isoformat()}/PT1H"
        if self == self.__class__.minute(self.start):
            return f"{self.start.isoformat()}/PT1M"
        if self == self.__class__.second(self.start):
            return f"{self.start.isoformat()}/PT1S"
        return f"{self.start.isoformat()}/{self.end.isoformat()}"


    @classmethod
    def year(cls:Type[_C], dt:datetime.datetime, offset:int=0) -> _C:
        # This should be correct both for UTC and naive objects, but not for
        # fixed-timezone objects
        assert dt.tzinfo in (None, datetime.timezone.utc)
        start = datetime.datetime(year=dt.year + offset, month=1, day=1,
                tzinfo=dt.tzinfo)
        end = start.replace(year=start.year+1)
        return cls(start, end)

    @classmethod
    def month(cls:Type[_C], dt:datetime.datetime, offset:int=0) -> _C:
        # This should be correct both for UTC and naive objects, but not for
        # fixed-timezone objects
        assert dt.tzinfo in (None, datetime.timezone.utc)
        start = datetime.datetime(year=dt.year + (dt.month + offset - 1) // 12,
                month=(dt.month + offset - 1) % 12 + 1, day=1, tzinfo=dt.tzinfo)
        end = datetime.datetime(year=dt.year + (dt.month + offset) // 12,
                month=(dt.month + offset) % 12 + 1, day=1, tzinfo=dt.tzinfo)
        return cls(start, end)

    @classmethod
    def day(cls:Type[_C], dt:datetime.datetime, offset:int=0) -> _C:
        # This should be correct both for UTC and naive objects, but not for
        # fixed-timezone objects
        assert dt.tzinfo in (None, datetime.timezone.utc)
        start = datetime.datetime(year=dt.year, month=dt.month, day=dt.day,
                tzinfo=dt.tzinfo) + datetime.timedelta(days=offset)
        end = start + datetime.timedelta(days=1)
        return cls(start, end)

    @classmethod
    def hour(cls:Type[_C], dt:datetime.datetime, offset:int=0) -> _C:
        # Hour manipulation is hard with shifting time zones. It's not clear how
        # hours should be defined when e.g. a time zone changes from UTC+04:00 to
        # UTC+04:30
        assert dt.tzinfo is datetime.timezone.utc
        start = dt.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=offset)
        end = start + datetime.timedelta(hours=1)
        return cls(start, end)

    @classmethod
    def minute(cls:Type[_C], dt:datetime.datetime, offset:int=0) -> _C:
        # POSIX unix time doesn't have leap seconds, and AFAICT no time zone has
        # ever changed the number of seconds in a minute
        start = dt.replace(second=0, microsecond=0) + datetime.timedelta(minutes=offset)
        end = start + datetime.timedelta(minutes=1)
        return cls(start, end)

    @classmethod
    def second(cls:Type[_C], dt:datetime.datetime, offset:int=0) -> _C:
        # As with minutes, no restriction on tzinfo
        start = dt.replace(microsecond=0) + datetime.timedelta(seconds=offset)
        end = start + datetime.timedelta(seconds=1)
        return cls(start, end)

    @classmethod
    def iter_range(cls:Type[_C], dt:datetime.datetime, *, years:Iterable[int]=(),
            months:Iterable[int]=(), days:Interval[int]=(),
            hours:Interval[int]=(), minutes:Interval[int]=(), seconds:Interval[int]=()) -> Iterator[_C]:
        for i in years:
            yield cls.year(dt, offset=i)
        for i in months:
            yield cls.month(dt, offset=i)
        for i in days:
            yield cls.day(dt, offset=i)
        for i in hours:
            yield cls.hour(dt, offset=i)
        for i in minutes:
            yield cls.minute(dt, offset=i)
        for i in seconds:
            yield cls.second(dt, offset=i)
