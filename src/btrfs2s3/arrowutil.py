from arrow import Arrow
from typing import Iterable

def iter_intersecting_intervals(a:Arrow, *, bounds:str="[)") -> Iterable[tuple[Arrow, Arrow]]:
    return iter_intervals(a, years=(0,), quarters=(0,), months=(0,),
            weeks=(0,), days=(0,), hours=(0,), minutes=(0,), seconds=(0,),
            microseconds=(0,), bounds=bounds)

def iter_intervals(a:Arrow, *, bounds:str="[)", years:Iterable[int]=(),
        quarters:Iterable[int]=(), months:Iterable[int]=(),
        weeks:Iterable[int]=(), days:Iterable[int]=(), hours:Iterable[int]=(),
        minutes:Iterable[int]=(), seconds:Iterable[int]=(),
        microseconds:Iterable[int]=()) -> Iterable[tuple[Arrow, Arrow]]:
    for y in years:
        yield a.shift(years=y).span("year", bounds=bounds)
    for q in quarters:
        yield a.shift(quarters=q).span("quarter", bounds=bounds)
    for m in months:
        yield a.shift(months=m).span("month", bounds=bounds)
    for w in weeks:
        yield a.shift(weeks=w).span("week", bounds=bounds)
    for d in days:
        yield a.shift(days=d).span("day", bounds=bounds)
    for h in hours:
        yield a.shift(hours=h).span("hour", bounds=bounds)
    for m in minutes:
        yield a.shift(minutes=m).span("minute", bounds=bounds)
    for s in seconds:
        yield a.shift(seconds=s).span("second", bounds=bounds)
    for m in microseconds:
        yield a.shift(microseconds=m).span("microsecond", bounds=bounds)
