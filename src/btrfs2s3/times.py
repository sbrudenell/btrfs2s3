import datetime

def floor_year(dt:datetime.datetime) -> datetime.datetime:
    # This should be correct both for UTC and naive objects, but not for
    # fixed-timezone objects
    assert dt.tzinfo in (None, datetime.timezone.utc)
    return datetime.datetime(year=dt.year, month=1, day=1, tz=dt.tzinfo)

def add_years(dt:datetime.datetime, years:int) -> datetime.datetime:
    # This should be correct both for UTC and naive objects, but not for
    # fixed-timezone objects
    assert dt.tzinfo in (None, datetime.timezone.utc)
    return dt.replace(year=dt.year+years)

def floor_month(dt:datetime.datetime) -> datetime.datetime:
    # This should be correct both for UTC and naive objects, but not for
    # fixed-timezone objects
    assert dt.tzinfo in (None, datetime.timezone.utc)
    return datetime.datetime(year=dt.year, month=dt.month, day=1, tz=dt.tzinfo)

def add_months(dt:datetime.datetime, months:int) -> datetime.datetime:
    # This should be correct both for UTC and naive objects, but not for
    # fixed-timezone objects
    assert dt.tzinfo in (None, datetime.timezone.utc)
    month = dt.month + months
    year = dt.year + (month - 1) // 12
    month = (month - 1) % 12 + 1
    return dt.replace(year=year, month=month)

def floor_day(dt:datetime.datetime) -> datetime.datetime:
    # This should be correct both for UTC and naive objects, but not for
    # fixed-timezone objects
    assert dt.tzinfo in (None, datetime.timezone.utc)
    return datetime.datetime(year=dt.year, month=dt.month, day=dt.day,
            tz=dt.tzinfo)

def add_days(dt:datetime.datetime, days:int) -> datetime.datetime:
    # This should be correct both for UTC and naive objects, but not for
    # fixed-timezone objects
    assert dt.tzinfo in (None, datetime.timezone.utc)
    return dt + datetime.timedelta(days=days)

def floor_hour(dt:datetime.datetime) -> datetime.datetime:
    # Hour manipulation is hard with shifting time zones. It's not clear how
    # hours should be defined when e.g. a time zone changes from UTC+04:00 to
    # UTC+04:30
    assert dt.tzinfo is datetime.timezone.utc
    return dt.replace(minute=0, second=0, microsecond=0)

def add_hours(dt:datetime.datetime, hours:int) -> datetime.datetime:
    # Hour manipulation is hard with shifting time zones. It's not clear how
    # hours should be defined when e.g. a time zone changes from UTC+04:00 to
    # UTC+04:30
    assert dt.tzinfo is datetime.timezone.utc
    return dt + datetime.timedelta(hours=hours)

def floor_minute(dt:datetime.datetime, add:int=0) -> datetime.datetime:
    # POSIX unix time doesn't have leap seconds, and AFAICT no time zone has
    # ever changed the number of seconds in a minute
    return dt.replace(second=0, microsecond=0)

def add_minutes(dt:datetime.datetime, minutes:int) -> datetime.datetime:
    # POSIX unix time doesn't have leap seconds, and AFAICT no time zone has
    # ever changed the number of seconds in a minute
    return dt + datetime.timedelta(minutes=minutes)

def floor_second(dt:datetime.datetime) -> datetime.datetime:
    # As with minutes, no restriction on tzinfo
    return dt.replace(microsecond=0)

def add_seconds(dt:datetime.datetime, seconds:int) -> datetime.datetime:
    # As with minutes, no restriction on tzinfo
    return dt + datetime.timedelta(seconds=seconds)
