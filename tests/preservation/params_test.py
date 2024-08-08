from btrfs2s3.preservation import Params
import pytest


def test_args() -> None:
    c = Params(
        years=1, quarters=2, months=3, weeks=4, days=5, hours=6, minutes=7, seconds=8
    )

    assert c.years == 1
    assert c.quarters == 2
    assert c.months == 3
    assert c.weeks == 4
    assert c.days == 5
    assert c.hours == 6
    assert c.minutes == 7
    assert c.seconds == 8


def test_args_invalid() -> None:
    with pytest.raises(ValueError, match="can't be negative"):
        Params(years=-1)


def test_parse() -> None:
    c = Params.parse("1y 4q 12m 52w 365d 8760h 525600M 31536000s")

    assert c.years == 1
    assert c.quarters == 4
    assert c.months == 12
    assert c.weeks == 52
    assert c.days == 365
    assert c.hours == 8760
    assert c.minutes == 525600
    assert c.seconds == 31536000


def test_parse_fail() -> None:
    with pytest.raises(ValueError, match="invalid preservation params"):
        Params.parse("invalid")


def test_parse_fail_wrong_format() -> None:
    with pytest.raises(ValueError, match="invalid preservation params"):
        Params.parse("1m 1y")
