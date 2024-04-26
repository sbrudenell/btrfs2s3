from btrfs2s3.retention import Params
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
    c = Params.parse("1y 2q 3m 4w 5d 6h 7M 8s")

    assert c.years == 1
    assert c.quarters == 2
    assert c.months == 3
    assert c.weeks == 4
    assert c.days == 5
    assert c.hours == 6
    assert c.minutes == 7
    assert c.seconds == 8


def test_parse_fail() -> None:
    with pytest.raises(ValueError, match="invalid retention params"):
        Params.parse("invalid")
