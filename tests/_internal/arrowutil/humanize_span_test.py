from __future__ import annotations

from datetime import timezone

import arrow
from btrfs2s3._internal.arrowutil import humanize_span
import pytest


@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [
        ("2006", "2007", "2006/P1Y"),
        ("2006-01", "2006-04", "2006-Q1/P1Q"),
        ("2006-01", "2006-02", "2006-01/P1M"),
        ("2006-01-02", "2006-01-09", "2006-W01/P1W"),
        ("2006-01-02", "2006-01-03", "2006-01-02/P1D"),
        ("2006-01-02 15", "2006-01-02 16", "2006-01-02T15/PT1H"),
        ("2006-01-02 15:04", "2006-01-02 15:05", "2006-01-02T15:04/PT1M"),
        ("2006-01-02 15:04:05", "2006-01-02 15:04:06", "2006-01-02T15:04:05/PT1S"),
        ("2006", "2008", "2006-01-01T00:00:00/2008-01-01T00:00:00"),
    ],
)
def test_humanize_span(start: str, end: str, expected: str) -> None:
    tzinfo = timezone.utc
    span = (arrow.get(start, tzinfo=tzinfo), arrow.get(end, tzinfo=tzinfo))
    assert humanize_span(span, bounds="[]") == expected
