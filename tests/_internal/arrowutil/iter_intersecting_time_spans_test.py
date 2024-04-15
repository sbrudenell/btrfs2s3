import arrow
from btrfs2s3._internal.arrowutil import iter_intersecting_time_spans


def test_iter_intersecting_time_spans() -> None:
    """Basic test of iter_intersecting_time_spans."""
    _ = arrow.get

    a = _("2006-01-02T15:04:05+07:00")

    got = list(iter_intersecting_time_spans(a, bounds="[]"))
    expected = [
        (_("2006-01-01T00:00:00+07:00"), _("2007-01-01T00:00:00+07:00")),
        (_("2006-01-01T00:00:00+07:00"), _("2006-04-01T00:00:00+07:00")),
        (_("2006-01-01T00:00:00+07:00"), _("2006-02-01T00:00:00+07:00")),
        (_("2006-01-02T00:00:00+07:00"), _("2006-01-09T00:00:00+07:00")),
        (_("2006-01-02T00:00:00+07:00"), _("2006-01-03T00:00:00+07:00")),
        (_("2006-01-02T15:00:00+07:00"), _("2006-01-02T16:00:00+07:00")),
        (_("2006-01-02T15:04:00+07:00"), _("2006-01-02T15:05:00+07:00")),
        (_("2006-01-02T15:04:05+07:00"), _("2006-01-02T15:04:06+07:00")),
    ]

    assert got == expected
