from __future__ import annotations

import functools
import random

from btrfs2s3._internal.util import mksubvol
from btrfs2s3.resolver import _MarkedItem
from btrfs2s3.resolver import _Marker
from btrfs2s3.resolver import Flags
from btrfs2s3.resolver import KeepMeta
from btrfs2s3.resolver import Reasons
from btrfsutil import SubvolumeInfo

mkuuid = functools.partial(random.randbytes, 16)


def test_empty() -> None:
    marker: _Marker[SubvolumeInfo] = _Marker()
    assert marker.get_result() == {}


def test_keep_reason() -> None:
    marker: _Marker[SubvolumeInfo] = _Marker()
    snapshot = mksubvol()
    with marker.with_reasons(Reasons.Retained):
        marker.mark(snapshot)
    assert marker.get_result() == {
        snapshot.uuid: _MarkedItem(
            item=snapshot, meta=KeepMeta(reasons=Reasons.Retained)
        )
    }


def test_keep_reason_and_time_span() -> None:
    marker: _Marker[SubvolumeInfo] = _Marker()
    snapshot = mksubvol()
    time_span = (0.0, 0.0)
    with marker.with_reasons(Reasons.Retained), marker.with_time_span(time_span):
        marker.mark(snapshot)
    assert marker.get_result() == {
        snapshot.uuid: _MarkedItem(
            item=snapshot,
            meta=KeepMeta(reasons=Reasons.Retained, time_spans={time_span}),
        )
    }


def test_keep_reason_flag_and_time_span() -> None:
    marker: _Marker[SubvolumeInfo] = _Marker()
    snapshot = mksubvol()
    time_span = (0.0, 0.0)
    with marker.with_reasons(Reasons.Retained), marker.with_time_span(time_span):
        marker.mark(snapshot, flags=Flags.New)
    assert marker.get_result() == {
        snapshot.uuid: _MarkedItem(
            item=snapshot,
            meta=KeepMeta(
                reasons=Reasons.Retained, flags=Flags.New, time_spans={time_span}
            ),
        )
    }


def test_keep_for_multiple_reasons() -> None:
    marker: _Marker[SubvolumeInfo] = _Marker()
    snapshot = mksubvol()
    with marker.with_reasons(Reasons.Retained):
        marker.mark(snapshot)
        with marker.with_reasons(Reasons.MostRecent):
            marker.mark(snapshot)
    assert marker.get_result() == {
        snapshot.uuid: _MarkedItem(
            item=snapshot, meta=KeepMeta(reasons=Reasons.Retained | Reasons.MostRecent)
        )
    }


def test_keep_with_reason_context() -> None:
    marker: _Marker[SubvolumeInfo] = _Marker()
    snapshot = mksubvol()
    with marker.with_reasons(Reasons.Retained):
        marker.mark(snapshot)
    with marker.with_reasons(Reasons.MostRecent):
        marker.mark(snapshot)
    assert marker.get_result() == {
        snapshot.uuid: _MarkedItem(
            item=snapshot, meta=KeepMeta(reasons=Reasons.Retained | Reasons.MostRecent)
        )
    }


def test_keep_with_reason_and_flags() -> None:
    marker: _Marker[SubvolumeInfo] = _Marker()
    snapshot = mksubvol()
    with marker.with_reasons(Reasons.Retained):
        marker.mark(snapshot)
        marker.mark(snapshot, flags=Flags.ReplacingNewer)
    assert marker.get_result() == {
        snapshot.uuid: _MarkedItem(
            item=snapshot,
            meta=KeepMeta(reasons=Reasons.Retained, flags=Flags.ReplacingNewer),
        )
    }


def test_keep_with_time_span_context() -> None:
    marker: _Marker[SubvolumeInfo] = _Marker()
    snapshot = mksubvol()
    time_span = (0.0, 0.0)
    with marker.with_reasons(Reasons.Retained), marker.with_time_span(time_span):
        marker.mark(snapshot)
    assert marker.get_result() == {
        snapshot.uuid: _MarkedItem[SubvolumeInfo](
            item=snapshot,
            meta=KeepMeta(reasons=Reasons.Retained, time_spans={time_span}),
        )
    }
