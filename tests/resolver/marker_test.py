from __future__ import annotations

import functools
import random

import arrow
from btrfs2s3._internal.util import mksubvol
from btrfs2s3._internal.util import TS
from btrfs2s3.resolver import _MarkedItem
from btrfs2s3.resolver import _Marker
from btrfs2s3.resolver import Reason
from btrfs2s3.resolver import ReasonCode
from btrfsutil import SubvolumeInfo

mkuuid = functools.partial(random.randbytes, 16)


def test_empty() -> None:
    marker: _Marker[SubvolumeInfo, TS] = _Marker()
    assert marker.get_result() == {}


def test_keep_code() -> None:
    marker: _Marker[SubvolumeInfo, TS] = _Marker()
    snapshot = mksubvol()
    marker.mark(snapshot, code=ReasonCode.Retained)
    assert marker.get_result() == {
        snapshot.uuid: _MarkedItem(
            item=snapshot, reasons={Reason(code=ReasonCode.Retained)}
        )
    }


def test_keep_code_and_time_span() -> None:
    marker: _Marker[SubvolumeInfo, TS] = _Marker()
    snapshot = mksubvol()
    time_span = (arrow.get(0), arrow.get(0))
    marker.mark(snapshot, code=ReasonCode.Retained, time_span=time_span)
    assert marker.get_result() == {
        snapshot.uuid: _MarkedItem(
            item=snapshot,
            reasons={Reason(code=ReasonCode.Retained, time_span=time_span)},
        )
    }


def test_keep_for_multiple_reasons() -> None:
    marker: _Marker[SubvolumeInfo, TS] = _Marker()
    snapshot = mksubvol()
    marker.mark(snapshot, code=ReasonCode.Retained)
    marker.mark(snapshot, code=ReasonCode.Retained | ReasonCode.New)
    assert marker.get_result() == {
        snapshot.uuid: _MarkedItem(
            item=snapshot,
            reasons={
                Reason(code=ReasonCode.Retained),
                Reason(code=ReasonCode.Retained | ReasonCode.New),
            },
        )
    }


def test_keep_with_code_context() -> None:
    marker: _Marker[SubvolumeInfo, TS] = _Marker()
    snapshot = mksubvol()
    with marker.with_code(ReasonCode.Retained):
        marker.mark(snapshot)
        marker.mark(snapshot, code=ReasonCode.New)
    assert marker.get_result() == {
        snapshot.uuid: _MarkedItem(
            item=snapshot,
            reasons={
                Reason(code=ReasonCode.Retained),
                Reason(code=ReasonCode.Retained | ReasonCode.New),
            },
        )
    }


def test_keep_with_time_span_context() -> None:
    marker: _Marker[SubvolumeInfo, TS] = _Marker()
    snapshot = mksubvol()
    marker.mark(snapshot, code=ReasonCode.Retained)
    time_span = (arrow.get(0), arrow.get(0))
    with marker.with_time_span(time_span):
        marker.mark(snapshot, code=ReasonCode.Retained)
    assert marker.get_result() == {
        snapshot.uuid: _MarkedItem[SubvolumeInfo, TS](
            item=snapshot,
            reasons={
                Reason[TS](code=ReasonCode.Retained),
                Reason[TS](code=ReasonCode.Retained, time_span=time_span),
            },
        )
    }
