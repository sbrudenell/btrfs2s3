# btrfs2s3 - maintains a tree of differential backups in object storage.
#
# Copyright (C) 2024 Steven Brudenell and other contributors.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

from btrfs2s3._internal.btrfsioctl import SubvolInfo
from btrfs2s3._internal.resolver import _Marker
from btrfs2s3._internal.resolver import Flags
from btrfs2s3._internal.resolver import Item
from btrfs2s3._internal.resolver import KeepMeta
from btrfs2s3._internal.resolver import Reasons


def test_empty() -> None:
    marker: _Marker[SubvolInfo] = _Marker()
    assert marker.get_result() == {}


def test_keep_reason() -> None:
    marker: _Marker[SubvolInfo] = _Marker()
    snapshot = SubvolInfo()
    with marker.with_reasons(Reasons.Preserved):
        marker.mark(snapshot)
    assert marker.get_result() == {
        snapshot.uuid: Item(item=snapshot, meta=KeepMeta(reasons=Reasons.Preserved))
    }


def test_keep_reason_and_time_span() -> None:
    marker: _Marker[SubvolInfo] = _Marker()
    snapshot = SubvolInfo()
    time_span = (0.0, 0.0)
    with marker.with_reasons(Reasons.Preserved), marker.with_time_span(time_span):
        marker.mark(snapshot)
    assert marker.get_result() == {
        snapshot.uuid: Item(
            item=snapshot,
            meta=KeepMeta(reasons=Reasons.Preserved, time_spans={time_span}),
        )
    }


def test_keep_reason_flag_and_time_span() -> None:
    marker: _Marker[SubvolInfo] = _Marker()
    snapshot = SubvolInfo()
    time_span = (0.0, 0.0)
    with marker.with_reasons(Reasons.Preserved), marker.with_time_span(time_span):
        marker.mark(snapshot, flags=Flags.New)
    assert marker.get_result() == {
        snapshot.uuid: Item(
            item=snapshot,
            meta=KeepMeta(
                reasons=Reasons.Preserved, flags=Flags.New, time_spans={time_span}
            ),
        )
    }


def test_keep_for_multiple_reasons() -> None:
    marker: _Marker[SubvolInfo] = _Marker()
    snapshot = SubvolInfo()
    with marker.with_reasons(Reasons.Preserved):
        marker.mark(snapshot)
        with marker.with_reasons(Reasons.MostRecent):
            marker.mark(snapshot)
    assert marker.get_result() == {
        snapshot.uuid: Item(
            item=snapshot, meta=KeepMeta(reasons=Reasons.Preserved | Reasons.MostRecent)
        )
    }


def test_keep_with_reason_context() -> None:
    marker: _Marker[SubvolInfo] = _Marker()
    snapshot = SubvolInfo()
    with marker.with_reasons(Reasons.Preserved):
        marker.mark(snapshot)
    with marker.with_reasons(Reasons.MostRecent):
        marker.mark(snapshot)
    assert marker.get_result() == {
        snapshot.uuid: Item(
            item=snapshot, meta=KeepMeta(reasons=Reasons.Preserved | Reasons.MostRecent)
        )
    }


def test_keep_with_reason_and_flags() -> None:
    marker: _Marker[SubvolInfo] = _Marker()
    snapshot = SubvolInfo()
    with marker.with_reasons(Reasons.Preserved):
        marker.mark(snapshot)
        marker.mark(snapshot, flags=Flags.ReplacingNewer)
    assert marker.get_result() == {
        snapshot.uuid: Item(
            item=snapshot,
            meta=KeepMeta(reasons=Reasons.Preserved, flags=Flags.ReplacingNewer),
        )
    }


def test_keep_with_time_span_context() -> None:
    marker: _Marker[SubvolInfo] = _Marker()
    snapshot = SubvolInfo()
    time_span = (0.0, 0.0)
    with marker.with_reasons(Reasons.Preserved), marker.with_time_span(time_span):
        marker.mark(snapshot)
    assert marker.get_result() == {
        snapshot.uuid: Item(
            item=snapshot,
            meta=KeepMeta(reasons=Reasons.Preserved, time_spans={time_span}),
        )
    }
