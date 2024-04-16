from __future__ import annotations

import functools
from typing import Callable
from typing import Hashable
from typing import Iterable
from typing import TypeAlias
from typing import TypeVar

import arrow
from arrow import Arrow
from btrfsutil import SubvolumeInfo

from btrfs2s3._internal import arrowutil
from btrfs2s3.backups import BackupInfo

NULL_UUID = b"\0" * 16

_TS = TypeVar("_TS", bound=Hashable)

IterTimeSpans: TypeAlias = Callable[[float], Iterable[_TS]]

IsTimeSpanRetained: TypeAlias = Callable[[_TS], bool]


def mksubvol(
    *,
    id: int = 0,  # noqa: A002
    parent_id: int = 0,
    dir_id: int = 0,
    flags: int = 0,
    uuid: bytes = NULL_UUID,
    parent_uuid: bytes = NULL_UUID,
    received_uuid: bytes = NULL_UUID,
    generation: int = 0,
    ctransid: int = 0,
    otransid: int = 0,
    stransid: int = 0,
    rtransid: int = 0,
    ctime: float = 0.0,
    otime: float = 0.0,
    stime: float = 0.0,
    rtime: float = 0.0,
) -> SubvolumeInfo:
    return SubvolumeInfo(
        (
            id,
            parent_id,
            dir_id,
            flags,
            uuid,
            parent_uuid,
            received_uuid,
            generation,
            ctransid,
            otransid,
            stransid,
            rtransid,
            ctime,
            otime,
        ),
        {"stime": stime, "rtime": rtime},
    )


def backup_of_snapshot(
    snapshot: SubvolumeInfo, send_parent: SubvolumeInfo | None = None
) -> BackupInfo:
    return BackupInfo(
        uuid=snapshot.uuid,
        parent_uuid=snapshot.parent_uuid,
        send_parent_uuid=None if send_parent is None else send_parent.uuid,
        ctransid=snapshot.ctransid,
        ctime=snapshot.ctime,
    )


TS: TypeAlias = tuple[Arrow, Arrow]


def mkretained(
    now: str,
    years: Iterable[int] = (),
    quarters: Iterable[int] = (),
    months: Iterable[int] = (),
    weeks: Iterable[int] = (),
    days: Iterable[int] = (),
    hours: Iterable[int] = (),
    minutes: Iterable[int] = (),
    seconds: Iterable[int] = (),
) -> tuple[IterTimeSpans[TS], IsTimeSpanRetained[TS]]:
    constrained_iter_time_spans = functools.partial(
        arrowutil.iter_time_spans,
        bounds="[]",
        years=(0,) if years else (),
        quarters=(0,) if quarters else (),
        months=(0,) if months else (),
        weeks=(0,) if weeks else (),
        days=(0,) if days else (),
        hours=(0,) if hours else (),
        minutes=(0,) if minutes else (),
        seconds=(0,) if seconds else (),
    )

    def iter_time_spans(timestamp: float) -> Iterable[TS]:
        return constrained_iter_time_spans(arrow.get(timestamp))

    retained_time_spans = list(
        arrowutil.iter_time_spans(
            arrow.get(now),
            bounds="[]",
            years=years,
            quarters=quarters,
            months=months,
            weeks=weeks,
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
        )
    )
    return iter_time_spans, retained_time_spans.__contains__
