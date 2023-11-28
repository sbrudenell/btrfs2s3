from typing import Iterator
import random
from btrfsutil import SubvolumeInfo
from btrfs2s3.resolver import resolve
from btrfs2s3.resolver import Reason
from btrfs2s3.resolver import ReasonCode
from btrfs2s3.backups import Backup
import arrow
from typing import Iterable
from typing import Callable
from typing import Tuple
from arrow import Arrow
from btrfs2s3 import arrowutil
from btrfs2s3 import subvolutil

TZ = "+07:00"

def iter_intervals(time:float) -> Iterator[Tuple[Arrow, Arrow]]:
    return arrowutil.iter_intersecting_intervals(arrow.get(time, tzinfo=TZ), bounds="[]")

REFTIME = arrow.get("2006-01-02T15:04:05", tzinfo=TZ)

def mkintervals(a: Arrow, years:Iterable[int]=(),
        quarters:Iterable[int]=(), months:Iterable[int]=(),
        weeks:Iterable[int]=(), days:Iterable[int]=(), hours:Iterable[int]=(),
        minutes:Iterable[int]=(), seconds:Iterable[int]=(),
        microseconds:Iterable[int]=()) -> Callable[[Tuple[Arrow, Arrow]], bool]:
    return set(arrowutil.iter_intervals(a, years=years, quarters=quarters,
            months=months, weeks=weeks, days=days, hours=hours,
            minutes=minutes, seconds=seconds, microseconds=microseconds,
            bounds="[]"))

def mkuuid() -> bytes:
    return bytes(random.randrange(256) for _ in range(16))

PARENT_UUID = mkuuid()

def mksnapshot(ctime:Arrow, ctransid:int) -> SubvolumeInfo:
    return subvolutil.mkinfo(uuid=mkuuid(), parent_uuid=PARENT_UUID,
            ctime=ctime.float_timestamp, ctransid=ctransid)

def assert_backup_matches(backup:Backup, snapshot:SubvolumeInfo) -> None:
    assert backup.uuid == snapshot.uuid
    assert backup.parent_uuid == snapshot.parent_uuid
    assert backup.ctransid == snapshot.ctransid
    assert backup.ctime == snapshot.ctime

def test_retain_snapshot() -> None:
    snap = mksnapshot(REFTIME, 0)
    intervals = mkintervals(REFTIME, years=(0,))

    result = resolve(snapshots=[snap], backups=[],
            iter_intervals=iter_intervals,
            is_interval_retained=intervals.__contains__)

    expected_keep_snapshots = {snap.uuid: {Reason(ReasonCode.MostRecent),
        Reason(ReasonCode.Retained, interval=list(intervals)[0])}}
    assert result.keep_snapshots == expected_keep_snapshots

    assert result.keep_backups == {"TODO": 1}

    assert len(result.new_backups) == 1
    backup = result.new_backups[0]
    assert_backup_matches(backup, snap)
    assert backup.send_parent_uuid is None
