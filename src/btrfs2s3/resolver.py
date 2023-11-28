import dataclasses
from btrfsutil import SubvolumeInfo
import collections
from typing import Iterable
from .backups import Backup
import enum
from typing import Callable
from typing import Optional
from typing import Mapping
from typing import FrozenSet
from typing import Tuple
from typing import Dict
from typing import Set
from typing import List
from typing import Collection
from typing import TypeVar
from typing import Hashable
from typing import Generic

_I = TypeVar("_I", bound=Hashable)


class Index(Generic[_I]):
    def __init__(self, *, snapshots:Collection[SubvolumeInfo],
            backups:Collection[Backup],
            iter_intervals:Callable[[float], Iterable[_I]]) -> None:
        self.iter_intervals = iter_intervals
        self._snapshots_by_uuid = {}
        self._snapshots_by_interval = collections.defaultdict(list)
        self._backups_by_uuid = {}
        self._backups_by_interval = collections.defaultdict(list)

        for snapshot in snapshots:
            self._snapshots_by_uuid[snapshot.uuid] = snapshot
            for interval in self.iter_intervals(snapshot.ctime):
                self._snapshots_by_interval[interval].append(snapshot)
        for backup in backups:
            self._backups_by_uuid[backup.uuid] = backup
            for interval in iter_intervals(backup.ctime):
                self._backups_by_interval[interval].append(backup)

    def get_nominal_snapshot(self, interval:_I) -> SubvolumeInfo| None:
        snapshots = self._snapshots_by_interval.get(interval)
        if snapshots:
            return min(snapshots, key=lambda s: s.ctransid)
        return None

    def get_nominal_backup(self, interval:_I) -> Backup | None:
        backups = self._backups_by_interval.get(interval)
        if backups:
            return min(backups, key=lambda b: b.ctransid)
        return None

    def get_snapshot(self, uuid:bytes) -> SubvolumeInfo| None:
        return self._snpashots_by_uuid.get(uuid)

    def get_backup(self, uuid:bytes) -> Backup | None:
        return self._backups_by_uuid.get(uuid)

    def get_most_recent_snapshot(self) -> SubvolumeInfo| None:
        if self._snapshots_by_uuid:
            return min(self._snapshots_by_uuid.values(), key=lambda s: s.ctransid)
        return None

    def get_all_intervals(self) -> Collection[_I]:
        return self._snapshots_by_interval.keys() | self._backups_by_interval.keys()

class ReasonCode(enum.Flag):
    Retained = enum.auto()
    New = enum.auto()
    ReplacingNewer = enum.auto()
    NoSnapshot = enum.auto()
    SnapshotIsNewer = enum.auto()
    MostRecent = enum.auto()
    SendAncestor = enum.auto()

@dataclasses.dataclass(frozen=True)
class Reason(Generic[_I]):
    code: ReasonCode
    interval: Optional[_I] = None
    other: Optional[bytes] = None

@dataclasses.dataclass
class Result(Generic[_I]):
    keep_snapshots: Mapping[bytes, FrozenSet[Reason[_I]]]
    keep_backups: Mapping[bytes, FrozenSet[Reason[_I]]]
    backups: Mapping[bytes, Backup]

class ResultBuilder(Generic[_I]):
    def __init__(self, *, index:Index[_I]) -> None:
        self._index = index
        self._keep_snapshots: dict[bytes, set[Reason[_I]]] = {}
        self._keep_backups: dict[bytes, set[Reason[_I]]] = {}
        self._backups: dict[bytes, Backup] = {}

    def get_result(self) -> Result[_I]:
        return Result(self._keep_snapshots, self._keep_backups,
                self._backups)

    def get_backups(self) -> Mapping[bytes, Backup]:
        return self._backups

    def keep_backup(self, backup:Backup, reason:Reason) -> None:
        if backup.uuid not in self._keep_backups:
            self._keep_backups[backup.uuid] = set()
        self._keep_backups[backup.uuid].add(reason)
        self._backups[backup.uuid] = backup

    def keep_snapshot(self, snapshot:SubvolumeInfo, reason:Reason) -> None:
        if snapshot.uuid not in self._keep_snapshots:
            self._keep_snapshots[snapshot.uuid] = set()
        self._keep_snapshots[snapshot.uuid].add(reason)

    def keep_new_backup(self, snapshot:SubvolumeInfo, reason:Reason) -> Backup:
        backup = self._backups.get(snapshot.uuid)

        # Use an existing backup when available
        backup = self._index.get_backup(snapshot.uuid)
        if backup is None:
            # Determine send-parent for a new backup
            send_parent:SubvolumeInfo | None = None
            for interval in self._index.iter_intervals(snapshot.ctime):
                nominal_snapshot = self._index.get_nominal_snapshot(interval)
                assert nominal_snapshot is not None
                if nominal_snapshot.uuid == snapshot.uuid:
                    break
                send_parent = nominal_snapshot

            backup = Backup(
                uuid=snapshot.uuid,
                parent_uuid=snapshot.parent_uuid,
                send_parent_uuid=None if send_parent is None else send_parent.uuid,
                ctransid=snapshot.ctransid, ctime=snapshot.ctime)

        self.keep_backup(backup, reason)
        return backup

def resolve(*, snapshots:Collection[SubvolumeInfo],
            backups:Collection[Backup],
            iter_intervals:Callable[[float], Iterable[_I]],
            is_interval_retained:Callable[[_I], bool]
            ) -> Tuple[Dict[bytes, Set[Reason]], Dict[bytes, Set[Reason]],
                    List[Backup]]:
    index = Index(snapshots=snapshots, backups=backups,
            iter_intervals=iter_intervals)
    builder = ResultBuilder(index=index)
    for interval in index.get_all_intervals():
        if not is_interval_retained(interval):
            continue
        nominal_snapshot = index.get_nominal_snapshot(interval)
        nominal_backup = index.get_nominal_backup(interval)

        # NB: we could have a nominal backup older than our nominal snapshot.
        # If so, we keep the existing backup, but don't back up the snapshot.
        # There's some duplication in the following logic, but the duplicated
        # cases are expected to be rare and this makes it easier to follow
        if nominal_snapshot:
            builder.keep_snapshot(nominal_snapshot, Reason(ReasonCode.Retained,
                interval=interval))
            if nominal_backup is None:
                builder.keep_new_backup(nominal_snapshot,
                        Reason(ReasonCode.Retained | ReasonCode.New,
                            interval=interval))
            elif nominal_backup.ctransid > nominal_snapshot.ctransid:
                builder.keep_new_backup(nominal_snapshot,
                        Reason(ReasonCode.Retained | ReasonCode.ReplacingNewer,
                            interval=interval))
        if nominal_backup:
            if nominal_snapshot is None:
                builder.keep_backup(nominal_backup, Reason(
                    ReasonCode.Retained | ReasonCode.NoSnapshot,
                    interval=interval))
            elif nominal_backup.ctransid < nominal_snapshot.ctransid:
                builder.keep_backup(nominal_backup, Reason(
                    ReasonCode.Retained | ReasonCode.SnapshotIsNewer,
                    interval=interval))
            elif nominal_backup.ctransid == nominal_snapshot.ctransid:
                builder.keep_backup(nominal_backup, Reason(ReasonCode.Retained,
                    interval=interval))

    # always keep the most recent snapshot
    most_recent_snapshot = index.get_most_recent_snapshot()
    if most_recent_snapshot:
        builder.keep_snapshot(most_recent_snapshot,
                Reason(ReasonCode.MostRecent))
        builder.keep_new_backup(most_recent_snapshot,
                Reason(ReasonCode.MostRecent))

    # Future: is there a case where we need to keep a snapshot because it'll be
    # used as the send-parent of a future backup, but *isn't* otherwise kept?

    # Ensure the send-parent ancestors of any kept backups are also kept
    backups_to_check = list(builder.get_backups().values())
    for backup in backups_to_check:
        if not backup.send_parent_uuid:
            continue
        if backup.send_parent_uuid in builder.get_backups():
            continue
        parent_backup = index.get_backup(backup.send_parent_uuid)
        if parent_backup:
            # This can be common. For example in January, if December's monthly
            # backup should be retained, but the December backup's send-parent
            # is last year's yearly backup
            builder.keep_backup(parent_backup, Reason(ReasonCode.Ancestor,
                other=backup.uuid))
        else:
            parent_snapshot = index.get_snapshot(backup.send_parent_uuid)
            if parent_snapshot:
                parent_backup = builder.keep_new_backup(parent_snapshot, 
                        Reason(ReasonCode.Ancestor | ReasonCode.New,
                            other=backup.uuid))
            else:
                warnings.warn(
                    f"Backup chain is broken: {uuid.UUID(bytes=backup.uuid)} "
                    f"has parent {uuid.UUID(bytes=backup.send_parent_uuid)}, "
                    "which is missing")
        if parent_backup:
            backups_to_check.append(parent_backup)

    return builder.get_result()
