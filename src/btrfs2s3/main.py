import contextlib
import pathlib
import os
from . import util
from . import snapshots as snapshots_lib
from . import backups as backups_lib
from . import resolver

def find_snapshots(dir_fd:int) -> Iterator[str, SubvolumeInfo]:
    with btrfsutil.SubvolumeIterator(dir_fd, top=0, info=True) as it:
        for name, info in it:
            # uuid, ctime and ctransid were introduced in 3.10
            if info.uuid == NULL_UUID or info.ctransid == 0 or info.ctime == 0:
                continue
            # not a read-only snapshot
            if not (info.flags & FLAG_READ_ONLY):
                continue
            yield name, info

class Action:
    pass

class CreateSnapshot(Action):
    src_fd: int
    dst_name: str
    dst_dir_fd: int

class RenameSnapshot(Action):
    src_name: str
    src_dir_fd: int
    dst_name: str
    dst_dir_fd:int

class DeleteSnapshot(Action):
    dir_fd: int
    name: str

class UploadBackup(Action):
    src_dir_fd: int
    src_name: str
    dst_name: str
    send_parent_name: str

class DeleteBackup(Action):
    name: str

class Source:
    info: SubvolumeInfo
    fd: int
    name: str

class Snapshot:
    info: SubvolumeInfo
    name: str

class Index:
    @classmethod
    def build(cls, sources:Collection[tuple[int, str]], dir_fd:int) -> Index:
        index = cls(dir_fd=dir_fd)
        for fd, name in sources:
            info = btrfs.subvolume_info(fd)
            index.add_source(info=info, fd=fd, name=name)
        for name, info in find_snapshots(dir_fd):
            info = btrfs.subvolume_info(fd)
            index.add_snapshot(info=info, name=name)
        return index

    def __init__(self, *, dir_fd:int)->None:
        self._source_by_uuid = {}
        self._snapshot_by_source_and_uuid = {}
        self.dir_fd = dir_fd

    def add_source(self, *, info:SubvolumeInfo, fd:int, name:str) -> None:
        self._source_by_uuid[source.info.uuid] = Source(info=info, fd=fd,
                name=name)

    def add_snapshot(self, *, info:SubvolumeInfo, name:str) -> None:
        if info.parent_uuid not in self._snapshot_by_source_and_uuid:
            self._snapshot_by_source_and_uuid[info.parent_uuid] = {}
        self._snapshot_by_source_and_uuid[info.parent_uuid][info.uuid] = Snapshot(
                info=info, name=name)

    def iter_items(self) -> Iterator[tuple[Source, Collection[Snapshot]]]:
        for uuid, source in self._source_by_uuid.items():
            snapshots = self._snapshots_by_source_and_uuid.get(source, {})
            yield source, snapshots.values()


def create_new_snapshots_if_needed(*, index:Index) -> None:
    for source, snapshots in list(index.iter_items()):
        if source.info.ctransid <= max(s.info.ctransid for s in snapshots):
            continue
        tmp_name = f"{source.name}.tmp.{os.getpid()}"
        tmp_path = pathlib.Path("/proc/self/fd") / str(index.dir_fd) / tmp_name
        btrfsutil.create_snapshot(source.fd, tmp_path, read_only=True)
        info = btrfsutil.subvolume_info(tmp_path)
        index.add_snapshot(info=info, name=tmp_name)


def rename_snapshots(*, index:Index) -> None:
    for snapshot in list(index.iter_snapshots()):
        ctime = arrow.get(snapshot.info.ctime, tzinfo="local")
        ts = ctime.isoformat(timespec="seconds")
        source = index.get_source(snapshot.info.parent_uuid)
        name = f"{source.name}.{ts}"
        if snapshot.name != name:
            os.rename(snapshot.name, name, src_dir_fd=index.dir_fd,
                    dst_dir_fd=index.dir_fd)
            index.rename_snapshot(snapshot, name)


def dooo(*, sources:Collection[tuple[int, str]], dir_fd:int) -> Sequence[Action]:
    index = Index.build(sources=sources, dir_fd=dir_fd)

    create_new_snapshots_if_needed(index=index, dir_fd=dir_fd)
    rename_snapshots(index=index, dir_fd=dir_fd)

    backups = list_backups()

    retained_intervals = set(
        iter_interval_slices(
            datetime.datetime.now(datetime.timezone.utc), years=(0,), months=range(0, -3, -1), days=range(0, -30, -1),
            hours=range(0,
                -24, -1), minutes=range(0, -60, -1), seconds=range(0, -60,
                    -1)))

    result = resolver.resolve(source_name=source_name,backups=backups,
            snapshots=snapshots,
            is_interval_retained=retained_intervals.__contains__,
            iter_intervals=iter_intervals_for_ctime)




def create_new_if_needed(source:Source, dir_fd:int, snapshots:Collection[Snapshot]) -> Snapshot | None:

    def create_new(self) -> SubvolInfo:
        tmp_name = f"{self._name}.tmp.{os.getpid()}"
        tmp_path = self._snapshots_path / tmp_name
        # create_snapshot only supports full paths, destination dir fd is
        # disallowed for some reason
        btrfsutil.create_snapshot(self._source_fd, tmp_path, read_only=True)
        snapshot = btrfsutil.subvolume_info(tmp_path)
        snapshot_name = self._get_snapshot_name(snapshot)
        os.rename(tmp_path, snapshot_name, dst_dir_fd=self._snapshots_dir_fd)
        self._name_snapshot_by_uuid[snapshot.uuid] = (snapshot_name, snapshot)
        return snapshot

    def is_up_to_date(self) -> bool:
        return self._source.ctransid <= max(s.ctransid for _, s in
                self._name_snapshot_by_uuid.items())

def run(*, source_path:pathlib.Path, source_name:str, snapshots_path:pathlib.Path) -> None:
    with contextlib.ExitStack() as stack:
        source_fd = stack.enter_context(util.with_fd(source_path, os.O_RDONLY))
        snapshots_dir_fd = stack.enter_context(util.with_fd(snapshots_path,
            os.O_RDONLY))

        snapshots = snapshots_lib.Snapshots.get(source_name=source_name,
                source_fd=source_fd, snapshots_path=snapshots_path,
                snapshots_dir_fd=snapshots_dir_fd)

        if not snapshots.is_up_to_date():
            snapshots.create_new()

        backups = list_backups()

        retained_intervals = set(
            iter_interval_slices(
                datetime.datetime.now(datetime.timezone.utc), years=(0,), months=range(0, -3, -1), days=range(0, -30, -1),
                hours=range(0,
                    -24, -1), minutes=range(0, -60, -1), seconds=range(0, -60,
                        -1)))

        result = resolver.resolve(source_name=source_name,backups=backups,
                snapshots=snapshots,
                is_interval_retained=retained_intervals.__contains__,
                iter_intervals=iter_intervals_for_ctime)

        new_backups = [b for u, b in result.keep_backups.items() if u not in
                backups]
        upload_missing_backups(backups, snapshots)

        delete_expired_backups(backups)

        delete_expired_snapshots(snapshots)
