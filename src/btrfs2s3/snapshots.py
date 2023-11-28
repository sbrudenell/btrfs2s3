from btrfsutil import SubvolumeInfo
import btrfsutil
import pathlib
import datetime

# btrfsutil offers a large number of ways to refer to a subvolume.

# absolute path
# absolute path -> subvolume id within volume
# absolute path -> subvolume id within volume ("top") -> relative path
# file descriptor
# file descriptor -> relative path
# file descriptor -> subvolume id within volume
# file descriptor -> subvolume id within volume ("top") -> relative path

Ref:TypeAlias = (
    Path | tuple[Path, int] | tuple[Path, int, Path] |
    int | tuple[int, Path] | tuple[int, int] | tuple[int, int, Path])

class Subvol:
    def __init__(self, ref:Ref, info:SubvolumeInfo=None) -> None:
        self._ref = ref
        self._info = info

    def get_ref(self) -> Ref:
        return self._ref

    def get_info(self) -> SubvolumeInfo:
        if self._info is None:
            match self._ref:
                case int(fd):
                    self._info = btrfsutil.subvolume_info(fd)
                case _:
                    raise NotImplementedError
        return self._info

    def get_uuid(self) -> UUID:
        return UUID(bytes=self.get_info().uuid)

    def get_parent_uuid(self) -> UUID:
        return UUID(bytes=self.get_info().parent_uuid)

NULL_UUID = b"\0" * 16
FLAG_READ_ONLY = 1

def create(src:Ref, dst:Ref, read_only=False, recursive=False) -> None:
    match src:
        case int(_) | Path(_):
            match dst:
                case Path(_):
                    btrfsutil.create_snapshot(src, dst, read_only=read_only,
                            recursive=recursive)
                case _:
                    raise NotImplementedError
        case _:
            raise NotImplementedError

def rename(src:Ref, dst:Ref) -> None:
    match src:
        case int(src_fd), Path(src_path):
            match dst:
                case int(dst_fd), Path(dst_path):
                    os.rename(src_path, dst_path, src_dir_fd=src_fd,
                            dst_dir_fd=dst_fd)
                case _:
                    raise NotImplementedError
        case _:
            raise NotImplementedError


@contextlib.contextmanager
def iter_info(ref:Ref) -> Iterator[tuple[btrfsutil.SubvolumeIterator, Iterator[Subvol]]]:
    match ref:
        case int(_):
            pass
        case _:
            raise NotImplementedError
    with btrfsutil.SubvolumeIterator(ref, top=0, info=True) as subvoliter:
        def inner() -> Iterator[Subvol]:
            for path, info in subvoliter:
                yield Subvol((ref, pathlib.Path(path)), info=info)
        yield subvoliter, inner()


_C = TypeVar("_C", bound=Snapshots)

class Snapshots:
    @classmethod
    def get(cls:Type[_C], *, source_fd:int, name:str, snapshots_dir_fd:int,
            snapshots_path:pathlib.Path)->_C:
        source = btrfs.subvolume_info(source_fd)
        # snapshot uuid was introduced in 3.10
        assert source.uuid != NULL_UUID
        snapshots = []
        with btrfsutil.SubvolumeIterator(snapshots_dir_fd, top=0, info=True) as it:
            for snapshot_name, info in it:
                if info.parent_uuid != source.uuid:
                    continue
                if not (info.flags & FLAG_READ_ONLY):
                    continue
                # ctime and ctransid were introduced in 3.10
                assert info.ctransid > 0, info.ctransid
                assert info.ctime > 0, info.ctime
                snapshots.append((snapshot_name, info))
        return cls(source=source, source_fd=source_fd,
                name=name,
                snapshots_dir_fd=snapshots_dir_fd,
                snapshots_path=snapshots_path,
                snapshots=snapshots)

    def __init__(self, *, source:Subvol, name:str,
            snapshots_path:pathlib.Path,
            snapshots:Collection[tuple[str, SubvolumeInfo]]) -> None:
        self._source = source
        self._name = name
        self._source_fd = source_fd
        self._name_snapshot_by_uuid = {info.uuid: (snapshot_name, info) for snapshot_name, info
                in snapshots}

    def _get_snapshot_name(self, snapshot:SubvolumeInfo) -> str:
        assert snapshot.parent_uuid == self._source.uuid
        assert snapshot.ctime > 0, snapshot.ctime
        ts = datetime.datetime.fromtimestamp(snapshot.ctime,
                tz=self._tz)
        return f"{self._name}.{ts.isoformat()}"

    def rename_all(self) -> None:
        for snapshot_name, snapshot in list(self._name_snapshot_by_uuid.values()):
            target_snapshot_name = self._get_snapshot_name(snapshot)
            if snapshot_name != target_snapshot_name:
                os.rename(snapshot_name, target_snapshot_name,
                        src_dir_fd=self._snapshots_dir_fd,
                        dst_dir_fd=self._snapshots_dir_fd)
                self._name_snapshot_by_uuid[snapshot.uuid] = (target_snapshot_name, snapshot)

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
