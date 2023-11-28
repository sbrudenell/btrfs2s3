from typing import TypeVar
from typing import Iterator
import os
import logging
from typing import Generic
from typing import Hashable
import dataclasses
import pathlib
import uuid

from .intervals import Interval

_LOG = logging.getLogger(__name__)



def iter_intervals_utc(t:float) -> Iterator[Interval]:
    return iter_all_intervals(datetime.datetime.fromtimestamp(has_ctime.ctime,
        tz=datetime.timezone.utc))


def run(source_path:pathlib.Path, snapshots_path:pathlib.Path) -> None:
    with contextlib.ExitStack() as stack:
        source_fd = stack.enter_context(open_context(source_path, os.O_RDONLY))
        snapshots_fd = stack.enter_context(open_context(snapshots_path,
            os.O_RDONLY))

        source = btrfs.subvolume_info(source_fd)
        snapshots = get_snapshots_for_subvolume(snapshots_fd, source)

        if is_new_snapshot_needed(source, snapshots):
            new_snapshot = create_new_snapshot_if_needed(source, snapshots)
            snapshots[new_snapshot.uuid] = new_snapshot

        backups = list_backups()

        retained_intervals = set(
            iter_interval_slices(
                datetime.datetime.now(datetime.timezone.utc), years=(0,), months=range(0, -3, -1), days=range(0, -30, -1),
                hours=range(0,
                    -24, -1), minutes=range(0, -60, -1), seconds=range(0, -60,
                        -1)))

        result = resolve(source_name=pathlib.Path(btrfsutil.subvolume_path(source_fd)).name
                , backups=backups,
                snapshots=snapshots,
                is_interval_retained=retained_intervals.__contains__,
                iter_intervals=iter_intervals_for_ctime)

        new_backups = [b for u, b in result.keep_backups.items() if u not in
                backups]
        upload_missing_backups(backups, snapshots)

        delete_expired_backups(backups)

        delete_expired_snapshots(snapshots)
