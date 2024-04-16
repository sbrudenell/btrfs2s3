import dataclasses
from pathlib import Path

import btrfsutil

from btrfs2s3._internal.util import NULL_UUID
from btrfs2s3._internal.util import SubvolumeFlags


@dataclasses.dataclass(frozen=True)
class CreateSnapshot:
    source: Path
    path: Path

    def __call__(self) -> None:
        btrfsutil.create_snapshot(self.source, self.path, read_only=True)


@dataclasses.dataclass(frozen=True)
class DeleteSnapshot:
    path: Path

    def __call__(self) -> None:
        # Do some extra checks to make sure we only ever delete read-only
        # snapshots, not source subvolumes.
        if not btrfsutil.is_subvolume(self.path):
            msg = "target isn't a subvolume"
            raise RuntimeError(msg)
        info = btrfsutil.subvolume_info(self.path)
        if info.parent_uuid == NULL_UUID:
            msg = "target isn't a snapshot"
            raise RuntimeError(msg)
        if not info.flags & SubvolumeFlags.ReadOnly:
            msg = "target isn't a read-only snapshot"
            raise RuntimeError(msg)
        btrfsutil.delete_subvolume(self.path)
