import dataclasses
from pathlib import Path

import btrfsutil


@dataclasses.dataclass(frozen=True)
class CreateSnapshot:
    source: Path
    path: Path

    def __call__(self) -> None:
        btrfsutil.create_snapshot(self.source, self.path, read_only=True)
