"""Types and functions relating to backups."""

from __future__ import annotations

from typing import NamedTuple


class BackupInfo(NamedTuple):
    """Information about a backup."""

    uuid: bytes
    parent_uuid: bytes
    send_parent_uuid: bytes | None
    ctransid: int
    ctime: float
