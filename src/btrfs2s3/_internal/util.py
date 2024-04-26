from __future__ import annotations

from enum import IntFlag

from btrfsutil import SubvolumeInfo

from btrfs2s3.backups import BackupInfo

NULL_UUID = b"\0" * 16


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


class SubvolumeFlags(IntFlag):
    ReadOnly = 1
    Proposed = 2**30
