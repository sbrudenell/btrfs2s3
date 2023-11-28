import dataclasses
from btrfsutil import SubvolumeInfo

@dataclasses.dataclass
class Backup:
    uuid: bytes
    parent_uuid: bytes
    send_parent_uuid: bytes | None
    ctransid: int
    ctime: float

def get() -> dict[bytes, Backup]:
    TODO

def upload(snapshot:SubvolumeInfo, send_parent:SubvolumeInfo) -> None:
    TODO

