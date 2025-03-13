# btrfs2s3 - maintains a tree of differential backups in object storage.
#
# Copyright (C) 2024-2025 Steven Brudenell and other contributors.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

from typing import Protocol
from typing import TypeVar

from btrfs2s3._internal.backups import BackupInfo


class SubvolumeLike(Protocol):
    @property
    def uuid(self) -> bytes: ...
    @property
    def parent_uuid(self) -> bytes | None: ...
    @property
    def ctransid(self) -> int: ...
    @property
    def ctime(self) -> float: ...


_S = TypeVar("_S", bound=SubvolumeLike)


def backup_of_snapshot(snapshot: _S, send_parent: _S | None = None) -> BackupInfo:
    assert snapshot.parent_uuid is not None
    return BackupInfo(
        uuid=snapshot.uuid,
        parent_uuid=snapshot.parent_uuid,
        send_parent_uuid=None if send_parent is None else send_parent.uuid,
        ctransid=snapshot.ctransid,
        ctime=snapshot.ctime,
    )
