# btrfs2s3 - maintains a tree of differential backups in object storage.
#
# Copyright (C) 2024 Steven Brudenell and other contributors.
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

"""Types and functions relating to backups."""

from __future__ import annotations

from contextlib import suppress
import dataclasses
from math import floor
import pathlib
from typing import TYPE_CHECKING
from uuid import UUID

import arrow
from arrow import Arrow
from typing_extensions import Self

from btrfs2s3._internal.cvar import TZINFO

if TYPE_CHECKING:
    from collections.abc import Sequence

_CTIM = "ctim"
_CTID = "ctid"
_UUID = "uuid"
_SNDP = "sndp"
_PRNT = "prnt"
_MDVN = "mdvn"
_SEQN = "seqn"

_METADATA_VERSION = 1


@dataclasses.dataclass(frozen=True)
class BackupInfo:
    """Information about a backup."""

    uuid: bytes
    parent_uuid: bytes
    send_parent_uuid: bytes | None
    ctransid: int
    ctime: float

    def __post_init__(self) -> None:
        """Post-initialization fixups."""
        # We need to use object.__setattr__ to fix up an attribute for a frozen
        # instance. Not sure if there's a better way to do this
        object.__setattr__(self, "ctime", floor(self.ctime))

    def get_path_suffixes(self) -> Sequence[str]:
        """Get path suffixes suitable for naming a backup as an S3 object.

        The intent is to encode a BackupInfo object as an S3 object key. The
        goal of this is to retrieve all backups in a bucket with a single
        ListObjectsV2 call, which only provides keys, not any other metadata
        such as taggings.

        To construct such a key, callers should choose an arbitrary base name,
        and append the suffixes returned by this function, as well as any other
        suffixes as appropriate (such as .gz if the backup is gzipped).

        The order of suffixes is not significant for decoding, but they must
        all be present. The suffixes are returned in an order intended to make
        lists of backups more human-readable (this just means that the ctime is
        formatted as an ISO 8601 timestamp, and appears as the first suffix in
        the return value).

        The prefixes will start with '.'. It's suitable to join them into a
        filename like: f"basename{''.join(suffixes)}.gz".

        As the intent is to construct S3 keys, the return values are "regular"
        unicode strings, not fsdecode()'ed strings.

        Returns:
            A sequence of suffix strings.
        """
        uuid = UUID(bytes=self.uuid)
        parent_uuid = UUID(bytes=self.parent_uuid)
        send_parent_uuid = (
            UUID(bytes=self.send_parent_uuid) if self.send_parent_uuid else UUID(int=0)
        )
        # arrow.get(float, tzinfo=None) raises an error, so we explicitly
        # default to UTC
        tzinfo = TZINFO.get()
        ctime = arrow.get(self.ctime, tzinfo="UTC" if tzinfo is None else tzinfo)
        return [
            f".{_CTIM}{ctime.isoformat(timespec='seconds')}",
            f".{_CTID}{self.ctransid}",
            f".{_UUID}{uuid}",
            f".{_SNDP}{send_parent_uuid}",
            f".{_PRNT}{parent_uuid}",
            f".{_MDVN}{_METADATA_VERSION}",
            f".{_SEQN}0",
        ]

    @classmethod
    def from_path(cls, path: str) -> Self:  # noqa: C901
        """Creates a BackupInfo from a backup filename or path.

        The path is expected to have a base name, and suffixes as returned by
        get_path_suffixes(). Other suffixes may be present, and will be
        ignored.

        The path is expected to be a "regular" unicode string, not a
        fsdecode()'ed string.

        Args:
            path: A filename or path for the backup.

        Returns:
            A BackupInfo object which had been previously encoded into an S3
            key.

        Raises:
            ValueError: If the input isn't a valid encoded BackupInfo.
        """
        uuid: UUID | None = None
        parent_uuid: UUID | None = None
        send_parent_uuid: UUID | None = None
        ctransid: int | None = None
        ctime: Arrow | None = None
        version: int | None = None
        sequence_number: int | None = None

        suffixes = pathlib.PurePath(path).suffixes
        for suffix in suffixes:
            code, rest = suffix[1:5], suffix[5:]
            if code == _PRNT:
                with suppress(ValueError):
                    parent_uuid = UUID(rest)
            elif code == _CTIM:
                with suppress(arrow.ParserError):
                    ctime = arrow.get(rest)
            elif code == _CTID:
                with suppress(ValueError):
                    ctransid = int(rest)
            elif code == _UUID:
                with suppress(ValueError):
                    uuid = UUID(rest)
            elif code == _SNDP:
                with suppress(ValueError):
                    send_parent_uuid = UUID(rest)
            elif code == _MDVN:
                with suppress(ValueError):
                    version = int(rest)
            elif code == _SEQN:
                with suppress(ValueError):
                    sequence_number = int(rest)

        if version is None:
            msg = "backup name metadata version missing (not a backup?)"
            raise ValueError(msg)
        if version != _METADATA_VERSION:
            msg = "unsupported backup name metadata version"
            raise ValueError(msg)
        if sequence_number != 0:
            msg = "unsupported sequence number"
            raise ValueError(msg)
        if (
            uuid is None
            or parent_uuid is None
            or ctransid is None
            or ctime is None
            or send_parent_uuid is None
        ):
            msg = "missing or incomplete parameters for backup name"
            raise ValueError(msg)
        return cls(
            uuid=uuid.bytes,
            parent_uuid=parent_uuid.bytes,
            ctransid=ctransid,
            ctime=ctime.timestamp(),
            send_parent_uuid=send_parent_uuid.bytes if send_parent_uuid.int else None,
        )
