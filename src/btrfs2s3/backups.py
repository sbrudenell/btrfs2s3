"""Types and functions relating to backups."""

from __future__ import annotations

from contextlib import suppress
import dataclasses
from math import floor
import pathlib
from typing import Sequence
from typing import TYPE_CHECKING
from uuid import UUID

import arrow
from arrow import Arrow
from typing_extensions import Self

if TYPE_CHECKING:
    from datetime import tzinfo


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

    def get_path_suffixes(self, *, tzinfo: tzinfo | str | None = None) -> Sequence[str]:
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

        Args:
            tzinfo: A time zone for formatting the ctime of the backup. The
                ctime will be output as an ISO 8601 string which always
                includes the time zone, so any value may be specified and the
                round-trip decoding won't be affected. For human readability,
                it's best to use the same time zone used for defining our
                preservation schedule.

        Returns:
            A sequence of suffix strings.
        """
        uuid = UUID(bytes=self.uuid)
        parent_uuid = UUID(bytes=self.parent_uuid)
        send_parent_uuid = (
            UUID(bytes=self.send_parent_uuid) if self.send_parent_uuid else None
        )
        # arrow.get(float, tzinfo=None) raises an error, so we explicitly
        # default to UTC
        ctime = arrow.get(self.ctime, tzinfo="UTC" if tzinfo is None else tzinfo)
        suffixes = [
            f".t{ctime.isoformat(timespec='seconds')}",
            f".i{self.ctransid}",
            f".u{uuid}",
        ]
        if send_parent_uuid is None:
            suffixes.append(".full")
        else:
            suffixes.append(f".s{send_parent_uuid}")
        suffixes.append(f".p{parent_uuid}")
        return suffixes

    @classmethod
    def from_path(cls, path: str) -> Self:
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
        is_full = False
        ctransid: int | None = None
        ctime: Arrow | None = None

        suffixes = pathlib.PurePath(path).suffixes
        for suffix in suffixes:
            if suffix == ".full":
                is_full = True
            match suffix[1], suffix[2:]:
                case "p", rest:
                    with suppress(ValueError):
                        parent_uuid = UUID(rest)
                case "t", rest:
                    with suppress(arrow.ParserError):
                        ctime = arrow.get(rest)
                case "i", rest:
                    with suppress(ValueError):
                        ctransid = int(rest)
                case "u", rest:
                    with suppress(ValueError):
                        uuid = UUID(rest)
                case "s", rest:
                    with suppress(ValueError):
                        send_parent_uuid = UUID(rest)
        if (
            uuid is None
            or parent_uuid is None
            or ctransid is None
            or ctime is None
            or (send_parent_uuid is None and not is_full)
        ):
            msg = "missing or incomplete parameters for backup name"
            raise ValueError(msg)
        return cls(
            uuid=uuid.bytes,
            parent_uuid=parent_uuid.bytes,
            ctransid=ctransid,
            ctime=ctime.timestamp(),
            send_parent_uuid=send_parent_uuid.bytes if send_parent_uuid else None,
        )
