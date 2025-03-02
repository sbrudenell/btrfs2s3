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

"""Code for manipulating config for btrfs2s3."""

from __future__ import annotations

from collections import namedtuple
from typing import Any
from typing import cast
from typing import TYPE_CHECKING
from typing import TypedDict

from cfgv import Array
from cfgv import check_array
from cfgv import check_string
from cfgv import check_type
from cfgv import load_from_filename
from cfgv import Map
from cfgv import OptionalNoDefault
from cfgv import OptionalRecurse
from cfgv import Required
from cfgv import RequiredRecurse
from typing_extensions import NotRequired
from yaml import safe_load

from btrfs2s3._internal.preservation import Params

if TYPE_CHECKING:
    from os import PathLike


class Error(Exception):
    """The top-level class for exceptions generated by this module."""


class InvalidConfigError(Error):
    """An error for invalid config."""


def _check_preserve(v: Any) -> None:  # noqa: ANN401
    check_string(v)
    try:
        Params.parse(v)
    except ValueError as ex:
        msg = "Expected a valid schedule"
        raise InvalidConfigError(msg) from ex


# this is the same style used in cfgv
_OptionalRecurseNoDefault = namedtuple(  # noqa: PYI024
    "_OptionalRecurseNoDefault", ("key", "schema")
)
_OptionalRecurseNoDefault.check = OptionalRecurse.check  # type: ignore[attr-defined]
_OptionalRecurseNoDefault.check_fn = OptionalRecurse.check_fn  # type: ignore[attr-defined]
_OptionalRecurseNoDefault.apply_default = OptionalNoDefault.apply_default  # type: ignore[attr-defined]
_OptionalRecurseNoDefault.remove_default = OptionalNoDefault.remove_default  # type: ignore[attr-defined]


class S3EndpointConfig(TypedDict):
    """A config dict for how to talk to an S3 endpoint."""

    aws_access_key_id: NotRequired[str]
    aws_secret_access_key: NotRequired[str]
    region_name: NotRequired[str]
    profile_name: NotRequired[str]
    verify: NotRequired[bool | str]
    endpoint_url: NotRequired[str]


_S3_ENDPOINT_SCHEMA = Map(
    "S3EndpointConfig",
    None,
    OptionalNoDefault("aws_access_key_id", check_string),
    OptionalNoDefault("aws_secret_access_key", check_string),
    OptionalNoDefault("region_name", check_string),
    OptionalNoDefault("profile_name", check_string),
    OptionalNoDefault("verify", check_type((bool, str), typename="bool or path")),
    OptionalNoDefault("endpoint_url", check_string),
)


class S3RemoteConfig(TypedDict):
    """A config dict for how to access an S3 remote."""

    bucket: str
    endpoint: NotRequired[S3EndpointConfig]


_S3_SCHEMA = Map(
    "S3RemoteConfig",
    None,
    Required("bucket", check_string),
    _OptionalRecurseNoDefault("endpoint", _S3_ENDPOINT_SCHEMA),
)


class RemoteConfig(TypedDict):
    """A config dict for how to access a remote."""

    id: str
    s3: NotRequired[S3RemoteConfig]


_REMOTE_SCHEMA = Map(
    "RemoteConfig",
    None,
    Required("id", check_string),
    RequiredRecurse("s3", _S3_SCHEMA),
)


class UploadToRemoteConfig(TypedDict):
    """A config dict for uploading a source to a remote."""

    id: str
    preserve: str
    pipe_through: NotRequired[list[list[str]]]


_UPLOAD_TO_REMOTE_SCHEMA = Map(
    "UploadToRemoteConfig",
    "id",
    Required("preserve", _check_preserve),
    OptionalNoDefault("pipe_through", check_array(check_array(check_string))),
)


class SourceConfig(TypedDict):
    """A config dict for a source."""

    path: str
    snapshots: str
    upload_to_remotes: list[UploadToRemoteConfig]


_SOURCE_SCHEMA = Map(
    "SourceConfig",
    "path",
    Required("path", check_string),
    Required("snapshots", check_string),
    RequiredRecurse(
        "upload_to_remotes", Array(_UPLOAD_TO_REMOTE_SCHEMA, allow_empty=False)
    ),
)


class Config(TypedDict):
    """The top-level config dict.

    This just matches the data as it's stored in config.yaml. We don't do any
    transformation up front (for example "preserve" values are just their
    strings, not Policy objects).
    """

    timezone: str
    sources: list[SourceConfig]
    remotes: list[RemoteConfig]


_SCHEMA = Map(
    "Config",
    None,
    Required("timezone", check_string),
    RequiredRecurse("sources", Array(_SOURCE_SCHEMA, allow_empty=False)),
    RequiredRecurse("remotes", Array(_REMOTE_SCHEMA, allow_empty=False)),
)


def load_from_path(path: str | PathLike[str]) -> Config:
    """Load config from a file path.

    This performs some basic syntactic validation on the config, to ensure it
    really conforms to the return type.

    Args:
        path: The path to the config.

    Returns:
        A Config instance.

    Raises:
        InvalidConfigError: If the config does not pass validation.
    """
    config = cast(
        Config, load_from_filename(path, _SCHEMA, safe_load, exc_tp=InvalidConfigError)
    )

    remote_ids = {remote["id"] for remote in config["remotes"]}
    for source in config["sources"]:
        for upload_to_remote in source["upload_to_remotes"]:
            if upload_to_remote["id"] not in remote_ids:
                msg = (
                    f"remote id {upload_to_remote['id']!r} for source "
                    f"{source['path']!r} is not defined in the list of remotes"
                )
                raise InvalidConfigError(msg)

    # https://github.com/sbrudenell/btrfs2s3/issues/29
    if len(config["remotes"]) > 1:
        msg = "multiple remotes not supported in this release"
        raise InvalidConfigError(msg)

    all_sources = config["sources"]

    # https://github.com/sbrudenell/btrfs2s3/issues/81
    if len({source["snapshots"] for source in all_sources}) > 1:
        msg = "multiple snapshot locations not supported in this release"
        raise InvalidConfigError(msg)

    all_uploads = [up for src in all_sources for up in src["upload_to_remotes"]]

    # https://github.com/sbrudenell/btrfs2s3/issues/79
    if len({upload["preserve"] for upload in all_uploads}) > 1:
        msg = "multiple preserve configurations not supported in this release"
        raise InvalidConfigError(msg)

    all_pipe_throughs = [up.get("pipe_through", []) for up in all_uploads]

    # https://github.com/sbrudenell/btrfs2s3/issues/80
    if len({tuple(tuple(cmd) for cmd in p) for p in all_pipe_throughs}) > 1:
        msg = "multiple pipe_through configurations not supported in this release"
        raise InvalidConfigError(msg)

    return config
