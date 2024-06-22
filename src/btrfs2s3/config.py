from __future__ import annotations

from functools import partial
from typing import Any
from typing import TypedDict
from urllib.parse import urlparse

from cfgv import Array
from cfgv import check_int
from cfgv import check_string
from cfgv import load_from_filename
from cfgv import Map
from cfgv import OptionalNoDefault
from cfgv import OptionalRecurse
from cfgv import Required
from cfgv import RequiredRecurse
from typing_extensions import NotRequired
from yaml import safe_load

from btrfs2s3.preservation import Params


class Error(Exception):
    pass


class InvalidConfigError(Error):
    pass


def check_verify(v: Any) -> None:  # noqa: ANN401
    if isinstance(v, bool):
        return
    if not isinstance(v, str):
        raise InvalidConfigError(f"Expected bool or url, got {type(v).__name__}")
    check_url(v)


def check_url(v: Any) -> None:  # noqa: ANN401
    if not isinstance(v, str):
        raise InvalidConfigError(f"Expected url, got {type(v).__name__}")
    try:
        urlparse(v)
    except ValueError as ex:
        raise InvalidConfigError("Expected a valid url") from ex


def check_preserve(v: Any) -> None:  # noqa: ANN401
    check_string(v)
    try:
        Params.parse(v)
    except ValueError as ex:
        raise InvalidConfigError("Expected a valid schedule") from ex


class S3EndpointConfig(TypedDict):
    aws_access_key_id: NotRequired[str]
    aws_secret_access_key: NotRequired[str]
    region_name: NotRequired[str]
    profile_name: NotRequired[str]
    api_version: NotRequired[str]
    verify: NotRequired[bool | str]
    endpoint_url: NotRequired[str]


S3_ENDPOINT_SCHEMA = Map(
    "S3EndpointConfig",
    None,
    OptionalNoDefault("aws_access_key_id", check_string),
    OptionalNoDefault("aws_secret_access_key", check_string),
    OptionalNoDefault("region_name", check_string),
    OptionalNoDefault("profile_name", check_string),
    OptionalNoDefault("api_version", check_string),
    OptionalNoDefault("verify", check_verify),
    OptionalNoDefault("endpoint_url", check_url),
)


class S3TransferConfig(TypedDict):
    multipart_chunksize: NotRequired[int]


S3_TRANSFER_SCHEMA = Map(
    "S3TransferConfig", None, OptionalNoDefault("multipart_chunksize", check_int)
)


class S3Config(TypedDict):
    endpoint: S3EndpointConfig
    transfer: NotRequired[S3TransferConfig]


S3_SCHEMA = Map(
    "S3Config",
    None,
    RequiredRecurse("endpoint", S3_ENDPOINT_SCHEMA),
    OptionalRecurse("transfer", S3_TRANSFER_SCHEMA, {}),
)


class TargetConfig(TypedDict):
    s3: S3Config


TARGET_SCHEMA = Map("TargetConfig", None, OptionalRecurse("s3", S3_SCHEMA, {}))


class SourceConfig(TypedDict):
    source: str
    snapshot_dir: str
    preserve: str
    targets: list[TargetConfig]


SOURCE_SCHEMA = Map(
    "SourceConfig",
    "path",
    Required("path", check_string),
    Required("snapshots", check_string),
    Required("preserve", check_preserve),
    RequiredRecurse("targets", Array(TARGET_SCHEMA, allow_empty=False)),
)


class Config(TypedDict):
    timezone: str
    sources: list[SourceConfig]


SCHEMA = Map(
    "Config",
    None,
    Required("timezone", check_string),
    RequiredRecurse("sources", Array(SOURCE_SCHEMA, allow_empty=False)),
)


load = partial(
    load_from_filename, SCHEMA, load_strategy=safe_load, exc_tp=InvalidConfigError
)
