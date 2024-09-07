from __future__ import annotations

from typing import TYPE_CHECKING

from btrfs2s3.config import Config
from btrfs2s3.config import InvalidConfigError
from btrfs2s3.config import load_from_path
from btrfs2s3.config import RemoteConfig
from btrfs2s3.config import S3EndpointConfig
from btrfs2s3.config import S3RemoteConfig
from btrfs2s3.config import SourceConfig
from btrfs2s3.config import UploadToRemoteConfig
import pytest

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture()
def path(tmp_path: Path) -> Path:
    return tmp_path / "config.yaml"


def test_malformed(path: Path) -> None:
    path.write_text("malformed, bad text")
    with pytest.raises(InvalidConfigError):
        load_from_path(path)


def test_basic(path: Path) -> None:
    path.write_text("""
        timezone: a
        sources:
        - path: b
          snapshots: c
          upload_to_remotes:
          - id: aws
            preserve: 1y 1m
        remotes:
        - id: aws
          s3:
            bucket: d
    """)
    config = load_from_path(path)
    assert config == Config(
        {
            "timezone": "a",
            "sources": [
                SourceConfig(
                    {
                        "path": "b",
                        "snapshots": "c",
                        "upload_to_remotes": [
                            UploadToRemoteConfig({"id": "aws", "preserve": "1y 1m"})
                        ],
                    }
                )
            ],
            "remotes": [
                RemoteConfig({"id": "aws", "s3": S3RemoteConfig({"bucket": "d"})})
            ],
        }
    )


def test_multiple_sources_with_anchors_and_refs(path: Path) -> None:
    path.write_text("""
        timezone: a
        sources:
        - path: b
          snapshots: &snapshots c
          upload_to_remotes: &my_remotes
          - id: aws
            preserve: 1y 1m
        - path: otherpath
          snapshots: *snapshots
          upload_to_remotes: *my_remotes
        remotes:
        - id: aws
          s3:
            bucket: d
    """)
    config = load_from_path(path)
    source0 = config["sources"][0]
    source1 = config["sources"][1]
    assert source0["snapshots"] == source1["snapshots"]
    assert source1["upload_to_remotes"] == source1["upload_to_remotes"]


def test_s3_endpoint_config(path: Path) -> None:
    path.write_text("""
        timezone: a
        sources:
        - path: b
          snapshots: c
          upload_to_remotes:
          - id: aws
            preserve: 1y 1m
        remotes:
        - id: aws
          s3:
            bucket: d
            endpoint:
              aws_access_key_id: key
              aws_secret_access_key: secret
              region_name: region
              profile_name: profile
              verify: true
              endpoint_url: https://example.com
    """)
    endpoint = load_from_path(path)["remotes"][0]["s3"]["endpoint"]
    assert endpoint == S3EndpointConfig(
        {
            "aws_access_key_id": "key",
            "aws_secret_access_key": "secret",
            "region_name": "region",
            "profile_name": "profile",
            "verify": True,
            "endpoint_url": "https://example.com",
        }
    )


def test_pipe_through(path: Path) -> None:
    path.write_text("""
        timezone: a
        sources:
        - path: b
          snapshots: c
          upload_to_remotes:
          - id: aws
            preserve: 1y 1m
            pipe_through:
            - [gzip]
            - [gpg, encrypt, -r, me@example.com]
        remotes:
        - id: aws
          s3:
            bucket: d
    """)
    config = load_from_path(path)
    assert config["sources"][0]["upload_to_remotes"][0]["pipe_through"] == [
        ["gzip"],
        ["gpg", "encrypt", "-r", "me@example.com"],
    ]


def test_no_sources(path: Path) -> None:
    path.write_text("""
        timezone: a
        sources: []
        upload_to_remotes:
        - id: aws
          s3:
            bucket: d
    """)
    with pytest.raises(InvalidConfigError):
        load_from_path(path)


def test_no_remotes(path: Path) -> None:
    path.write_text("""
        timezone: a
        sources:
        - path: b
          snapshots: c
          remotes:
          - id: aws
            preserve: 1y 1m
        remotes: []
    """)
    with pytest.raises(InvalidConfigError):
        load_from_path(path)


def test_source_with_no_upload_to_remotes(path: Path) -> None:
    path.write_text("""
        timezone: a
        sources:
        - path: b
          snapshots: c
          upload_to_remotes: []
        remotes:
        - id: aws
          s3:
            bucket: d
    """)
    with pytest.raises(InvalidConfigError):
        load_from_path(path)


def test_invalid_preserve(path: Path) -> None:
    path.write_text("""
        timezone: a
        sources:
        - path: b
          snapshots: c
          upload_to_remotes:
          - id: aws
            preserve: invalid
        remotes:
        - id: aws
          s3:
            bucket: d
    """)
    with pytest.raises(InvalidConfigError):
        load_from_path(path)


def test_invalid_upload_to_remote_id(path: Path) -> None:
    path.write_text("""
        timezone: a
        sources:
        - path: b
          snapshots: c
          upload_to_remotes:
          - id: does_not_Exist
            preserve: 1y
        remotes:
        - id: aws
          s3:
            bucket: d
    """)
    with pytest.raises(InvalidConfigError):
        load_from_path(path)
