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

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from btrfs2s3._internal.config import Config
from btrfs2s3._internal.config import CostPerByteAndTimeConfig
from btrfs2s3._internal.config import InvalidConfigError
from btrfs2s3._internal.config import load_from_path
from btrfs2s3._internal.config import RemoteConfig
from btrfs2s3._internal.config import S3CostsConfig
from btrfs2s3._internal.config import S3EndpointConfig
from btrfs2s3._internal.config import S3RemoteConfig
from btrfs2s3._internal.config import S3StorageClassCostConfig
from btrfs2s3._internal.config import SourceConfig
from btrfs2s3._internal.config import UploadToRemoteConfig
from tests.conftest import AWS_COSTS_CFG
from tests.conftest import AWS_COSTS_YAML
from tests.conftest import B2_COSTS_CFG
from tests.conftest import B2_COSTS_YAML

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
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


def test_multiple_remotes(path: Path) -> None:
    path.write_text("""
        timezone: a
        sources:
        - path: b
          snapshots: c
          upload_to_remotes:
          - id: aws
            preserve: 1y 1m
          - id: b2
            preserve: 1y 1m
        remotes:
        - id: aws
          s3:
            bucket: d
        - id: b2
          s3:
            bucket: e
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
                            UploadToRemoteConfig({"id": "aws", "preserve": "1y 1m"}),
                            UploadToRemoteConfig({"id": "b2", "preserve": "1y 1m"}),
                        ],
                    }
                )
            ],
            "remotes": [
                RemoteConfig({"id": "aws", "s3": S3RemoteConfig({"bucket": "d"})}),
                RemoteConfig({"id": "b2", "s3": S3RemoteConfig({"bucket": "e"})}),
            ],
        }
    )


def test_multiple_snapshot_locations(path: Path) -> None:
    path.write_text("""
        timezone: a
        sources:
        - path: b
          snapshots: c
          upload_to_remotes:
          - id: aws
            preserve: 1y 1m
        - path: x
          snapshots: y
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
                ),
                SourceConfig(
                    {
                        "path": "x",
                        "snapshots": "y",
                        "upload_to_remotes": [
                            UploadToRemoteConfig({"id": "aws", "preserve": "1y 1m"})
                        ],
                    }
                ),
            ],
            "remotes": [
                RemoteConfig({"id": "aws", "s3": S3RemoteConfig({"bucket": "d"})})
            ],
        }
    )


def test_multiple_preserves(path: Path) -> None:
    path.write_text("""
        timezone: a
        sources:
        - path: b
          snapshots: c
          upload_to_remotes:
          - id: aws
            preserve: 1y 1m
        - path: x
          snapshots: c
          upload_to_remotes:
          - id: aws
            preserve: 1y
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
                ),
                SourceConfig(
                    {
                        "path": "x",
                        "snapshots": "c",
                        "upload_to_remotes": [
                            UploadToRemoteConfig({"id": "aws", "preserve": "1y"})
                        ],
                    }
                ),
            ],
            "remotes": [
                RemoteConfig({"id": "aws", "s3": S3RemoteConfig({"bucket": "d"})})
            ],
        }
    )


def test_multiple_pipe_throughs(path: Path) -> None:
    path.write_text("""
        timezone: a
        sources:
        - path: b
          snapshots: c
          upload_to_remotes:
          - id: aws
            preserve: 1y 1m
            pipe_through:
            - [gpg, --encrypt, -r, a@example.com]
        - path: x
          snapshots: c
          upload_to_remotes:
          - id: aws
            preserve: 1y 1m
            pipe_through:
            - [gpg, --encrypt, -r, b@example.com]
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
                            UploadToRemoteConfig(
                                {
                                    "id": "aws",
                                    "preserve": "1y 1m",
                                    "pipe_through": [
                                        ["gpg", "--encrypt", "-r", "a@example.com"]
                                    ],
                                }
                            )
                        ],
                    }
                ),
                SourceConfig(
                    {
                        "path": "x",
                        "snapshots": "c",
                        "upload_to_remotes": [
                            UploadToRemoteConfig(
                                {
                                    "id": "aws",
                                    "preserve": "1y 1m",
                                    "pipe_through": [
                                        ["gpg", "--encrypt", "-r", "b@example.com"]
                                    ],
                                }
                            )
                        ],
                    }
                ),
            ],
            "remotes": [
                RemoteConfig({"id": "aws", "s3": S3RemoteConfig({"bucket": "d"})})
            ],
        }
    )


def test_costs_with_defaults(path: Path) -> None:
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
            costs:
              storage_classes:
              - name: DEEP_ARCHIVE
                storage:
                  cost: 0.00099
    """)
    config = load_from_path(path)
    assert config == Config(
        timezone="a",
        sources=[
            SourceConfig(
                path="b",
                snapshots="c",
                upload_to_remotes=[UploadToRemoteConfig(id="aws", preserve="1y 1m")],
            )
        ],
        remotes=[
            RemoteConfig(
                id="aws",
                s3=S3RemoteConfig(
                    bucket="d",
                    costs=S3CostsConfig(
                        billing_period="P1M",
                        storage_time_granularity="PT1H",
                        storage_classes=[
                            S3StorageClassCostConfig(
                                name="DEEP_ARCHIVE",
                                storage=CostPerByteAndTimeConfig(
                                    cost=0.00099, per_bytes="GB", per_time="P1M"
                                ),
                            )
                        ],
                    ),
                ),
            )
        ],
    )


def test_costs_with_explicit_settings(path: Path) -> None:
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
            costs:
              billing_period: P1Y
              storage_classes:
              - name: DEEP_ARCHIVE
                min_time: P180D
                storage:
                  cost: 0.99
                  per_bytes: TB
                  per_time: PT1H
              storage_time_granularity: P1W
    """)
    config = load_from_path(path)
    assert config == Config(
        timezone="a",
        sources=[
            SourceConfig(
                path="b",
                snapshots="c",
                upload_to_remotes=[UploadToRemoteConfig(id="aws", preserve="1y 1m")],
            )
        ],
        remotes=[
            RemoteConfig(
                id="aws",
                s3=S3RemoteConfig(
                    bucket="d",
                    costs=S3CostsConfig(
                        billing_period="P1Y",
                        storage_time_granularity="P1W",
                        storage_classes=[
                            S3StorageClassCostConfig(
                                name="DEEP_ARCHIVE",
                                min_time="P180D",
                                storage=CostPerByteAndTimeConfig(
                                    cost=0.99, per_bytes="TB", per_time="PT1H"
                                ),
                            )
                        ],
                    ),
                ),
            )
        ],
    )


@pytest.mark.parametrize(
    ("costs_yaml", "expected_costs"),
    [(AWS_COSTS_YAML, AWS_COSTS_CFG), (B2_COSTS_YAML, B2_COSTS_CFG)],
    ids=["aws", "b2"],
)
def test_costs_from_fixtures(
    path: Path, costs_yaml: str, expected_costs: S3CostsConfig
) -> None:
    path.write_text(f"""
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
            costs: {costs_yaml}
    """)
    config = load_from_path(path)
    assert config == Config(
        timezone="a",
        sources=[
            SourceConfig(
                path="b",
                snapshots="c",
                upload_to_remotes=[UploadToRemoteConfig(id="aws", preserve="1y 1m")],
            )
        ],
        remotes=[
            RemoteConfig(id="aws", s3=S3RemoteConfig(bucket="d", costs=expected_costs))
        ],
    )


def test_bad_bytes_value(path: Path) -> None:
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
            costs:
              storage_classes:
              - name: DEEP_ARCHIVE
                storage:
                  cost: 0.00099
                  per_bytes: invalid
    """)
    with pytest.raises(InvalidConfigError):
        load_from_path(path)


def test_invalid_duration(path: Path) -> None:
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
            costs:
              storage_classes:
              - name: DEEP_ARCHIVE
                min_time: invalid
                storage:
                  cost: 0.00099
    """)
    with pytest.raises(InvalidConfigError):
        load_from_path(path)


def test_zero_duration(path: Path) -> None:
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
            costs:
              storage_classes:
              - name: DEEP_ARCHIVE
                min_time: P0D
                storage:
                  cost: 0.00099
    """)
    with pytest.raises(InvalidConfigError):
        load_from_path(path)


def test_invalid_per_time(path: Path) -> None:
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
            costs:
              storage_classes:
              - name: DEEP_ARCHIVE
                storage:
                  cost: 0.00099
                  per_time: invalid
    """)
    with pytest.raises(InvalidConfigError):
        load_from_path(path)


def test_multiple_items_in_per_time(path: Path) -> None:
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
            costs:
              storage_classes:
              - name: DEEP_ARCHIVE
                storage:
                  cost: 0.00099
                  per_time: P1M1D
    """)
    with pytest.raises(InvalidConfigError):
        load_from_path(path)


def test_non_unit_billing_period(path: Path) -> None:
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
            costs:
              billing_period: P2M
              storage_classes:
              - name: DEEP_ARCHIVE
                storage:
                  cost: 0.00099
    """)
    with pytest.raises(InvalidConfigError):
        load_from_path(path)


def test_non_unit_storage_time_granularity(path: Path) -> None:
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
            costs:
              storage_time_granularity: PT30M
              storage_classes:
              - name: DEEP_ARCHIVE
                storage:
                  cost: 0.00099
    """)
    with pytest.raises(InvalidConfigError):
        load_from_path(path)


def test_negative_cost(path: Path) -> None:
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
            costs:
              storage_classes:
              - name: DEEP_ARCHIVE
                storage:
                  cost: -1.0
    """)
    with pytest.raises(InvalidConfigError):
        load_from_path(path)
