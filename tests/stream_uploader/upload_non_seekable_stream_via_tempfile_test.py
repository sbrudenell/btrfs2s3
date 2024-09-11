from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import Mock

from btrfs2s3._internal.stream_uploader import upload_non_seekable_stream_via_tempfile
import pytest

if TYPE_CHECKING:
    from typing import IO

    from mypy_boto3_s3.client import S3Client


def test_file_is_uploaded(
    s3: S3Client, stream: IO[bytes], bucket: str, part_size: int, stream_data: bytes
) -> None:
    key = "test-key"

    upload_non_seekable_stream_via_tempfile(
        client=s3, stream=stream, bucket=bucket, key=key, part_size=part_size
    )

    assert s3.get_object(Bucket=bucket, Key=key)["Body"].read() == stream_data


class FakeError(Exception):
    pass


def test_multipart_upload_gets_cancelled_on_error(
    s3: S3Client, stream: IO[bytes], bucket: str, part_size: int
) -> None:
    key = "test-key"

    mock_client = Mock(wraps=s3)
    mock_client.put_object.side_effect = FakeError()
    mock_client.upload_part.side_effect = FakeError()

    with pytest.raises(FakeError):
        upload_non_seekable_stream_via_tempfile(
            client=mock_client,
            stream=stream,
            bucket=bucket,
            key=key,
            part_size=part_size,
        )

    assert s3.list_multipart_uploads(Bucket=bucket).get("Uploads", []) == []
