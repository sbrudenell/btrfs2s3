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

"""Functions for uploading a non-seekable stream to S3."""

from __future__ import annotations

import os
from tempfile import TemporaryFile
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import AnyStr
    from typing import IO

    from types_boto3_s3.client import S3Client
    from types_boto3_s3.type_defs import CompletedPartTypeDef

_COPY_BUFFER_SIZE = 2**20


def _copy(*, input_file: IO[bytes], output_file: IO[bytes], count: int) -> int:
    # notes for fast copy:
    # - buffered IO classes have seek position out of sync with underlying file
    #   descriptor, mitigate this with SEEK_END
    # - sendfile and splice. which should be used first?
    # - use readinto and reuse buffers for slow mode
    written = 0
    eof = False
    while written < count and not eof:
        # This may result in one or multiple underlying reads
        buf = input_file.read(min(count - written, _COPY_BUFFER_SIZE))
        if not buf:
            eof = True
            break
        # https://docs.python.org/3/library/io.html#io.RawIOBase.write
        # indicates this loop is needed
        offset = 0
        while offset < len(buf):
            offset += output_file.write(buf[offset:])
        written += len(buf)
    return written


def _iter_parts_via_tempfile(stream: IO[bytes], part_size: int) -> Iterator[IO[bytes]]:
    while True:
        with TemporaryFile() as part_file:
            written = _copy(input_file=stream, output_file=part_file, count=part_size)
            if written > 0:
                part_file.seek(0, os.SEEK_SET)
                yield part_file
            if written < part_size:
                break


def _stream_len(stream: IO[AnyStr]) -> int:
    cur = stream.tell()
    end = stream.seek(0, os.SEEK_END)
    stream.seek(cur, os.SEEK_SET)
    return end


def upload_non_seekable_stream_via_tempfile(
    *, stream: IO[bytes], client: S3Client, bucket: str, key: str, part_size: int
) -> None:
    """Upload a non-seekable stream to S3.

    This will store the stream in parts to temporary files, of part_size bytes
    each. If less than one full part is consumed from the stream, it will
    upload the object with put_object. Otherwise, a multipart upload will be
    used.

    If any error is raised, this function will attempt to cancel the multipart
    upload with abort_multipart_upload().

    Args:
        stream: A stream to upload. The stream may be seekable, but this
            function is designed for the non-seekable case.
        client: The S3 client object.
        bucket: The name of the S3 bucket.
        key: The key of the S3 object in the bucket.
        part_size: The maximum size of a single part.
    """
    # If the first part is the maximum part size, assume there will be more parts. This
    # is suboptimal in the rare case that the stream is exactly one part length long.
    # The alternative is to attempt to read an extra byte from the stream after the
    # first part has been collected, and append it to the next part. The 1-byte reads
    # will frequently be unaligned and lead to cache thrashing. The optimal strategy
    # would be:
    # - Read the first full part
    # - Read 1 test byte
    # - Read the second part minus one byte
    # - Read the remaining parts as normal
    # This would be a lot of code complexity for a very rare gain.
    upload_id: str | None = None
    completed_parts: list[CompletedPartTypeDef] = []
    try:
        for part_index, part_file in enumerate(
            _iter_parts_via_tempfile(stream, part_size)
        ):
            if upload_id is None and _stream_len(part_file) == part_size:
                upload_id = client.create_multipart_upload(Bucket=bucket, Key=key)[
                    "UploadId"
                ]
            if upload_id is not None:
                part_number = part_index + 1
                up_response = client.upload_part(
                    Bucket=bucket,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=part_file,
                )
                completed_parts.append(
                    {"ETag": up_response["ETag"], "PartNumber": part_number}
                )
            else:
                client.put_object(Bucket=bucket, Key=key, Body=part_file)
        if upload_id is not None:
            client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": completed_parts},
            )
    except Exception:
        if upload_id is not None:
            client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise
