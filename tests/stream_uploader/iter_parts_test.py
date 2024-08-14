from __future__ import annotations

from typing import TYPE_CHECKING

from btrfs2s3.stream_uploader import _iter_parts_via_tempfile

if TYPE_CHECKING:
    from typing import IO


def test_iter_parts_via_tempfile(
    stream: IO[bytes], stream_data: bytes, part_size: int
) -> None:
    expected_data = [
        stream_data[i : i + part_size] for i in range(0, len(stream_data), part_size)
    ]

    got_data = []
    for part_file in _iter_parts_via_tempfile(stream, part_size):
        assert part_file.seekable()
        assert part_file.tell() == 0
        got_data.append(part_file.read())

    assert got_data == expected_data
