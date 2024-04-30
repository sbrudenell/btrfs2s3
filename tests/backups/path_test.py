from collections.abc import Sequence
import itertools
from uuid import UUID

import arrow
from btrfs2s3.backups import BackupInfo
import pytest


def test_get_path_suffixes_with_real_timezone() -> None:
    info = BackupInfo(
        uuid=UUID("3fd11d8e-8110-4cd0-b85c-bae3dda86a3d").bytes,
        parent_uuid=UUID("9d9d3bcb-4b62-46a3-b6e2-678eeb24f54e").bytes,
        ctransid=12345,
        ctime=arrow.get("2006-01-01", tzinfo="US/Pacific").timestamp(),
        send_parent_uuid=UUID("3ae01eae-d50d-4187-b67f-cef0ef973e1f").bytes,
    )
    got = info.get_path_suffixes(tzinfo="US/Pacific")
    expected = [
        ".t2006-01-01T00:00:00-08:00",
        ".i12345",
        ".u3fd11d8e-8110-4cd0-b85c-bae3dda86a3d",
        ".s3ae01eae-d50d-4187-b67f-cef0ef973e1f",
        ".p9d9d3bcb-4b62-46a3-b6e2-678eeb24f54e",
    ]
    assert got == expected

    round_trip = BackupInfo.from_path(f"name{''.join(got)}.gz")
    assert round_trip == info


def test_get_path_suffixes_default_to_utc() -> None:
    info = BackupInfo(
        uuid=UUID("3fd11d8e-8110-4cd0-b85c-bae3dda86a3d").bytes,
        parent_uuid=UUID("9d9d3bcb-4b62-46a3-b6e2-678eeb24f54e").bytes,
        ctransid=12345,
        ctime=arrow.get("2006-01-01").timestamp(),
        send_parent_uuid=UUID("3ae01eae-d50d-4187-b67f-cef0ef973e1f").bytes,
    )
    got = info.get_path_suffixes()
    expected = [
        ".t2006-01-01T00:00:00+00:00",
        ".i12345",
        ".u3fd11d8e-8110-4cd0-b85c-bae3dda86a3d",
        ".s3ae01eae-d50d-4187-b67f-cef0ef973e1f",
        ".p9d9d3bcb-4b62-46a3-b6e2-678eeb24f54e",
    ]
    assert got == expected

    round_trip = BackupInfo.from_path(f"name{''.join(got)}.gz")
    assert round_trip == info


def test_get_path_suffixes_with_full_backup() -> None:
    info = BackupInfo(
        uuid=UUID("3fd11d8e-8110-4cd0-b85c-bae3dda86a3d").bytes,
        parent_uuid=UUID("9d9d3bcb-4b62-46a3-b6e2-678eeb24f54e").bytes,
        ctransid=12345,
        ctime=arrow.get("2006-01-01").timestamp(),
        send_parent_uuid=None,
    )
    got = info.get_path_suffixes()
    expected = [
        ".t2006-01-01T00:00:00+00:00",
        ".i12345",
        ".u3fd11d8e-8110-4cd0-b85c-bae3dda86a3d",
        ".full",
        ".p9d9d3bcb-4b62-46a3-b6e2-678eeb24f54e",
    ]
    assert got == expected

    round_trip = BackupInfo.from_path(f"name{''.join(got)}.gz")
    assert round_trip == info


@pytest.mark.parametrize(
    "suffixes",
    itertools.permutations(
        [
            ".t2006-01-01T00:00:00-08:00",
            ".i12345",
            ".u3fd11d8e-8110-4cd0-b85c-bae3dda86a3d",
            ".s3ae01eae-d50d-4187-b67f-cef0ef973e1f",
            ".p9d9d3bcb-4b62-46a3-b6e2-678eeb24f54e",
            ".gz",
        ]
    ),
)
def test_from_path_with_suffixes_in_any_order(suffixes: Sequence[str]) -> None:
    path = f"my-backup{''.join(suffixes)}"
    got = BackupInfo.from_path(path)
    expected = BackupInfo(
        uuid=UUID("3fd11d8e-8110-4cd0-b85c-bae3dda86a3d").bytes,
        parent_uuid=UUID("9d9d3bcb-4b62-46a3-b6e2-678eeb24f54e").bytes,
        ctransid=12345,
        ctime=arrow.get("2006-01-01", tzinfo="US/Pacific").timestamp(),
        send_parent_uuid=UUID("3ae01eae-d50d-4187-b67f-cef0ef973e1f").bytes,
    )
    assert got == expected


@pytest.mark.parametrize(
    "bad_path",
    [
        "bad-path-no-suffixes",
        "bad.path.with.suffixes",
        ".t20O6-01-01T00:00:00-08:00.i12345.u3fd11d8e-8110-4cd0-b85c-bae3dda86a3d"
        ".s3ae01eae-d50d-4187-b67f-cef0ef973e1f.p9d9d3bcb-4b62-46a3-b6e2-678eeb24f54e",
        ".t2006-01-01T00:00:00-08:00.i12345.u3fd11d8e-811O-4cd0-b85c-bae3dda86a3d"
        ".s3ae01eae-d50d-4187-b67f-cef0ef973e1f.p9d9d3bcb-4b62-46a3-b6e2-678eeb24f54e",
        ".t2006-01-01T00:00:00-08:00.il2345.u3fd11d8e-8110-4cd0-b85c-bae3dda86a3d"
        ".s3ae01eae-d50d-4187-b67f-cef0ef973e1f.p9d9d3bcb-4b62-46a3-b6e2-678eeb24f54e",
        ".t2006-01-01T00:00:00-08:00.i12345.u3fd11d8e-8110-4cd0-b85c-bae3dda86a3d"
        ".s3ae01eae-d50d-4187-b67f-cef0ef973e1f.p9d9d3bcb-4b62-46a3-b6e2-678eeb24f54e",
        ".t2006-01-01T00:00:00-08:00.i12345.u3fd11d8e-8110-4cd0-b85c-bae3dda86a3d"
        ".s3ae01eae-d50d-4187-b67f-cef0ef973e1f.p9d9d3gcb-4b62-46a3-b6e2-678eeb24f54e",
    ],
)
def test_bad_paths(bad_path: str) -> None:
    with pytest.raises(
        ValueError, match="missing or incomplete parameters for backup name"
    ):
        BackupInfo.from_path(bad_path)
