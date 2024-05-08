from __future__ import annotations

from datetime import timezone
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

import arrow
from btrfs2s3._internal.util import backup_of_snapshot
from btrfs2s3._internal.util import mksubvol
from btrfs2s3._internal.util import SubvolumeFlags
from btrfs2s3.assessor import Assessment
from btrfs2s3.assessor import BackupAssessment
from btrfs2s3.assessor import SnapshotAssessment
from btrfs2s3.assessor import SourceAssessment
from btrfs2s3.commands.run import print_assessment
from btrfs2s3.resolver import Flags
from btrfs2s3.resolver import KeepMeta
from btrfs2s3.resolver import Reasons
from btrfs2s3.thunk import Thunk
import pytest

if TYPE_CHECKING:
    from rich.console import Console


print_assessment_under_test = partial(
    print_assessment,
    tzinfo=timezone.utc,
    snapshot_dir=Path("/snapshots"),
    bucket="my-bucket",
)


@pytest.fixture()
def golden_asmt_with_no_changes() -> Assessment:
    asmt = Assessment()
    source1 = mksubvol(
        id=12345,
        uuid=b"$\x96\xea\xb9\x1f\xbcD\x7f\xba\x98\xf1\xc5b\xaf\xc6A",
        ctransid=123,
        ctime=arrow.get("2006-01-02").timestamp(),
    )
    source1_asmt = SourceAssessment(path=Path("/path/to/source1"), info=source1)
    asmt.sources[source1.uuid] = source1_asmt
    snap1_1 = mksubvol(
        id=source1.id + 1,
        uuid=b"4k\xd4D\xb1\x87O\xdb\x88\xdd=\xa6\xfd\xef\x84B",
        parent_uuid=source1.uuid,
        flags=SubvolumeFlags.ReadOnly,
        ctransid=source1.ctransid - 3,
        ctime=source1.ctime - 86400,
    )
    source1_asmt.snapshots[snap1_1.uuid] = SnapshotAssessment(
        initial_path=Path("/snapshots/snap1_1"),
        info=snap1_1,
        target_path=Thunk(Path("/snapshots/snap1_1")),
        real_info=Thunk(snap1_1),
        keep_meta=KeepMeta(
            reasons=Reasons.Preserved,
            time_spans={(arrow.get("2006").timestamp(), arrow.get("2007").timestamp())},
        ),
    )
    snap1_2 = mksubvol(
        id=snap1_1.id + 1,
        uuid=b"\x11[\xab\xa0\xec\xedA\xa6\xbbm3D\x9c\x92\xbf=",
        parent_uuid=source1.uuid,
        flags=SubvolumeFlags.ReadOnly,
        ctransid=source1.ctransid - 1,
        ctime=source1.ctime,
    )
    source1_asmt.snapshots[snap1_2.uuid] = SnapshotAssessment(
        initial_path=Path("/snapshots/snap1_2"),
        info=snap1_2,
        target_path=Thunk(Path("/snapshots/snap1_2")),
        real_info=Thunk(snap1_2),
        keep_meta=source1_asmt.snapshots[snap1_1.uuid].keep_meta,
    )
    backup1_1 = backup_of_snapshot(snap1_1, send_parent=None)
    source1_asmt.backups[backup1_1.uuid] = BackupAssessment(
        backup=Thunk(backup1_1),
        key=Thunk("backup1_1"),
        keep_meta=source1_asmt.snapshots[snap1_1.uuid].keep_meta,
    )
    backup1_2 = backup_of_snapshot(snap1_2, send_parent=snap1_1)
    source1_asmt.backups[backup1_2.uuid] = BackupAssessment(
        backup=Thunk(backup1_2),
        key=Thunk("backup1_2"),
        keep_meta=source1_asmt.snapshots[snap1_2.uuid].keep_meta,
    )
    return asmt


def test_print_assessment(
    golden_asmt_with_no_changes: Assessment, goldifyconsole: Console
) -> None:
    print_assessment_under_test(
        asmt=golden_asmt_with_no_changes, console=goldifyconsole
    )


def test_dont_keep(
    golden_asmt_with_no_changes: Assessment, goldifyconsole: Console
) -> None:
    # Pick an arbitrary, stable source
    _, source_asmt = sorted(golden_asmt_with_no_changes.sources.items())[0]
    # Pick an arbitrary, stable snapshot
    _, snapshot_asmt = sorted(source_asmt.snapshots.items())[0]
    # Mark the snapshot as not kept
    snapshot_asmt.keep_meta = KeepMeta()
    # Associated backup is not kept either
    backup_asmt = source_asmt.backups[snapshot_asmt.info.uuid]
    backup_asmt.keep_meta = KeepMeta()

    print_assessment_under_test(
        asmt=golden_asmt_with_no_changes, console=goldifyconsole
    )


def test_most_recent(
    golden_asmt_with_no_changes: Assessment, goldifyconsole: Console
) -> None:
    # Pick an arbitrary, stable source
    _, source_asmt = sorted(golden_asmt_with_no_changes.sources.items())[0]
    # Pick an arbitrary, stable snapshot
    _, snapshot_asmt = sorted(source_asmt.snapshots.items())[0]
    # Mark the snapshot as kept due to being the most recent
    snapshot_asmt.keep_meta = KeepMeta(reasons=Reasons.MostRecent)
    # Associated backup is kept for the same reason
    backup_asmt = source_asmt.backups[snapshot_asmt.info.uuid]
    backup_asmt.keep_meta = KeepMeta(reasons=Reasons.MostRecent)

    print_assessment_under_test(
        asmt=golden_asmt_with_no_changes, console=goldifyconsole
    )


def test_new_proposed(
    golden_asmt_with_no_changes: Assessment, goldifyconsole: Console
) -> None:
    # Pick an arbitrary, stable source
    _, source_asmt = sorted(golden_asmt_with_no_changes.sources.items())[0]
    # Create a new proposed snapshot
    proposed = mksubvol(
        id=999,
        uuid=b"\xda\x07m\x9ex\xfaLw\x89\xafYF>\xdc1\xf4",
        parent_uuid=source_asmt.info.uuid,
        flags=SubvolumeFlags.ReadOnly | SubvolumeFlags.Proposed,
        ctransid=source_asmt.info.ctransid,
        ctime=source_asmt.info.ctime,
    )
    source_asmt.snapshots[proposed.uuid] = SnapshotAssessment(
        initial_path=Path("/snapshots/proposed"),
        info=proposed,
        target_path=Thunk(lambda: Path("/snapshots/snap1_3")),  # pragma: no cover
        real_info=Thunk(lambda: proposed),  # pragma: no cover
        keep_meta=KeepMeta(reasons=Reasons.MostRecent, flags=Flags.New),
    )
    # And associated backup
    backup = backup_of_snapshot(proposed, send_parent=None)
    source_asmt.backups[backup.uuid] = BackupAssessment(
        backup=Thunk(lambda: backup),  # pragma: no cover
        key=Thunk(lambda: "backup1-3"),  # pragma: no cover
        keep_meta=source_asmt.snapshots[proposed.uuid].keep_meta,
    )

    print_assessment_under_test(
        asmt=golden_asmt_with_no_changes, console=goldifyconsole
    )


def test_backup_without_snapshot(
    golden_asmt_with_no_changes: Assessment, goldifyconsole: Console
) -> None:
    # Pick an arbitrary, stable source
    _, source_asmt = sorted(golden_asmt_with_no_changes.sources.items())[0]
    # Pick an arbitrary, stable snapshot
    uuid, _ = sorted(source_asmt.snapshots.items())[0]
    source_asmt.snapshots.pop(uuid)

    print_assessment_under_test(
        asmt=golden_asmt_with_no_changes, console=goldifyconsole
    )


def test_send_ancestor(
    golden_asmt_with_no_changes: Assessment, goldifyconsole: Console
) -> None:
    # Pick an arbitrary, stable source
    _, source_asmt = sorted(golden_asmt_with_no_changes.sources.items())[0]
    # Pick an arbitrary, stable backup which has a send-parent
    child_uuid, child_backup_asmt = sorted(
        source_asmt.backups.items(),
        key=lambda i: (i[1].backup().send_parent_uuid is None, i[0]),
    )[0]
    # Get its parent snapshot and backup
    parent_uuid = child_backup_asmt.backup().send_parent_uuid
    assert parent_uuid is not None
    parent_snapshot_asmt = source_asmt.snapshots[parent_uuid]
    parent_backup_asmt = source_asmt.backups[parent_uuid]
    # Snapshot is not kept
    parent_snapshot_asmt.keep_meta = KeepMeta()
    # Backup must be kept because it's the send-parent of another backup
    parent_backup_asmt.keep_meta = KeepMeta(
        reasons=Reasons.SendAncestor, other_uuids={child_uuid}
    )

    print_assessment_under_test(
        asmt=golden_asmt_with_no_changes, console=goldifyconsole
    )
