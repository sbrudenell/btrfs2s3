"""Code for "btrfs2s3 run"."""

from __future__ import annotations

import logging
from pathlib import Path
import sys
from typing import Iterable
from typing import TYPE_CHECKING

import arrow
from boto3.session import Session
import zoneinfo

from btrfs2s3._internal.arrowutil import humanize_span
from btrfs2s3.action import Actions
from btrfs2s3.assessor import assess
from btrfs2s3.assessor import Assessment
from btrfs2s3.assessor import assessment_to_actions
from btrfs2s3.assessor import BackupAssessment
from btrfs2s3.assessor import SourceAssessment
from btrfs2s3.preservation import Params
from btrfs2s3.preservation import Policy
from btrfs2s3.preservation import TS
from btrfs2s3.thunk import TBD

if TYPE_CHECKING:
    import argparse
    from datetime import tzinfo

    from btrfs2s3.resolver import Reasons

_LOG = logging.getLogger(__name__)


def _time_span_key(time_span: TS) -> tuple[float, float]:
    start, end = time_span
    return (start - end, start)


def _describe_time_spans(time_spans: Iterable[TS], tzinfo: tzinfo) -> str:
    if not time_spans:
        return ""
    start_timestamp, end_timestamp = sorted(time_spans, key=_time_span_key)[0]
    return humanize_span(
        (
            arrow.get(start_timestamp, tzinfo=tzinfo),
            arrow.get(end_timestamp, tzinfo=tzinfo),
        ),
        bounds="[]",
    )


def _describe_reasons(reasons: Reasons) -> str:
    return str(reasons) if reasons else "(not keeping)"


def _backup_asmt_key(asmt: BackupAssessment) -> tuple[bool, int]:
    info = asmt.backup.peek()
    return (asmt.new, 0 if info is TBD else info.ctransid)


def _print_source_assessment(asmt: SourceAssessment, tzinfo: tzinfo) -> None:
    print("  snapshots of", asmt.path, ":")
    for snapshot_asmt in sorted(asmt.snapshots.values(), key=lambda a: a.info.ctransid):
        new = "*" if snapshot_asmt.new else ""
        path = str(snapshot_asmt.initial_path)
        time = arrow.get(snapshot_asmt.info.ctime, tzinfo=tzinfo).format(
            "YYYY-MM-DDTHH:mm:ss"
        )
        reasons = _describe_reasons(snapshot_asmt.keep_meta.reasons)
        time_span = _describe_time_spans(snapshot_asmt.keep_meta.time_spans, tzinfo)
        print("    ", new, path, time, reasons, time_span)

    print("  backups of", asmt.path, ":")
    for uuid, backup_asmt in sorted(
        asmt.backups.items(), key=lambda i: _backup_asmt_key(i[1])
    ):
        new = "*" if backup_asmt.new else ""
        if uuid in asmt.snapshots:
            path = str(asmt.snapshots[uuid].initial_path)
        else:
            path = "(no snapshot)"
        reasons = _describe_reasons(backup_asmt.keep_meta.reasons)
        time_span = _describe_time_spans(backup_asmt.keep_meta.time_spans, tzinfo)
        print("    ", new, path, reasons, time_span)


def _print_assessment(asmt: Assessment, tzinfo: tzinfo) -> None:
    print("Assessments:")
    for source_asmt in sorted(asmt.sources.values(), key=lambda a: a.path):
        _print_source_assessment(source_asmt, tzinfo)
    print()


def _print_actions(actions: Actions) -> None:
    print("Action plan:")
    for csi in actions.iter_create_snapshot_intents():
        print("  create snapshot:", csi.source.peek(), "->", csi.path.peek())
    for rsi in actions.iter_rename_snapshot_intents():
        print("  rename snapshot:", rsi.source.peek(), "->", rsi.target.peek())
    for cbi in actions.iter_create_backup_intents():
        print(
            "  create backup (",
            cbi.source.peek(),
            "):",
            cbi.snapshot.peek(),
            "/",
            cbi.send_parent.peek(),
            "->",
            cbi.key.peek(),
        )
    for dsi in actions.iter_delete_snapshot_intents():
        print("  delete snapshot:", dsi.path.peek())
    for dbi in actions.iter_delete_backup_intents():
        print("  delete backup:", dbi.key.peek())


NAME = "run"


def add_args(parser: argparse.ArgumentParser) -> None:
    """Add args for "btrfs2s3 run" to an ArgumentParser."""
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--pretend", action="store_true")
    parser.add_argument("--region")
    parser.add_argument("--profile")
    parser.add_argument("--endpoint-url")
    parser.add_argument("--no-verify", action="store_false")
    parser.add_argument("--source", action="append", type=Path, required=True)
    parser.add_argument("--snapshot-dir", type=Path, required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--timezone", type=zoneinfo.ZoneInfo, required=True)
    parser.add_argument("--preserve", type=Params.parse, required=True)


def command(args: argparse.Namespace) -> int:
    """Implements "btrfs2s3 run"."""
    if not sys.stdin.isatty() and not (args.force or args.pretend):
        _LOG.error("to run in unattended mode, use --force")
        return 1

    session = Session(region_name=args.region, profile_name=args.profile)
    s3 = session.client("s3", verify=not args.no_verify, endpoint_url=args.endpoint_url)
    policy = Policy(tzinfo=args.timezone, params=args.preserve)
    asmt = assess(
        snapshot_dir=args.snapshot_dir,
        sources=args.source,
        s3=s3,
        bucket=args.bucket,
        policy=policy,
    )
    actions = Actions()
    assessment_to_actions(asmt, actions)

    _print_assessment(asmt, args.timezone)
    _print_actions(actions)

    if args.pretend:
        return 0

    if actions.empty():
        print("nothing to be done")
        return 0

    if not args.force and input("continue? (Y/n)") != "Y":
        return 1  # pragma: no cover

    actions.execute(s3, args.bucket)

    return 0
