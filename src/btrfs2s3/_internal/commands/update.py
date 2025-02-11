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

"""Code for "btrfs2s3 update"."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import cast
from typing import TYPE_CHECKING

import arrow
from boto3.session import Session
from rich.box import HORIZONTALS
from rich.console import Console
from rich.console import Group
from rich.highlighter import ISO8601Highlighter
from rich.panel import Panel
from rich.prompt import Confirm
from rich.rule import Rule
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

from btrfs2s3._internal.action import Actions
from btrfs2s3._internal.assessor import assess
from btrfs2s3._internal.assessor import Assessment
from btrfs2s3._internal.assessor import assessment_to_actions
from btrfs2s3._internal.assessor import BackupAssessment
from btrfs2s3._internal.assessor import SourceAssessment
from btrfs2s3._internal.config import Config
from btrfs2s3._internal.config import load_from_path
from btrfs2s3._internal.preservation import Params
from btrfs2s3._internal.preservation import Policy
from btrfs2s3._internal.preservation import TS
from btrfs2s3._internal.resolver import Flags
from btrfs2s3._internal.resolver import KeepMeta
from btrfs2s3._internal.resolver import Reasons
from btrfs2s3._internal.thunk import TBD
from btrfs2s3._internal.zoneinfo import get_zoneinfo

if TYPE_CHECKING:
    import argparse
    from datetime import tzinfo
    from typing import Iterable
    from typing import Literal
    from typing import Sequence
    from typing import TypedDict

    from typing_extensions import TypeAlias

    _Bounds: TypeAlias = Literal["[)", "()", "(]", "[]"]

_iso8601_highlight = ISO8601Highlighter()


def _time_span_key(time_span: TS) -> tuple[float, float]:
    start, end = time_span
    return (start - end, start)


def _describe_time_spans(time_spans: Iterable[TS], tzinfo: tzinfo) -> Text:
    return describe_time_span(
        sorted(time_spans, key=_time_span_key)[0], tzinfo, bounds="[]"
    )


def describe_time_span(
    time_span: TS, tzinfo: tzinfo, *, bounds: _Bounds = "[)"
) -> Text:
    """Returns a highlighted summary of a time span in context of preservation."""
    a_timestamp, b_timestamp = time_span
    a = arrow.get(a_timestamp, tzinfo=tzinfo)
    b = arrow.get(b_timestamp, tzinfo=tzinfo)
    if (a, b) == a.span("year", bounds=bounds):
        return Text.from_markup(f"[iso8601.date]{a.year:04d}[/] yearly")
    if (a, b) == a.span("quarter", bounds=bounds):
        return Text.from_markup(f"[iso8601.date]{a.year:04d}-Q{a.quarter}[/] quarterly")
    if (a, b) == a.span("month", bounds=bounds):
        return Text.from_markup(f"[iso8601.date]{a.year:04d}-{a.month:02d}[/] monthly")
    if (a, b) == a.span("week", bounds=bounds):
        return Text.from_markup(f"[iso8601.date]{a.year:04d}-W{a.week:02d}[/] weekly")
    if (a, b) == a.span("day", bounds=bounds):
        return Text.from_markup(
            f"[iso8601.date]{a.year:04d}-{a.month:02d}-{a.day:02d}[/] daily"
        )
    if (a, b) == a.span("hour", bounds=bounds):
        return Text.from_markup(
            f"[iso8601.date]{a.year:04d}-{a.month:02d}-{a.day:02d}[/]T"
            f"[iso8601.time]{a.hour:02d}[/] hourly"
        )
    if (a, b) == a.span("minute", bounds=bounds):
        return Text.from_markup(
            f"[iso8601.date]{a.year:04d}-{a.month:02d}-{a.day:02d}[/]T"
            f"[iso8601.time]{a.hour:02d}:{a.minute:02d}[/] minutely"
        )
    if (a, b) == a.span("second", bounds=bounds):
        return Text.from_markup(
            f"[iso8601.date]{a.year:04d}-{a.month:02d}-{a.day:02d}[/]T"
            f"[iso8601.time]{a.hour:02d}:{a.minute:02d}:{a.second:02d}[/] secondly"
        )
    return (
        _iso8601_highlight(a.format("YYYY-MM-DDTHH:mm:ss"))
        .append("/")
        .append(_iso8601_highlight(b.format("YYYY-MM-DDTHH:mm:ss")))
    )


def _describe_preserve(keep_meta: KeepMeta, tzinfo: tzinfo) -> Text:
    if keep_meta.reasons & Reasons.Preserved:
        return _describe_time_spans(keep_meta.time_spans, tzinfo)
    if keep_meta.reasons & Reasons.MostRecent:
        return Text("<most recent>", style="keep")
    if keep_meta.reasons & Reasons.SendAncestor:
        return Text("<ancestor>", style="keep")
    if keep_meta.reasons:  # pragma: no cover
        return Text("<keep!>", style="keep")
    return Text("<not keeping>", style="not_keeping")


def _describe_time(time: float, tzinfo: tzinfo) -> Text:
    return _iso8601_highlight(
        arrow.get(time, tzinfo=tzinfo).format("YYYY-MM-DDTHH:mm:ss")
    )


def _keep_emoji(keep_meta: KeepMeta) -> Text:
    if keep_meta.flags & Flags.New:
        return Text.from_markup(":sparkles:")
    if not keep_meta.reasons:
        return Text.from_markup(":skull:")
    return Text()


def _backup_asmt_key(asmt: BackupAssessment) -> tuple[bool, int]:
    info = asmt.backup.peek()
    return (asmt.new, 0 if info is TBD else info.ctransid)


def _make_snapshots_table(
    *, asmt: SourceAssessment, tzinfo: tzinfo, snapshot_dir: Path
) -> Table:
    table = Table(
        title=f"snapshots of {asmt.path} in {snapshot_dir}",
        title_justify="left",
        row_styles=["none", "dim"],
        box=HORIZONTALS,
    )
    table.add_column("path", no_wrap=True)
    table.add_column("ctime", no_wrap=True)
    table.add_column("ctransid", style="ctransid", no_wrap=True)
    table.add_column("preserve", no_wrap=True)
    for snapshot in sorted(asmt.snapshots.values(), key=lambda a: a.info.ctransid):
        path = _keep_emoji(snapshot.keep_meta).append(str(snapshot.initial_path))
        ctime = _describe_time(snapshot.info.ctime, tzinfo)
        ctransid = str(snapshot.info.ctransid)
        preserve = _describe_preserve(snapshot.keep_meta, tzinfo)
        table.add_row(path, ctime, ctransid, preserve)
    return table


def _make_backups_tree(*, asmt: SourceAssessment, tzinfo: tzinfo, bucket: str) -> Tree:
    uuid_to_children: dict[bytes | None, list[bytes]] = defaultdict(list)
    for uuid, backup in asmt.backups.items():
        parent: bytes | None = None
        if not backup.backup.is_tbd():
            parent = backup.backup().send_parent_uuid
        uuid_to_children[parent].append(uuid)

    def add_children(tree: Tree, parent: bytes | None) -> None:
        children = {u: asmt.backups[u] for u in uuid_to_children.get(parent, ())}
        for uuid, backup in sorted(
            children.items(), key=lambda i: _backup_asmt_key(i[1])
        ):
            lines = []

            key = Text("key: ", no_wrap=True)
            key.append(_keep_emoji(backup.keep_meta))
            key.append(str(backup.key.peek()), style="key")

            if not backup.backup.is_tbd():
                info = Text(no_wrap=True)
                info.append(_describe_time(backup.backup().ctime, tzinfo))
                info.append(" / ctransid ")
                info.append(str(backup.backup().ctransid), style="ctransid")
                lines.append(info)

            preserve = Text(no_wrap=True)
            preserve.append(_describe_preserve(backup.keep_meta, tzinfo))
            lines.append(preserve)

            if uuid in asmt.snapshots:
                origin = Text(no_wrap=True)
                origin.append("backup of: ")
                origin.append(_keep_emoji(asmt.snapshots[uuid].keep_meta))
                origin.append(str(asmt.snapshots[uuid].initial_path))
                lines.append(origin)

            subtree = tree.add(Panel(Group(*lines), title=key, title_align="left"))
            add_children(subtree, uuid)

    tree = Tree(f"tree of differential backups of {asmt.path} in bucket {bucket}")
    add_children(tree, None)
    return tree


def _print_source_assessment(
    *,
    console: Console,
    asmt: SourceAssessment,
    tzinfo: tzinfo,
    snapshot_dir: Path,
    bucket: str,
) -> None:
    console.print(
        _make_snapshots_table(asmt=asmt, tzinfo=tzinfo, snapshot_dir=snapshot_dir)
    )
    console.print()
    console.print(_make_backups_tree(asmt=asmt, tzinfo=tzinfo, bucket=bucket))
    console.print()


def print_assessment(
    *,
    console: Console,
    asmt: Assessment,
    tzinfo: tzinfo,
    snapshot_dir: Path,
    bucket: str,
) -> None:
    """Prints an Assessment to a Console."""
    console.print(Rule("assessment and proposed new state"))
    console.print()
    for source_asmt in sorted(asmt.sources.values(), key=lambda a: a.path):
        _print_source_assessment(
            console=console,
            asmt=source_asmt,
            tzinfo=tzinfo,
            snapshot_dir=snapshot_dir,
            bucket=bucket,
        )
        console.print()


def _make_action_tables(actions: Actions) -> Sequence[Table]:
    create_snapshots = Table(
        "action",
        "source",
        "arrow",
        "path",
        box=None,
        show_header=False,
        row_styles=["none", "dim"],
    )
    for csi in actions.iter_create_snapshot_intents():
        create_snapshots.add_row(
            Text.from_markup(":camera_with_flash: create snapshot:"),
            str(csi.source.peek()),
            "->",
            str(csi.path.peek()),
            style="create",
        )

    rename_snapshots = Table(
        "action",
        "source",
        "arrow",
        "dest",
        box=None,
        show_header=False,
        row_styles=["none", "dim"],
    )
    for rsi in actions.iter_rename_snapshot_intents():
        rename_snapshots.add_row(
            Text.from_markup(":pencil-emoji: rename snapshot:"),
            str(rsi.source.peek()),
            "->",
            str(rsi.target.peek()),
            style="modify",
        )

    create_backups = Table(
        "action", "source", box=None, show_header=False, row_styles=["none", "dim"]
    )
    for cbi in actions.iter_create_backup_intents():
        if cbi.snapshot.is_tbd():
            source = f"(new snapshot of {cbi.source.peek()})"
        else:
            source = str(cbi.snapshot.peek())
        create_backups.add_row(
            Text.from_markup(":cloud-emoji: upload backup:"), source, style="create"
        )

    delete_snapshots = Table(
        "action", "path", box=None, show_header=False, row_styles=["none", "dim"]
    )
    for dsi in actions.iter_delete_snapshot_intents():
        delete_snapshots.add_row(
            Text.from_markup(":skull: delete snapshot:"),
            str(dsi.path.peek()),
            style="delete",
        )

    delete_backups = Table(
        "action", "key", box=None, show_header=False, row_styles=["none", "dim"]
    )
    for dbi in actions.iter_delete_backup_intents():
        delete_backups.add_row(
            Text.from_markup(":skull: delete backup: "),
            str(dbi.key.peek()),
            style="delete",
        )

    return (
        create_snapshots,
        rename_snapshots,
        create_backups,
        delete_snapshots,
        delete_backups,
    )


def print_actions(*, console: Console, actions: Actions) -> None:
    """Prints a list of Actions to a Console."""
    if actions.empty():
        return

    console.print(Rule("actions to be taken"))

    tables = _make_action_tables(actions)

    for table in tables:
        if table.row_count:
            console.print(table)
            console.print()


NAME = "update"


if TYPE_CHECKING:

    class _Args(TypedDict, total=False):
        help: str
        description: str
        epilog: str


ARGS: _Args = {
    # shown in top-level help
    "help": "one-time update of snapshots and backups",
    # shown in subcommand help
    "description": "One-time update of snapshots and backups.",
    "epilog": "For detailed docs and usage, see https://github.com/sbrudenell/btrfs2s3",
}


def add_args(parser: argparse.ArgumentParser) -> None:
    """Add args for "btrfs2s3 update" to an ArgumentParser."""
    parser.add_argument("config_file", type=load_from_path)
    parser.add_argument(
        "--force", action="store_true", help="perform actions without prompting"
    )
    parser.add_argument(
        "--pretend",
        action="store_true",
        help="do not perform actions, just print a preview and exit",
    )


def command(*, console: Console, args: argparse.Namespace) -> int:
    """Implements "btrfs2s3 update"."""
    if not console.is_terminal and not (args.force or args.pretend):
        console.print("to run in unattended mode, use --force")
        return 1

    config = cast(Config, args.config_file)
    tzinfo = get_zoneinfo(config["timezone"])
    s3_remote = config["remotes"][0]["s3"]
    s3_endpoint = s3_remote.get("endpoint", {})

    sources = config["sources"]

    session = Session(
        region_name=s3_endpoint.get("region_name"),
        profile_name=s3_endpoint.get("profile_name"),
    )
    s3 = session.client(
        "s3",
        # https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/locking-api-versions.html
        # says that pinning api_version is a best practice
        api_version="2006-03-01",
        verify=s3_endpoint.get("verify"),
        endpoint_url=s3_endpoint.get("endpoint_url"),
    )
    policy = Policy(
        tzinfo=tzinfo,
        params=Params.parse(sources[0]["upload_to_remotes"][0]["preserve"]),
    )
    asmt = assess(
        snapshot_dir=Path(sources[0]["snapshots"]),
        sources=[Path(source["path"]) for source in sources],
        s3=s3,
        bucket=s3_remote["bucket"],
        policy=policy,
    )
    actions = Actions()
    assessment_to_actions(asmt, actions)

    if console.is_terminal:
        print_assessment(
            console=console,
            asmt=asmt,
            tzinfo=tzinfo,
            snapshot_dir=Path(sources[0]["snapshots"]),
            bucket=s3_remote["bucket"],
        )
        print_actions(console=console, actions=actions)

    if args.pretend:
        return 0

    if actions.empty():
        console.print("nothing to be done!")
        return 0

    if args.force or Confirm(console=console).ask("continue?"):
        actions.execute(
            s3,
            s3_remote["bucket"],
            pipe_through=sources[0]["upload_to_remotes"][0].get("pipe_through", []),
        )

    return 0
