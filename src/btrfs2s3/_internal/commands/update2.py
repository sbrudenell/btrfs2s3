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
from contextlib import ExitStack
from enum import auto
from enum import Enum
from functools import partial
from pathlib import Path
from typing import cast
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import arrow
from boto3.session import Session
from rich.box import HORIZONTALS
from rich.columns import Columns
from rich.console import Console
from rich.console import Group
from rich.filesize import decimal
from rich.highlighter import ISO8601Highlighter
from rich.markup import escape
from rich.prompt import Confirm
from rich.prompt import Prompt
from rich.table import Column
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

from btrfs2s3._internal.config import Config
from btrfs2s3._internal.config import load_from_path
from btrfs2s3._internal.cvar import TZINFO
from btrfs2s3._internal.cvar import use_tzinfo
from btrfs2s3._internal.piper import filter_pipe
from btrfs2s3._internal.planner import Actions
from btrfs2s3._internal.planner import assess
from btrfs2s3._internal.planner import AssessedBackup
from btrfs2s3._internal.planner import AssessedSnapshot
from btrfs2s3._internal.planner import Assessment
from btrfs2s3._internal.planner import assessment_to_actions
from btrfs2s3._internal.planner import ConfigTuple
from btrfs2s3._internal.planner import destroy_new_snapshots
from btrfs2s3._internal.planner import Remote
from btrfs2s3._internal.planner import SnapshotDir
from btrfs2s3._internal.planner import Source
from btrfs2s3._internal.preservation import Params
from btrfs2s3._internal.preservation import Policy
from btrfs2s3._internal.preservation import TS
from btrfs2s3._internal.resolver import Flags
from btrfs2s3._internal.resolver import KeepMeta
from btrfs2s3._internal.resolver import Reasons
from btrfs2s3._internal.time_span_describer import describe_time_span

if TYPE_CHECKING:
    import argparse
    from collections.abc import Collection
    from collections.abc import Iterable
    from collections.abc import Iterator
    from collections.abc import Sequence
    from typing import TypedDict


_iso8601_highlight = ISO8601Highlighter()


def _time_span_key(time_span: TS) -> tuple[float, float]:
    start, end = time_span
    return (start - end, start)


def _describe_time_spans(time_spans: Iterable[TS]) -> Text:
    return describe_time_span(sorted(time_spans, key=_time_span_key)[0], bounds="[]")


def _describe_preserve(keep_meta: KeepMeta) -> Text:
    if keep_meta.reasons & Reasons.Preserved:
        return _describe_time_spans(keep_meta.time_spans)
    if keep_meta.reasons & Reasons.MostRecent:
        return Text("<most recent>", style="keep")
    if keep_meta.reasons & Reasons.SendAncestor:
        return Text("<ancestor>", style="keep")
    if keep_meta.reasons:  # pragma: no cover
        return Text("<keep!>", style="keep")
    return Text("<not keeping>", style="not_keeping")


def _describe_time(time: float) -> Text:
    return _iso8601_highlight(
        arrow.get(time, tzinfo=TZINFO.get()).format("YYYY-MM-DDTHH:mm:ss")
    )


def _keep_emoji(keep_meta: KeepMeta) -> Text:
    if keep_meta.flags & Flags.New:
        return Text.from_markup(":sparkles:")
    if not keep_meta.reasons:
        return Text.from_markup(":skull:")
    return Text()


def _make_snapshots_table(snapshots: Collection[AssessedSnapshot]) -> Table:
    table = Table(
        Column("path"),
        Column("ctime"),
        Column("ctransid", style="ctransid"),
        Column("preserve"),
        title="snapshots",
        title_justify="left",
        row_styles=["none", "dim"],
        box=HORIZONTALS,
    )
    snapshots = sorted(
        snapshots, key=lambda k: (k.snapshot_dir.path, k.source.path, k.info.ctransid)
    )
    for snap in snapshots:
        path = (
            Text(style="" if snap.meta.reasons else "delete")
            .append(_keep_emoji(snap.meta))
            .append(escape(str(snap.snapshot_dir.get_path(snap.info.id))))
        )
        ctime = _describe_time(snap.info.ctime)
        ctransid = str(snap.info.ctransid)
        preserve = _describe_preserve(snap.meta)
        table.add_row(path, ctime, ctransid, preserve)
    return table


def _make_new_snapshots_table(asmt: Assessment) -> Table | None:
    new_snaps = sorted(
        (s for s in asmt.snapshots.values() if s.meta.flags & Flags.New),
        key=lambda s: (s.source.path, s.snapshot_dir.get_path(s.info.id)),
    )
    if not new_snaps:
        return None
    table = Table(
        Column("source"),
        Column("path"),
        title=":sparkles:newly-created snapshots:sparkles:",
        title_justify="left",
        row_styles=["none", "dim"],
        box=HORIZONTALS,
    )
    for new_snap in new_snaps:
        source = escape(str(new_snap.source.path))
        path = escape(str(new_snap.snapshot_dir.get_path(new_snap.info.id)))
        table.add_row(source, path)
    return table


def _backup_key(backup: AssessedBackup) -> tuple[Path, int, float]:
    return (backup.source.path, backup.info.ctransid, backup.info.ctime)


def _make_backups_tree(backups: Collection[AssessedBackup]) -> Tree:
    uuid_to_children: dict[tuple[Remote, bytes], list[AssessedBackup]] = defaultdict(
        list
    )
    remote_full_backups: dict[Remote, list[AssessedBackup]] = defaultdict(list)
    for backup in backups:
        send_parent_uuid = backup.info.send_parent_uuid
        if send_parent_uuid:
            uuid_to_children[(backup.remote, send_parent_uuid)].append(backup)
        else:
            remote_full_backups[backup.remote].append(backup)

    def add_backup_node(parent: Tree, backup: AssessedBackup) -> None:
        key = Text(no_wrap=True, overflow="ellipsis").append(
            _keep_emoji(backup.meta).append(
                escape(backup.key), style="key" if backup.meta.reasons else "delete"
            )
        )
        stats = [
            _describe_preserve(backup.meta),
            _describe_time(backup.info.ctime),
            Text(str(backup.info.ctransid), style="ctransid"),
        ]
        size = backup.stat and backup.stat.size
        if size is not None:
            stats.append(Text(decimal(size), style="cost"))
        storage_class = backup.stat and backup.stat.storage_class
        if storage_class:
            stats.append(Text(storage_class, style="cost"))
        info = Columns(stats)
        node = parent.add(Group(key, info))
        children = uuid_to_children.get((backup.remote, backup.info.uuid), ())
        for child in sorted(children, key=_backup_key):
            add_backup_node(node, child)

    tree = Tree("backups")
    for remote in sorted(remote_full_backups.keys(), key=lambda r: r.name):
        remote_tree = tree.add(
            Text.from_markup(":cloud-emoji:").append(escape(remote.name))
        )
        for backup in sorted(remote_full_backups[remote], key=_backup_key):
            add_backup_node(remote_tree, backup)
    return tree


def _make_action_tables(actions: Actions) -> Sequence[Table]:
    rename_snapshots = Table(
        Column("action"),
        Column("source"),
        Column("arrow"),
        Column("dest"),
        box=None,
        show_header=False,
        row_styles=["none", "dim"],
    )
    for rename_args in actions.rename_snapshots:
        rename_snapshots.add_row(
            Text.from_markup(":pencil-emoji: rename:"),
            escape(str(rename_args.snapshot_dir.get_path(rename_args.info.id))),
            "->",
            escape(rename_args.target_name),
            style="modify",
        )

    upload_backups = Table(
        Column("action"),
        Column("source"),
        Column("arrow"),
        Column("remote"),
        Column("key"),
        box=None,
        show_header=False,
        row_styles=["none", "dim"],
    )
    for upload_args in actions.upload_backups:
        upload_backups.add_row(
            Text.from_markup(":cloud-emoji: upload:"),
            escape(str(upload_args.snapshot_dir.get_path(upload_args.info.id))),
            "->",
            Text.from_markup(":cloud-emoji:").append(escape(upload_args.remote.name)),
            escape(upload_args.key),
            style="create",
        )

    destroy_snapshots = Table(
        Column("action"),
        Column("path"),
        box=None,
        show_header=False,
        row_styles=["none", "dim"],
    )
    for destroy_args in actions.destroy_snapshots:
        destroy_snapshots.add_row(
            Text.from_markup(":skull: destroy:"),
            escape(str(destroy_args.snapshot_dir.get_path(destroy_args.info.id))),
            style="delete",
        )

    delete_backups = Table(
        Column("action"),
        Column("remote"),
        Column("key"),
        box=None,
        show_header=False,
        row_styles=["none", "dim"],
    )
    for delete_args in actions.delete_backups:
        delete_backups.add_row(
            Text.from_markup(":skull: delete:"),
            Text.from_markup(":cloud-emoji:").append(escape(delete_args.remote.name)),
            escape(delete_args.key),
            style="delete",
        )

    return (rename_snapshots, upload_backups, destroy_snapshots, delete_backups)


def print_plan(*, console: Console, assessment: Assessment, actions: Actions) -> None:
    console.print(_make_snapshots_table(assessment.snapshots.values()))
    console.print()
    console.print(_make_backups_tree(assessment.backups.values()))
    console.print()

    new_snapshots_table = _make_new_snapshots_table(assessment)
    if new_snapshots_table:
        console.print(new_snapshots_table)
        console.print()

    action_tables = _make_action_tables(actions)
    if any(t.row_count for t in action_tables):
        console.print("actions to take:")
        for table in action_tables:
            if table.row_count:
                console.print(table)
        console.print()
    else:
        console.print("nothing to be done!")
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


class _Action(Enum):
    Execute = auto()
    Undo = auto()


class _Updater:
    def __init__(self, config: Config, *, force: bool) -> None:
        self._config = config
        self._force = force

        self._tzinfo = ZoneInfo(config["timezone"])
        self._id_to_remote_cfg = {cfg["id"]: cfg for cfg in config["remotes"]}
        self._id_to_remote: dict[str, Remote] = {}
        self._path_to_snapshot_dir: dict[Path, SnapshotDir] = {}
        # we open resources for each Source and SnapshotDir, and we discover
        # them by iterating over config. the ExitStack helps us manage these
        # resources.
        self._stack = ExitStack()

    def _get_snapshot_dir(self, path: Path) -> SnapshotDir:
        if path not in self._path_to_snapshot_dir:
            self._path_to_snapshot_dir[path] = self._stack.enter_context(
                SnapshotDir.create(path)
            )
        return self._path_to_snapshot_dir[path]

    def _get_remote(self, remote_id: str) -> Remote:
        if remote_id not in self._id_to_remote:
            s3_cfg = self._id_to_remote_cfg[remote_id]["s3"]
            s3_endpoint = s3_cfg.get("endpoint", {})
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
            self._id_to_remote[remote_id] = Remote.create(
                name=remote_id, s3=s3, bucket=s3_cfg["bucket"]
            )
        return self._id_to_remote[remote_id]

    def _iter_config_tuples(self) -> Iterator[ConfigTuple]:
        for source_cfg in self._config["sources"]:
            source = self._stack.enter_context(Source.create(Path(source_cfg["path"])))
            snapshot_dir = self._get_snapshot_dir(Path(source_cfg["snapshots"]))
            for upload_to_remote in source_cfg["upload_to_remotes"]:
                remote = self._get_remote(upload_to_remote["id"])
                policy = Policy(params=Params.parse(upload_to_remote["preserve"]))
                create_pipe = partial(
                    filter_pipe, upload_to_remote.get("pipe_through", [])
                )
                yield ConfigTuple(
                    source=source,
                    snapshot_dir=snapshot_dir,
                    remote=remote,
                    policy=policy,
                    create_pipe=create_pipe,
                )

    def _check_action(
        self, *, console: Console, actions: Actions, assessment: Assessment
    ) -> _Action | None:
        have_new_snapshots = any(
            s.meta.flags & Flags.New for s in assessment.snapshots.values()
        )
        if not actions.any_actions() and not have_new_snapshots:
            return None

        if self._force:
            return _Action.Execute

        if have_new_snapshots:
            console.print("we proactively created some read-only snapshots.")
            console.print("they can be deleted if desired.")
            console.print()
            choice = Prompt.ask(
                "continue? (y/n) or (u)ndo created snapshots?",
                choices=["y", "n", "u"],
                console=console,
            )
            if choice == "y":
                return _Action.Execute
            if choice == "u":
                return _Action.Undo
            return None

        if Confirm.ask("continue?", console=console):
            return _Action.Execute

        return None

    def update(self, console: Console) -> None:
        with self._stack:
            self._stack.enter_context(use_tzinfo(self._tzinfo))

            cfg_tuples = list(self._iter_config_tuples())

            asmt = assess(*cfg_tuples)
            actions = assessment_to_actions(asmt)

            if console.is_terminal:
                print_plan(console=console, assessment=asmt, actions=actions)

            action = self._check_action(
                console=console, actions=actions, assessment=asmt
            )

            if action == _Action.Execute:
                actions.execute()
            elif action == _Action.Undo:
                destroy_new_snapshots(asmt)


def command(*, console: Console, args: argparse.Namespace) -> int:
    """Implements "btrfs2s3 update"."""
    if not console.is_terminal and not args.force:
        console.print("to run in unattended mode, use --force")
        return 1

    _Updater(cast(Config, args.config_file), force=args.force).update(console)

    return 0
