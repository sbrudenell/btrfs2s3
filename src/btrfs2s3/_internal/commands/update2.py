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
from rich.highlighter import ISO8601Highlighter
from rich.prompt import Confirm
from rich.prompt import Prompt
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

from btrfs2s3._internal.config import Config
from btrfs2s3._internal.config import load_from_path
from btrfs2s3._internal.piper import filter_pipe
from btrfs2s3._internal.planner import Plan
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
from btrfs2s3._internal.util import TZINFO
from btrfs2s3._internal.util import use_tzinfo

if TYPE_CHECKING:
    import argparse
    from collections.abc import Iterable
    from collections.abc import Sequence
    from typing import TypedDict

    from btrfs2s3._internal.config import SourceConfig
    from btrfs2s3._internal.planner import KeepBackupArgs
    from btrfs2s3._internal.planner import Update

_iso8601_highlight = ISO8601Highlighter()


def _time_span_key(time_span: TS) -> tuple[float, float]:
    start, end = time_span
    return (start - end, start)


def _describe_time_spans(time_spans: Iterable[TS]) -> Text:
    return describe_time_span(
        sorted(time_spans, key=_time_span_key)[0], TZINFO.get(), bounds="[]"
    )


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


def _make_snapshots_table(plan: Plan) -> Table:
    table = Table(
        title="snapshots",
        title_justify="left",
        row_styles=["none", "dim"],
        box=HORIZONTALS,
    )
    table.add_column("path", no_wrap=True)
    table.add_column("ctime", no_wrap=True)
    table.add_column("ctransid", style="ctransid", no_wrap=True)
    table.add_column("preserve", no_wrap=True)
    keep_snapshots = sorted(
        plan.keep_snapshots.values(),
        key=lambda k: (k.snapshot_dir.path, k.source.path, k.snapshot.ctransid),
    )
    for keep_snap in keep_snapshots:
        path = _keep_emoji(keep_snap.meta).append(
            str(keep_snap.snapshot_dir.get_path(keep_snap.snapshot.id))
        )
        ctime = _describe_time(keep_snap.snapshot.ctime)
        ctransid = str(keep_snap.snapshot.ctransid)
        preserve = _describe_preserve(keep_snap.meta)
        table.add_row(path, ctime, ctransid, preserve)
    return table


def _make_created_snapshots_table(plan: Plan) -> Table:
    table = Table(
        title=":sparkles:newly-created snapshots:sparkles:",
        title_justify="left",
        row_styles=["none", "dim"],
        box=HORIZONTALS,
    )
    table.add_column("source", no_wrap=True)
    table.add_column("path", no_wrap=True)
    created_snapshots = sorted(
        plan.created_snapshots.values(),
        key=lambda c: (c.source.path, c.snapshot_dir.get_path(c.snapshot.id)),
    )
    for created_snapshot in created_snapshots:
        source = str(created_snapshot.source.path)
        path = str(created_snapshot.snapshot_dir.get_path(created_snapshot.snapshot.id))
        table.add_row(source, path)
    return table


def _backup_key(backup: KeepBackupArgs) -> tuple[Path, int, float]:
    return (backup.source.path, backup.info.ctransid, backup.info.ctime)


def _make_backups_tree(plan: Plan) -> Tree:
    uuid_to_children: dict[bytes, list[KeepBackupArgs]] = defaultdict(list)
    remote_full_backups: dict[str, list[KeepBackupArgs]] = defaultdict(list)
    for backup in plan.keep_backups.values():
        parent = backup.info.send_parent_uuid
        if parent:
            uuid_to_children[parent].append(backup)
        else:
            remote_full_backups[backup.remote.name].append(backup)

    def add_backup_node(parent: Tree, backup: KeepBackupArgs) -> None:
        key = _keep_emoji(backup.meta).append(backup.key, style="key")
        info = Columns(
            (
                _describe_preserve(backup.meta),
                _describe_time(backup.info.ctime),
                Text(str(backup.info.ctransid), style="ctransid"),
            )
        )
        node = parent.add(Group(key, info))
        children = uuid_to_children.get(backup.info.uuid, ())
        for child in sorted(children, key=_backup_key):
            add_backup_node(node, child)

    tree = Tree("backups")
    for name in sorted(remote_full_backups.keys()):
        remote_tree = tree.add(f":cloud: {name}")
        for backup in sorted(remote_full_backups[name], key=_backup_key):
            add_backup_node(remote_tree, backup)
    return tree


def _make_action_tables(plan: Plan) -> Sequence[Table]:
    rename_snapshots = Table(
        "action",
        "source",
        "arrow",
        "dest",
        box=None,
        show_header=False,
        row_styles=["none", "dim"],
    )
    for rename_args in plan.rename_snapshots:
        rename_snapshots.add_row(
            Text.from_markup(":pencil-emoji: rename:"),
            str(rename_args.snapshot_dir.get_path(rename_args.snapshot.id)),
            "->",
            rename_args.target_name,
            style="modify",
        )

    upload_backups = Table(
        "action",
        "source",
        "arrow",
        "remote",
        box=None,
        show_header=False,
        row_styles=["none", "dim"],
    )
    for upload_args in plan.upload_backups:
        upload_backups.add_row(
            Text.from_markup(":cloud-emoji: upload:"),
            str(upload_args.snapshot_dir.get_path(upload_args.snapshot.id)),
            "->",
            upload_args.remote.name,
            style="create",
        )

    destroy_snapshots = Table(
        "action", "path", box=None, show_header=False, row_styles=["none", "dim"]
    )
    for destroy_args in plan.destroy_snapshots:
        destroy_snapshots.add_row(
            Text.from_markup(":skull: destroy:"),
            str(destroy_args.snapshot_dir.get_path(destroy_args.snapshot.id)),
            style="delete",
        )

    delete_backups = Table(
        "action", "key", box=None, show_header=False, row_styles=["none", "dim"]
    )
    for delete_args in plan.delete_backups:
        delete_backups.add_row(
            Text.from_markup(":skull: delete: "),
            f"[{delete_args.remote.name}] {delete_args.key}",
            style="delete",
        )

    return (rename_snapshots, upload_backups, destroy_snapshots, delete_backups)


def print_plan(*, console: Console, plan: Plan) -> None:
    console.print(_make_snapshots_table(plan))
    console.print()
    console.print(_make_backups_tree(plan))
    console.print()

    if plan.created_snapshots:
        console.print(_make_created_snapshots_table(plan))
        console.print()

    action_tables = _make_action_tables(plan)
    if any(t.row_count for t in action_tables):
        console.print("actions to take:")
        for table in action_tables:
            if table.row_count:
                console.print(table)
                console.print()
    else:
        console.print("nothing to be done!")
        console.print()


NAME = "update2"


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


class Action(Enum):
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
        self._plan = Plan.create()
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

    def _update_source(self, source_cfg: SourceConfig, update: Update) -> None:
        source = self._stack.enter_context(Source.create(Path(source_cfg["path"])))
        snapshot_dir = self._get_snapshot_dir(Path(source_cfg["snapshots"]))

        for upload_to_remote in source_cfg["upload_to_remotes"]:
            remote = self._get_remote(upload_to_remote["id"])
            policy = Policy(
                tzinfo=self._tzinfo, params=Params.parse(upload_to_remote["preserve"])
            )
            update(
                source=source,
                snapshot_dir=snapshot_dir,
                remote=remote,
                policy=policy,
                create_pipe=partial(
                    filter_pipe, upload_to_remote.get("pipe_through", [])
                ),
            )

    def _check_action(self, console: Console) -> Action | None:
        if not self._plan.any_actions() and not self._plan.created_snapshots:
            return None

        if self._force:
            return Action.Execute

        if self._plan.created_snapshots:
            console.print("we proactively created some read-only snapshots.")
            console.print("they can be deleted if desired.")
            console.print()
            choice = Prompt.ask(
                "continue? (y/n) or (u)ndo created snapshots?",
                choices=["y", "n", "u"],
                console=console,
            )
            if choice == "y":
                return Action.Execute
            if choice == "u":
                return Action.Undo
            return None

        if Confirm.ask("continue?", console=console):
            return Action.Execute
        return None

    def update(self, console: Console) -> None:
        with self._stack:
            self._stack.enter_context(use_tzinfo(self._tzinfo))

            with self._plan.update() as update:
                for source_cfg in self._config["sources"]:
                    self._update_source(source_cfg, update)

            if console.is_terminal:
                print_plan(console=console, plan=self._plan)

            action = self._check_action(console)
            if action == Action.Execute:
                self._plan.execute()
            elif action == Action.Undo:
                self._plan.undo_created_snapshots()


def command(*, console: Console, args: argparse.Namespace) -> int:
    """Implements "btrfs2s3 update"."""
    if not console.is_terminal and not args.force:
        console.print("to run in unattended mode, use --force")
        return 1

    _Updater(cast(Config, args.config_file), force=args.force).update(console)

    return 0
