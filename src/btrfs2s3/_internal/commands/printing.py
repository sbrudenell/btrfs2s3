# btrfs2s3 - maintains a tree of differential backups in object storage.
#
# Copyright (C) 2025 Steven Brudenell and other contributors.
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

"""Code for printing to console."""

from __future__ import annotations

from collections import defaultdict
from time import time
from typing import TYPE_CHECKING

import arrow
from rich.box import HORIZONTALS
from rich.columns import Columns
from rich.console import Console
from rich.console import Group
from rich.filesize import decimal
from rich.highlighter import ISO8601Highlighter
from rich.markup import escape
from rich.table import Column
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

from btrfs2s3._internal.cvar import TZINFO
from btrfs2s3._internal.resolver import Flags
from btrfs2s3._internal.resolver import KeepMeta
from btrfs2s3._internal.resolver import Reasons
from btrfs2s3._internal.s3 import Timespan
from btrfs2s3._internal.time_span_describer import describe_time_span

if TYPE_CHECKING:
    from collections.abc import Collection
    from collections.abc import Iterable
    from collections.abc import Sequence
    from pathlib import Path

    from btrfs2s3._internal.planner import Actions
    from btrfs2s3._internal.planner import AssessedBackup
    from btrfs2s3._internal.planner import AssessedSnapshot
    from btrfs2s3._internal.planner import Assessment
    from btrfs2s3._internal.planner import Remote
    from btrfs2s3._internal.preservation import TS


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
    now = time()
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
        if backup.remote.costs and storage_class and size:
            assert list(backup.remote.costs.billing_period.values()) == [1]
            billing_timespan = Timespan(
                *arrow.get(now, tzinfo=backup.remote.costs.tzinfo).span(
                    next(iter(backup.remote.costs.billing_period)), bounds="[]"
                )
            )
            cost = backup.remote.costs.get_storage_cost(
                size=size, storage_class=storage_class, timespan=billing_timespan
            )
            stats.append(
                Text(f"{cost:.2f}", style="cost").append(
                    Text(f"/{backup.remote.costs.billing_period}")
                )
            )
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
