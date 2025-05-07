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

from contextlib import ExitStack
from enum import auto
from enum import Enum
from functools import partial
from pathlib import Path
from typing import cast
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from boto3.session import Session
from rich.prompt import Confirm
from rich.prompt import Prompt

from btrfs2s3._internal.commands.printing import print_plan
from btrfs2s3._internal.config import load_from_path
from btrfs2s3._internal.cvar import use_tzinfo
from btrfs2s3._internal.piper import filter_pipe
from btrfs2s3._internal.planner import Actions
from btrfs2s3._internal.planner import assess
from btrfs2s3._internal.planner import Assessment
from btrfs2s3._internal.planner import assessment_to_actions
from btrfs2s3._internal.planner import ConfigTuple
from btrfs2s3._internal.planner import destroy_new_snapshots
from btrfs2s3._internal.planner import Remote
from btrfs2s3._internal.planner import SnapshotDir
from btrfs2s3._internal.planner import Source
from btrfs2s3._internal.preservation import Params
from btrfs2s3._internal.preservation import Policy
from btrfs2s3._internal.resolver import Flags

if TYPE_CHECKING:
    import argparse
    from collections.abc import Iterator
    from typing import TypedDict

    from rich.console import Console

    from btrfs2s3._internal.config import Config


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

    _Updater(cast("Config", args.config_file), force=args.force).update(console)

    return 0
