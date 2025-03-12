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

"""Main code for the btrfs2s3 cli."""

from __future__ import annotations

import argparse
import logging
import sys
from typing import TYPE_CHECKING

from rich.logging import RichHandler

from btrfs2s3._internal.commands import update2
from btrfs2s3._internal.console import CONSOLE

if TYPE_CHECKING:
    from collections.abc import Sequence

    from rich.console import Console


_DESCRIPTION = """
btrfs2s3 maintains a tree of incremental backups in cloud storage.
"""

_EPILOG = """
For detailed docs and usage, see https://github.com/sbrudenell/btrfs2s3
"""


def main(*, console: Console | None = None, argv: Sequence[str] | None = None) -> int:
    """Main function for btrfs2s3."""
    console = console if console else CONSOLE
    argv = argv if argv is not None else sys.argv[1:]
    parser = argparse.ArgumentParser(description=_DESCRIPTION, epilog=_EPILOG)

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="enable debug logs"
    )

    subparsers = parser.add_subparsers(
        dest="command", required=True, help="subcommand (required)"
    )

    update2.add_args(subparsers.add_parser(update2.NAME, **update2.ARGS))

    args = parser.parse_args(argv)

    logging.basicConfig(
        level="NOTSET" if args.verbose else logging.INFO,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console)],
    )

    if args.command == update2.NAME:
        return update2.command(console=console, args=args)
    raise NotImplementedError


if __name__ == "__main__":
    raise SystemExit(main())
