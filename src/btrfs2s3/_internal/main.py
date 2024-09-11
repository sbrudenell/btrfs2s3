"""Main code for the btrfs2s3 cli."""

from __future__ import annotations

import argparse
import logging
import sys
from typing import TYPE_CHECKING

from rich.logging import RichHandler

from btrfs2s3._internal.commands import update
from btrfs2s3._internal.console import CONSOLE

if TYPE_CHECKING:
    from typing import Sequence

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

    update.add_args(subparsers.add_parser(update.NAME, **update.ARGS))

    args = parser.parse_args(argv)

    logging.basicConfig(
        level="NOTSET" if args.verbose else logging.INFO,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console)],
    )

    if args.command == update.NAME:
        return update.command(console=console, args=args)
    raise NotImplementedError


if __name__ == "__main__":
    raise SystemExit(main())
