"""Main code for the btrfs2s3 cli."""

from __future__ import annotations

import argparse
import logging
import sys
from typing import TYPE_CHECKING

from rich.logging import RichHandler

from btrfs2s3.commands import update
from btrfs2s3.console import CONSOLE

if TYPE_CHECKING:
    from typing import Sequence

    from rich.console import Console


def main(*, console: Console | None = None, argv: Sequence[str] | None = None) -> int:
    """Main function for btrfs2s3."""
    console = console if console else CONSOLE
    argv = argv if argv is not None else sys.argv[1:]
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest="command", required=True)

    update.add_args(subparsers.add_parser(update.NAME))

    args = parser.parse_args(argv)

    logging.basicConfig(
        level="NOTSET",
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console)],
    )

    if args.command == update.NAME:
        return update.command(console=console, args=args)
    raise NotImplementedError


if __name__ == "__main__":
    raise SystemExit(main())
