"""Main code for the btrfs2s3 cli."""

from __future__ import annotations

import argparse
import logging
import sys
from typing import TYPE_CHECKING

from btrfs2s3.commands import run

if TYPE_CHECKING:
    from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    """Main function for btrfs2s3."""
    argv = argv if argv is not None else sys.argv[1:]
    parser = argparse.ArgumentParser()

    parser.add_argument("-v", "--verbose", action="store_true")

    subparsers = parser.add_subparsers(dest="command", required=True)

    run.add_args(subparsers.add_parser(run.NAME))

    args = parser.parse_args(argv)

    logging.basicConfig(
        stream=sys.stderr, level=logging.DEBUG if args.verbose else logging.INFO
    )

    if args.command == run.NAME:
        return run.command(args)
    raise NotImplementedError


if __name__ == "__main__":
    raise SystemExit(main())
