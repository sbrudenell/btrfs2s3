from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from btrfs2s3._internal.action import Actions
from btrfs2s3._internal.commands.update import print_actions
import pytest

if TYPE_CHECKING:
    from rich.console import Console


@pytest.mark.parametrize(
    (
        "n_create_snapshots",
        "n_rename_snapshots",
        "n_create_backups",
        "n_delete_snapshots",
        "n_delete_backups",
    ),
    [(0, 0, 0, 0, 0), (3, 3, 3, 3, 3)],
)
def test_print_actions(
    goldifyconsole: Console,
    n_create_snapshots: int,
    n_rename_snapshots: int,
    n_create_backups: int,
    n_delete_snapshots: int,
    n_delete_backups: int,
) -> None:
    actions = Actions()
    for i in range(n_create_snapshots):
        actions.create_snapshot(
            source=Path(f"/path/to/source{i}"), path=Path(f"/path/to/snapshot{i}")
        )
    for i in range(n_rename_snapshots):
        actions.rename_snapshot(
            source=Path(f"/path/to/source{i}"), target=Path(f"/path/to/target{i}")
        )
    for i in range(n_create_backups):
        actions.create_backup(
            source=Path(f"/path/to/source{i}"),
            snapshot=Path(f"/path/to/snapshot{i}"),
            key=f"key{i}",
            send_parent=Path(f"/path/to/send/parent{i}"),
        )
    for i in range(n_delete_snapshots):
        actions.delete_snapshot(Path(f"/path/to/snapshot{i}"))
    for i in range(n_delete_backups):
        actions.delete_backup(f"key{i}")

    print_actions(console=goldifyconsole, actions=actions)
