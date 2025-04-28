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

"""The global rich text console."""

from __future__ import annotations

from rich.console import Console
from rich.style import Style
from rich.theme import Theme

STYLE_KEY = Style.parse("green")
STYLE_KEEP = Style.parse("green")
STYLE_NOT_KEEPING = Style.parse("bold bright_red")
STYLE_CTRANSID = Style.parse("cyan")

STYLE_COST = Style.parse("yellow")

STYLE_CREATE = Style.parse("green")
STYLE_MODIFY = Style.parse("green")
STYLE_DELETE = Style.parse("red")

THEME = Theme(
    {
        "key": STYLE_KEY,
        "keep": STYLE_KEEP,
        "not_keeping": STYLE_NOT_KEEPING,
        "ctransid": STYLE_CTRANSID,
        "cost": STYLE_COST,
        "create": STYLE_CREATE,
        "modify": STYLE_MODIFY,
        "delete": STYLE_DELETE,
    }
)

CONSOLE = Console(theme=THEME)
"""The global rich text console."""
