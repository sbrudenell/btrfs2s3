"""The global rich text console."""

from __future__ import annotations

from rich.console import Console
from rich.style import Style
from rich.theme import Theme

STYLE_KEY = Style.parse("green")
STYLE_KEEP = Style.parse("green")
STYLE_NOT_KEEPING = Style.parse("bold bright_red")
STYLE_CTRANSID = Style.parse("cyan")

STYLE_CREATE = Style.parse("green")
STYLE_MODIFY = Style.parse("green")
STYLE_DELETE = Style.parse("red")

THEME = Theme(
    {
        "key": STYLE_KEY,
        "keep": STYLE_KEEP,
        "not_keeping": STYLE_NOT_KEEPING,
        "ctransid": STYLE_CTRANSID,
        "create": STYLE_CREATE,
        "modify": STYLE_MODIFY,
        "delete": STYLE_DELETE,
    }
)

CONSOLE = Console(theme=THEME)
"""The global rich text console."""
