"""Compatibility module for zoneinfo."""

import sys

if sys.version_info >= (3, 9):  # pragma: >=3.9 cover
    from zoneinfo import ZoneInfo
else:  # pragma: <3.9 cover
    from backports.zoneinfo import ZoneInfo


# This function is just syntactic sugar to call the ZoneInfo constructor with
# appropriate lint markers
def get_zoneinfo(key: str) -> ZoneInfo:
    """Return a ZoneInfo object from a string timezone name."""
    if sys.version_info >= (3, 9):  # pragma: >=3.9 cover
        return ZoneInfo(key)
    else:  # noqa: RET505 # pragma: <3.9 cover
        # https://github.com/pganssle/zoneinfo/issues/125
        # Unfortunately the workaround in the linked issue does not work with
        # mypy --strict (I get: Call to untyped function "ZoneInfo" in typed
        # context)

        # Also mypy requires if-return-else to understand the version
        # condition, which thus requires the RET505 block. Yakkity yak
        return ZoneInfo(key)  # type: ignore[abstract]
