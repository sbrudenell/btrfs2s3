from __future__ import annotations

from btrfs2s3.main import main
import pytest


def test_help(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        main(["btrfs2s3", "--help"])
    out, err = capsys.readouterr()
    assert out == ""
    assert "usage: " in err
