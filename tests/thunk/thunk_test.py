from __future__ import annotations

from btrfs2s3.thunk import StillTBDError
from btrfs2s3.thunk import TBD
from btrfs2s3.thunk import Thunk
import pytest
from typing_extensions import assert_type


def test_immediate() -> None:
    t1 = Thunk("a")
    assert not t1.is_tbd()
    peek1 = t1.peek()
    assert peek1 is not TBD
    assert_type(peek1, str)
    assert peek1 == "a"
    assert t1.check() == "a"
    assert_type(t1(), str)
    assert t1() == "a"

    t2 = Thunk(t1)
    assert not t2.is_tbd()
    peek2 = t2.peek()
    assert peek2 is not TBD
    assert_type(peek2, str)
    assert peek2 == "a"
    assert t2.check() == "a"
    assert_type(t2(), str)
    assert t2() == "a"


def test_lazy() -> None:
    t1 = Thunk(lambda: "a")
    assert t1.is_tbd()
    peek1 = t1.peek()
    assert peek1 is TBD
    with pytest.raises(StillTBDError):
        t1.check()

    t2 = Thunk(t1)
    assert t1() == "a"
    assert_type(t1(), str)
    peek1 = t1.peek()
    assert not t1.is_tbd()
    assert peek1 is not TBD
    assert_type(peek1, str)
    assert peek1 == "a"

    assert t2.is_tbd()
    peek2 = t2.peek()
    assert peek2 is TBD
    with pytest.raises(StillTBDError):
        t2.check()
    assert t2() == "a"
    assert_type(t2(), str)


def test_repr() -> None:
    t_a = Thunk("a")
    assert repr(t_a) == "Thunk('a')"
    t_b = Thunk(lambda: "b")
    assert repr(t_b) == "Thunk(TBD)"
    t_b()
    assert repr(t_b) == "Thunk('b')"


def test_lt() -> None:
    t_a = Thunk("a")
    t_b = Thunk("b")
    assert t_a < t_b
    assert t_b > t_a

    t_tbd0 = Thunk(lambda: 0)
    t_tbd1 = Thunk(lambda: 1)
    assert (t_tbd0 < t_tbd1) != (t_tbd1 < t_tbd0)
    t_tbd1()
    assert t_tbd1 < t_tbd0
    assert t_tbd0 > t_tbd1
    t_tbd0()
    assert t_tbd0 < t_tbd1


def test_order() -> None:
    t_a = Thunk("a")
    t_b = Thunk("b")
    t_tbd = Thunk(lambda: "0")

    # When all values are the same type, they should obey their natural order
    assert sorted([t_b, t_a]) == [t_a, t_b]
    # When some thunks are TBD, they should appear at the end
    assert sorted([t_tbd, t_b, t_a]) == [t_a, t_b, t_tbd]
    t_tbd()
    assert sorted([t_tbd, t_b, t_a]) == [t_tbd, t_a, t_b]
