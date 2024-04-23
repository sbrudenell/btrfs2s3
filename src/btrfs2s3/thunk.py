from __future__ import annotations

import enum
from functools import total_ordering
from typing import Callable
from typing import Generic
from typing import Literal
from typing import overload
from typing import Protocol
from typing import runtime_checkable
from typing import TypeAlias
from typing import TypeVar


class Error(Exception):
    pass


class StillTBDError(Error):
    pass


_T = TypeVar("_T")
_T_contra = TypeVar("_T_contra", contravariant=True)


@runtime_checkable
class _SupportsLT(Protocol[_T_contra]):
    def __lt__(self, other: _T_contra) -> bool: ...


class _TbdType(enum.Enum):
    TBD = enum.auto()


TBD = _TbdType.TBD


@total_ordering
class Thunk(Generic[_T]):
    _value: _T | Literal[_TbdType.TBD]
    _get_value: Callable[[], _T]

    # Without these overloads, mypy can't infer the type of Thunk(other_thunk),
    # presumably it can't figure out if this should match __init__(_T) or
    # __init__(Thunk[_T]).
    @overload
    def __init__(self, other: Thunk[_T]) -> None: ...
    @overload
    def __init__(self, other: Callable[[], _T]) -> None: ...
    @overload
    def __init__(self, other: _T) -> None: ...
    def __init__(self, other: Thunk[_T] | Callable[[], _T] | _T) -> None:
        if isinstance(other, Thunk):
            self._get_value = other._get_value  # noqa: SLF001
            self._value = other._value  # noqa: SLF001
        elif callable(other):
            self._get_value = other
            self._value = TBD
        else:
            self._get_value = lambda: other  # pragma: no cover
            self._value = other

    def peek(self) -> _T | Literal[_TbdType.TBD]:
        return self._value

    def check(self) -> _T:
        value = self._value
        if value is TBD:
            raise StillTBDError
        return value

    def is_tbd(self) -> bool:
        return self._value is TBD

    def __call__(self) -> _T:
        if self._value is TBD:
            self._value = self._get_value()
        return self._value

    def __repr__(self) -> str:
        value = self._value
        if value is TBD:
            return "Thunk(TBD)"
        return f"Thunk({value!r})"

    def __lt__(self, other: Thunk[_T]) -> bool:
        value = self._value
        othervalue = other._value
        if value is not TBD:
            if othervalue is not TBD:
                if not isinstance(value, _SupportsLT):
                    return NotImplemented
                return value.__lt__(othervalue)
            return True
        if othervalue is not TBD:
            return False
        return id(self) < id(other)


ThunkArg: TypeAlias = Thunk[_T] | _T | Callable[[], _T]
