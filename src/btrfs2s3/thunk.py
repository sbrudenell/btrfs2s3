"""A Thunk wraps a value that might be lazily evaluated."""

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
    """The top-level class for errors produced by this module."""


class StillTBDError(Error):
    """Thunk.check() was called, but the thunk hasn't been evaluated yet."""


_T = TypeVar("_T")
_T_contra = TypeVar("_T_contra", contravariant=True)


@runtime_checkable
class _SupportsLT(Protocol[_T_contra]):
    def __lt__(self, other: _T_contra) -> bool: ...


class _TbdType(enum.Enum):
    TBD = enum.auto()


TBD = _TbdType.TBD
"""A sentinel value, meaning the real value hasn't been determined yet."""


@total_ordering
class Thunk(Generic[_T]):
    """A Thunk is a wrapper for a value which may or may not be lazily computed.

    A pre-computed Thunk will return its pre-computed value from peek(),
    check() and __call__().

    A lazily-evaluated Thunk will return the TBD sentinel value from peek(),
    and will raise StillTBDError from check(). __call__() will run the lazy
    evaluation function, and save the value. After this the Thunk will behave
    exactly like a pre-computed Thunk. The evaluation function will not be run
    again.

    This is a very rudimentary lazy evaluation wrapper class. Its only goal is
    to facilitate creating a "dry-run plan" for code that does destructive
    changes, and to present that to the user.

    The intended use of Thunk is as a building block to create complex "intent"
    objects, e.g. an intent to create a backup of some data. The various
    parameters (data source, storage location, etc) of this intent can be
    stored as Thunks which may be precomputed
    or lazily evaluated. This intent can be previewed to a user, and optionally
    executed. Using Thunks allows the preview and execution code to be agnostic
    of data dependencies and edge cases. The preview code can just display all
    the precomputed values in the intent object, and the execution code can
    just evaluate the thunks as needed. Note that this means all the complexity
    is pushed to the code that creates the intent objects.
    """

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
        """Create a thunk.

        Thunk(other_thunk) creates a copy of other_thunk. The result will have
        the same precomputed value and/or evaluation function as the argument
        Thunk.

        Thunk(callable) creates a lazily-evaluated Thunk. The argument must be
        a zero-args callable.

        Thunk(any-other-value) creates a pre-computed Thunk.

        Args:
            other: A zero-args callable, or another Thunk, or any other
                precomputed value.
        """
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
        """Returns the precomputed value, or the TBD sentinel value."""
        return self._value

    def check(self) -> _T:
        """Returns the precomputed value, or raises StillTBDError.

        This asserts that the Thunk is pre-computed. It is mostly useful in
        tests.

        Raises:
            StillTBDError: If the thunk is lazy, and hasn't been evaluated yet.
        """
        value = self._value
        if value is TBD:
            raise StillTBDError
        return value

    def is_tbd(self) -> bool:
        """Returns True if the Thunk is lazy and hasn't been evaluated yet."""
        return self._value is TBD

    def __call__(self) -> _T:
        """Returns the value of the Thunk, possibly running its lazy computation.

        Returns:
            The value of the Thunk.
        """
        if self._value is TBD:
            self._value = self._get_value()
        return self._value

    def __repr__(self) -> str:
        """Return a string representation of the Thunk."""
        value = self._value
        if value is TBD:
            return "Thunk(TBD)"
        return f"Thunk({value!r})"

    def __lt__(self, other: Thunk[_T]) -> bool:
        """Returns whether this Thunk should appear before other Thunks.

        The natural ordering of Thunks preserves the natural ordering of their
        values, except that lazy Thunks appear after precomputed Thunks.
        """
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
"""An alias to allowed types for the argument to Thunk()."""
