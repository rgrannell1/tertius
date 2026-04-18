import types
import pytest

from tertius.vm.scope import resolve_fn, scan


def _make_module(name: str, members: dict) -> types.ModuleType:
    """Create a synthetic module with the given members for testing."""
    module = types.ModuleType(name)

    for key, val in members.items():
        setattr(module, key, val)

    return module


def public_fn(): ...
def another_fn(): ...
def _private_fn(): ...


NOT_A_FUNCTION = 42


def test_scan_finds_public_functions():
    """scan returns all public top-level functions."""

    module = _make_module("test", {"public_fn": public_fn, "another_fn": another_fn})
    result = scan(module)
    assert "public_fn" in result
    assert "another_fn" in result


def test_scan_excludes_private_functions():
    """Proves that scan excludes functions whose names start with _."""

    module = _make_module("test", {"_private_fn": _private_fn})
    assert "_private_fn" not in scan(module)


def test_scan_excludes_non_functions():
    """Proves that scan excludes non-callable members."""

    module = _make_module("test", {"NOT_A_FUNCTION": NOT_A_FUNCTION})
    assert "NOT_A_FUNCTION" not in scan(module)


def test_scan_returns_correct_callables():
    """Proves that scan maps names to the actual function objects."""

    module = _make_module("test", {"public_fn": public_fn})
    assert scan(module)["public_fn"] is public_fn


def test_resolve_fn_returns_callable():
    """Proves that resolve_fn resolves a 'module:name' string to the correct callable."""

    fn = resolve_fn("tertius.vm.scope:scan")
    assert fn is scan


def test_resolve_fn_rejects_missing_colon():
    """Proves that resolve_fn raises ValueError when fn_name has no colon separator."""

    with pytest.raises(ValueError, match="module.path:function_name"):
        resolve_fn("tertius.vm.scope.scan")


def test_resolve_fn_rejects_unknown_name():
    """Proves that resolve_fn raises KeyError when the function does not exist in the module."""

    with pytest.raises(KeyError):
        resolve_fn("tertius.vm.scope:nonexistent")
