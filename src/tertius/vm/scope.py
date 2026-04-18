import importlib
import inspect
from collections.abc import Callable
from types import ModuleType
from typing import Any


type Scope = dict[str, Callable[..., Any]]


def scan(module: ModuleType) -> Scope:
    """Return all public top-level functions in a module."""
    return {
        name: obj
        for name, obj in inspect.getmembers(module, inspect.isfunction)
        if not name.startswith("_")
    }


def resolve_fn(fn_name: str) -> Callable[..., Any]:
    """Resolve 'module.path:function_name' to a callable by importing and scanning the module."""
    if ":" not in fn_name:
        raise ValueError(
            f"fn_name must be in 'module.path:function_name' format, got {fn_name!r}"
        )

    module_path, name = fn_name.rsplit(":", 1)
    module = importlib.import_module(module_path)
    scope = scan(module)

    if name not in scope:
        raise KeyError(f"{name!r} not found in {module_path!r}")

    return scope[name]
