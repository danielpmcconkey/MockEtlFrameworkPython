"""External module — registry-based dispatcher.

In C# this used Assembly.LoadFrom() + reflection to load a DLL at runtime.
In Python we maintain a dict mapping typeName strings to callables.
Each callable has the signature:
    def execute(shared_state: dict[str, object]) -> dict[str, object]

OG modules live in src/etl/modules/externals/ and are loaded via standard
Python imports. RE modules live in RE/externals/ and are loaded dynamically
via importlib so the framework can pick them up without a code change or
rebuild.
"""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
from typing import Callable

from etl.modules.base import Module

logger = logging.getLogger(__name__)

# Type alias for external step functions.
ExternalStepFn = Callable[[dict[str, object]], dict[str, object]]

# Registry: maps "ExternalModules.<ClassName>" -> callable.
_REGISTRY: dict[str, ExternalStepFn] = {}


def register(type_name: str, fn: ExternalStepFn) -> None:
    """Register an external step function under a typeName."""
    _REGISTRY[type_name] = fn


def _load_from_dir(directory: Path, prefix: str) -> None:
    """Scan a directory for .py files and load each to trigger register() calls."""
    if not directory.is_dir():
        return
    for py_file in sorted(directory.glob("*.py")):
        if py_file.name.startswith("_"):
            continue
        module_name = f"{prefix}.{py_file.stem}"
        try:
            spec = importlib.util.spec_from_file_location(module_name, py_file)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)  # triggers register() calls
            logger.info("Loaded %s external module: %s", prefix, py_file.name)
        except Exception:
            logger.exception("Failed to load %s external module: %s", prefix, py_file.name)


def _load_all() -> None:
    """Import all external module files to trigger registration.

    Two sources:
    1. OG modules in etl/modules/externals/ (directory scan)
    2. RE modules in {ETL_ROOT}/RE/externals/ (directory scan)
    """
    from etl import path_helper

    # --- OG modules ---
    og_externals_dir = Path(__file__).parent / "externals"
    _load_from_dir(og_externals_dir, "og_externals")

    # --- RE modules ---
    re_externals_dir = Path(path_helper.resolve("RE/externals"))
    _load_from_dir(re_externals_dir, "re_externals")


_loaded = False


class External(Module):
    def __init__(self, assembly_path: str, type_name: str) -> None:
        self.assembly_path = assembly_path
        self.type_name = type_name

    def execute(self, shared_state: dict[str, object]) -> dict[str, object]:
        global _loaded
        if not _loaded:
            _load_all()
            _loaded = True

        fn = _REGISTRY.get(self.type_name)
        if fn is None:
            raise ValueError(
                f"No registered external module for typeName '{self.type_name}'. "
                f"Known: {sorted(_REGISTRY.keys())}"
            )
        return fn(shared_state)
