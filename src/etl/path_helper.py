"""Path resolution with {TOKEN} expansion.

Tokens are sourced from AppConfig. Relative paths resolve against the
project root (ETL_ROOT or walk up to pyproject.toml).
"""

from __future__ import annotations

import re
from pathlib import Path

from etl.app_config import AppConfig

_project_root: str | None = None
_token_map: dict[str, str] = {}


def initialize(config: AppConfig) -> None:
    global _project_root, _token_map
    _project_root = None
    _token_map = {}

    if config.paths.etl_root:
        _token_map["ETL_ROOT"] = config.paths.etl_root


def get_project_root() -> str:
    global _project_root
    if _project_root is not None:
        return _project_root

    if "ETL_ROOT" in _token_map and _token_map["ETL_ROOT"]:
        _project_root = _token_map["ETL_ROOT"]
        return _project_root

    d = Path(__file__).resolve().parent
    while d != d.parent:
        if (d / "pyproject.toml").exists():
            _project_root = str(d)
            return _project_root
        d = d.parent

    raise RuntimeError(
        f"Could not locate project root. No pyproject.toml found in any ancestor "
        f"of {Path(__file__).resolve().parent} and ETL_ROOT is not set."
    )


def resolve(path: str) -> str:
    path = _expand_tokens(path)
    if Path(path).is_absolute():
        return path
    return str(Path(get_project_root()) / path)


def _expand_tokens(path: str) -> str:
    def _replace(match: re.Match) -> str:
        token = match.group(1)
        # Case-insensitive lookup
        for key, value in _token_map.items():
            if key.upper() == token.upper():
                return value
        known = ", ".join(_token_map.keys())
        raise RuntimeError(
            f"Unknown path token '{{{token}}}' (referenced in path '{path}'). "
            f"Known tokens: {known}"
        )

    return re.sub(r"\{(\w+)\}", _replace, path)
