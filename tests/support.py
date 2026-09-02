"""Load the hyphenated plugin directory as an isolated test package."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PACKAGE = "synthetic_sociality_room_test_plugin"


def load_module(name: str):
    if PACKAGE not in sys.modules:
        package_spec = importlib.util.spec_from_file_location(
            PACKAGE,
            ROOT / "__init__.py",
            submodule_search_locations=[str(ROOT)],
        )
        package = importlib.util.module_from_spec(package_spec)
        sys.modules[PACKAGE] = package
        package.__path__ = [str(ROOT)]
    qualified = f"{PACKAGE}.{name}"
    if qualified in sys.modules:
        return sys.modules[qualified]
    spec = importlib.util.spec_from_file_location(qualified, ROOT / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[qualified] = module
    spec.loader.exec_module(module)
    return module
