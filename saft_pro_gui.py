"""
Thin wrapper around ``saft.parsers.saft_pro_gui.launch``.

This script lives at the project root to allow running the GUI directly
via ``python saft_pro_gui.py``.  It imports and invokes the canonical
GUI launcher from the ``saft.parsers.saft_pro_gui`` module.  By
delegating to the package implementation, any patches, bug fixes or
logging improvements made in the canonical GUI automatically take
effect regardless of whether you run this script or use the package
entry point (e.g. ``python -m saft.saft_pro_gui``).

If the import fails (for example if this script is moved or run with
an unexpected PYTHONPATH), we fall back to loading the module via
``importlib``.  This protects against ``ModuleNotFoundError`` when the
``saft`` package cannot be resolved on ``sys.path``.
"""
from __future__ import annotations

import importlib


def main() -> None:
    """Import and launch the GUI implementation from the package."""
    try:
        # Attempt to import the canonical launch function directly from the package.
        from parsers.saft_pro_gui import launch  # type: ignore[import]
    except Exception:
        # Fallback: import the module via importlib to avoid ModuleNotFoundError
        module = importlib.import_module("saft.parsers.saft_pro_gui")
        launch = getattr(module, "launch")  # type: ignore[assignment]
    launch()


if __name__ == "__main__":
    main()