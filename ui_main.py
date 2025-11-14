# ui_main.py (hotfix)
# -*- coding: utf-8 -*-
"""
SAF-T Parser – minimal oppstart.
Hotfix: Laster `parsers.zip` automatisk hvis den ligger i:
  - _MEIPASS\plugins\parsers.zip (bundlet i exe via --add-data)
  - EXE-mappen\plugins\parsers.zip
  - _MEIPASS\parsers.zip eller EXE-mappen\parsers.zip
"""
from __future__ import annotations
import sys, types
from pathlib import Path

def _path_exists(p: Path) -> bool:
    try:
        return p.exists()
    except Exception:
        return False

# -- Finn basefolder (PyInstaller onefile pakker ut til _MEIPASS)
BASE_DIR = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent))
# -- Finn faktisk EXE-mappe (slik at vi også kan finne sidecar-filer ved siden av .exe)
EXE_DIR = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else Path(__file__).resolve().parent

# Sørg for at både BASE_DIR og EXE_DIR er på importstien
for p in [EXE_DIR, BASE_DIR]:
    s = str(p)
    if s not in sys.path:
        sys.path.insert(0, s)

# Hotfix: se etter parsers.zip og legg den på sys.path (zipimport støttes)
def _try_add_parsers_zip():
    candidates = [
        BASE_DIR / "plugins" / "parsers.zip",
        BASE_DIR / "parsers.zip",
        EXE_DIR / "plugins" / "parsers.zip",
        EXE_DIR / "parsers.zip",
    ]
    for cand in candidates:
        if _path_exists(cand):
            sys.path.insert(0, str(cand))
            return str(cand)
    return ""

zip_used = _try_add_parsers_zip()

# Alias: app.parsers -> parsers (proj importerer noen steder app.parsers.*)
if "parsers" not in sys.modules:
    import importlib
    parsers = importlib.import_module("parsers")
else:
    parsers = sys.modules["parsers"]

app_pkg = types.ModuleType("app")
app_pkg.__path__ = []
app_pkg.parsers = parsers
sys.modules["app"] = app_pkg
sys.modules["app.parsers"] = parsers

def main():
    try:
        from parsers import App  # type: ignore
    except Exception as e:
        msg = "[fatal] Klarte ikke å importere parsers.saft_pro_gui.App: %s" % e
        print(msg)
        if zip_used:
            print("Prøvde med parsers.zip fra:", zip_used)
        else:
            print("Fant ingen parsers.zip ved siden av EXE eller i _MEIPASS.")
        raise

    root = App(BASE_DIR)
    if zip_used:
        try:
            root._log(f"[boot] Lastet parsers.zip fra: {zip_used}")
        except Exception:
            pass
    root.mainloop()

if __name__ == "__main__":
    main()
