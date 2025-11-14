# -*- coding: utf-8 -*-
"""
run_saft_pro_gui.py – CLI/GUI-runner for SAF‑T
- Tidsstemplet/versjonert output‑mappe
- Streaming/fallback parse
- Profil
- Rapporter (inkl. GL-basert subledger)
- Enkel Trial Balance (trial_balance.xlsx) m/fallback
- Mapping- og compliance-CSV
- Norsk Excel-format (visning)
"""
from __future__ import annotations

import os, sys, time, shutil, argparse, importlib, importlib.util, json
from pathlib import Path
from typing import Optional

THIS_FILE = Path(__file__).resolve()
SRC_DIR = THIS_FILE.parent

def _debug(msg: str) -> None:
    print(f"[run_saft_pro_gui] {msg}")

def _spec_from_file(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None: return None
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    except Exception as e:
        _debug(f"Kunne ikke importere lokal modul {path.name}: {e}")
        return None
    return mod

def _import_module(name: str):
    try:
        return importlib.import_module(name)
    except Exception as e:
        _debug(f"Kunne ikke importere {name}: {e!r}")
        return None

def _unique_dir(base: Path) -> Path:
    """Hvis base finnes, lag base (2), base (3) ..."""
    if not base.exists():
        return base
    i = 2
    while True:
        cand = base.parent / f"{base.name} ({i})"
        if not cand.exists():
            return cand
        i += 1

def _make_job_root(input_path: Path, outdir: Optional[Path]) -> Path:
    """
    Construct a root output directory for a SAF‑T run.

    If the caller passes a custom ``outdir``, version it if it already exists.
    Otherwise we derive a compact, timestamped directory name from the input
    filename.  We truncate the base of the name to 40 characters and use
    ``YYYYMMDD_%H%M%S`` for the timestamp.  This greatly reduces the risk of
    exceeding Windows path length limits when combined with nested subfolders
    and long file names.  The `_unique_dir` helper will append ``(2)``,
    ``(3)`` etc. to the directory name if the chosen name is already used.
    """
    if outdir:
        # respect user-supplied outdir but avoid clobbering existing folders
        return _unique_dir(Path(outdir))
    # derive a short name from the input stem and a compact timestamp
    ts = time.localtime()
    stamp = time.strftime("%Y%m%d_%H%M%S", ts)
    stem_trunc = input_path.stem[:40]
    name = f"{stem_trunc}_{stamp}_SAFT_OUTPUT"
    return _unique_dir(input_path.parent / name)

def _safe_outdirs(input_path: Path, outdir: Optional[Path]):
    job_root = _make_job_root(input_path, outdir)
    csv_dir = job_root / "csv"
    excel_dir = job_root / "excel"
    csv_dir.mkdir(parents=True, exist_ok=True)
    excel_dir.mkdir(parents=True, exist_ok=True)
    return job_root, csv_dir, excel_dir

def _console_progress_printer(enabled: bool):
    last_ts = 0.0
    def _cb(kind: str, payload: object):
        nonlocal last_ts
        if not enabled or kind != "tick": return True
        now = time.perf_counter()
        if now - last_ts < 1.0: return True
        last_ts = now
        d = payload if isinstance(payload, dict) else {}
        rate = d.get("rate_events_per_sec", 0.0)
        events = d.get("events", 0)
        csv_rows = d.get("csv_rows", {})
        top = d.get("top_times", [])
        top_str = ", ".join([f"{k}:{v:.1f}s" for k, v in top[:3]])
        rows_str = ", ".join([f"{k}:{v}" for k, v in sorted(csv_rows.items())])
        print(f"[progress] events={events:,}  rate={rate:,.0f}/s  rows[{rows_str}]  top={top_str}")
        return True
    return _cb

def _write_run_meta(job_root: Path, input_path: Path, started: float, finished: float, parser: str):
    meta = {
        "input_file": str(input_path),
        "job_root": str(job_root),
        "started_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(started)),
        "finished_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(finished)),
        "duration_sec": round(finished - started, 3),
        "parser": parser,
    }
    (job_root / "csv" / "run_meta.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")

def _run_mapping_probe(input_path: Path, csv_dir: Path) -> Optional[dict]:
    probe = _spec_from_file("_local_probe", SRC_DIR / "saft_mapping_probe.py")
    if not probe:
        probe = _import_module("app.parsers.saft_mapping_probe")
    if not probe or not hasattr(probe, "_probe"):
        _debug("Fant ikke saft_mapping_probe")
        return None
    try:
        summary = probe._probe(input_path, csv_dir)  # type: ignore
        c = summary.get("counts", {})
        _debug(f"[mapping] accounts_with_grouping={c.get('accounts_with_grouping',0)} / accounts_total={c.get('accounts_total',0)}")
        return summary
    except Exception as e:
        _debug(f"Mapping-probe feilet: {e!r}")
        return None

def _build_profile(csv_dir: Path, input_path: Path) -> Path:
    local = SRC_DIR / "saft_profile_builder.py"
    if local.exists():
        m = _spec_from_file("_local_profile_builder", local)
        if m and hasattr(m, "build_profile"):
            return m.build_profile(csv_dir, input_file=input_path)  # type: ignore
    mod = _import_module("app.parsers.saft_profile_builder")
    if not mod or not hasattr(mod, "build_profile"):
        raise RuntimeError("Finner ikke saft_profile_builder.build_profile")
    return mod.build_profile(csv_dir, input_file=input_path)  # type: ignore

def _run_excel_pipeline(csv_dir: Path, excel_dir: Path, profile_path: Path, *, make_excel: bool):
    try:
        prof = json.loads(profile_path.read_text(encoding="utf-8"))
    except Exception as e:
        _debug(f"Kunne ikke lese profil: {e!r}")
        prof = {}
    has_ar = bool(prof.get("presence", {}).get("has_ar_lines"))
    has_ap = bool(prof.get("presence", {}).get("has_ap_lines"))

    # 1) GL-basert subledger
    gl_mod = _import_module("app.parsers.saft_subledger_from_gl")
    if gl_mod:
        if has_ar: getattr(gl_mod, "make_subledger", lambda *a, **k: None)(csv_dir, "AR")
        else: _debug("Hopper AR-subledger: ingen CustomerID på linjer.")
        if has_ap: getattr(gl_mod, "make_subledger", lambda *a, **k: None)(csv_dir, "AP")
        else: _debug("Hopper AP-subledger: ingen SupplierID på linjer.")

    # 2) Eksisterende moduler
    for mname in ("app.parsers.saft_controls_and_exports",
                  "app.parsers.saft_reports",
                  "app.parsers.saft_full_run"):
        mod = _import_module(mname)
        if not mod: continue
        for fn, args in (("make_general_ledger", (csv_dir,)),
                         ("make_trial_balance", (csv_dir,)),
                         ("make_subledger", (csv_dir, "AR") if has_ar else None),
                         ("make_subledger", (csv_dir, "AP") if has_ap else None),
                         ("export_reports", (csv_dir,))):
            if args is None: continue
            try: getattr(mod, fn, lambda *a, **k: None)(*args)
            except TypeError: getattr(mod, fn, lambda *a, **k: None)(csv_dir)

    # 2b) Enkel Trial Balance – robust
    simple_tb = _import_module("app.parsers.saft_trial_balance_simple")
    if simple_tb and hasattr(simple_tb, "make_simple_trial_balance"):
        try:
            path = simple_tb.make_simple_trial_balance(csv_dir)  # type: ignore
            _debug(f"Skrev enkel Trial Balance: {path.name}")
            meta_p = csv_dir / "simple_trial_balance_meta.json"
            if meta_p.exists():
                meta = json.loads(meta_p.read_text(encoding="utf-8"))
                cnt = meta.get("counts", {})
                _debug(f"TB-meta: totals={cnt.get('gl_totals_rows',0)}  "
                       f"tb_rows={cnt.get('trialbalance_rows',0)}  "
                       f"delim={meta.get('sources',{}).get('gl_totals_delimiter','?')}  "
                       f"fallback_used={meta.get('notes',{}).get('fallback_used', False)}")
        except Exception as e:
            _debug(f"Enkel Trial Balance feilet: {e!r}")

    # 3) Flytt Excel -> excel/
    for p in list(csv_dir.glob("*.xls*")):
        try: shutil.move(str(p), str(excel_dir / p.name))
        except Exception: pass

    # 4) Mapping/compliance-CSV + Excel mapping (valgfri)
    mapping = _import_module("app.parsers.saft_mapping_report")
    if mapping and hasattr(mapping, "generate"):
        try:
            acc_csv, tax_csv, findings_csv, findings_sum_csv, xlsx_path, stats = mapping.generate(csv_dir, excel_dir, make_excel=make_excel)  # type: ignore
            _debug(f"Skrev mapping CSV: {acc_csv.name}, {tax_csv.name}; compliance: {findings_csv.name}")
        except Exception as e:
            _debug(f"Mapping-rapport feilet: {e!r}")

    # 5) Norsk Excel-format (valgfri visning)
    fmt = _import_module("app.parsers.excel_formatter")
    if fmt and hasattr(fmt, "format_all") and make_excel:
        try:
            n = fmt.format_all(excel_dir, verbose=False)  # type: ignore
            _debug(f"Formatterte {n} Excel-filer i {excel_dir}")
        except Exception as e:
            _debug(f"Excel-formattering feilet: {e!r}")

def parse_file(input_path: Path, outdir: Optional[Path]=None, *, progress: bool=False, force_stream: bool=True, raw_mode: str="off"):
    parse_stream, ver_stream, parse_pro, ver_pro = _import_parsers()
    job_root, csv_dir, excel_dir = _safe_outdirs(Path(input_path), outdir)

    if raw_mode.lower() in ("on","off"):
        os.environ["SAFT_WRITE_RAW"] = "1" if raw_mode.lower()=="on" else "0"
    os.environ.setdefault("SAFT_PROGRESS_EVENTS", "50000")

    progress_cb = _console_progress_printer(progress)

    started = time.time()
    parser_name = "unknown"

    if force_stream and parse_stream:
        _debug(f"Bruker STREAM‑parser (versjon {ver_stream or '?'})")
        parser_name = f"stream:{ver_stream or '?'}"
        try:
            parse_stream(Path(input_path), csv_dir, on_progress=progress_cb)  # type: ignore[misc]
        except Exception as e:
            _debug(f"Streaming‑parser feilet: {e!r} – prøver fallback")
            if parse_pro:
                _debug(f"Bruker FALLBACK‑parser (versjon {ver_pro or '?'})")
                parser_name = f"fallback:{ver_pro or '?'}"
                parse_pro(Path(input_path), csv_dir)  # type: ignore[misc]
            else:
                raise
    else:
        if parse_pro:
            _debug(f"Bruker FALLBACK‑parser (versjon {ver_pro or '?'})")
            parser_name = f"fallback:{ver_pro or '?'}"
            parse_pro(Path(input_path), csv_dir)  # type: ignore[misc]
        else:
            raise RuntimeError("Ingen parser tilgjengelig.")

    finished = time.time()
    _write_run_meta(job_root, Path(input_path), started, finished, parser_name)
    return job_root, csv_dir, excel_dir

def _ask_file_gui() -> Optional[str]:
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception:
        return None
    try:
        root = tk.Tk(); root.withdraw()
        path = filedialog.askopenfilename(
            title="Velg SAF‑T‑fil (.xml/.zip)",
            filetypes=[("SAF‑T XML/ZIP", "*.xml *.zip"), ("Alle filer", "*.*")]
        )
        root.destroy()
        return path or None
    except Exception:
        return None

def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("input", nargs="?", help="SAF‑T .xml eller .zip")
    p.add_argument("outdir", nargs="?", help="Output‑mappe (default: navngitt mappe ved siden av filen)")
    p.add_argument("--no-stream", action="store_true")
    p.add_argument("--no-gui", action="store_true")
    p.add_argument("--no-excel", action="store_true")
    p.add_argument("--progress", action="store_true")
    p.add_argument("--raw", choices=["on","off"], default="off")
    p.add_argument("--ui", action="store_true", help="Start GUI")
    args = p.parse_args(argv)

    if args.ui:
        try:
            from .saft_pro_gui import launch as _ui_launch  # type: ignore
            _ui_launch()
        except Exception as e:
            _debug(f"GUI feilet: {e!r}")
            return 2
        return 0

    input_path = args.input or (None if args.no_gui else _ask_file_gui())
    if not input_path:
        p.print_help(sys.stderr); _debug("Ingen input gitt – avbryter.")
        return 2

    job_root, csv_dir, excel_dir = parse_file(Path(input_path),
                                              Path(args.outdir) if args.outdir else None,
                                              progress=args.progress,
                                              force_stream=not args.no_stream,
                                              raw_mode=args.raw)

    _debug(f"Outputmappe: {job_root}")

    _run_mapping_probe(Path(input_path), csv_dir)
    prof_path = _build_profile(csv_dir, Path(input_path))
    _debug(f"Skrev profil: {prof_path}")

    _run_excel_pipeline(csv_dir, excel_dir, prof_path, make_excel=not args.no_excel)

    print("CSV skrevet til:", csv_dir)
    if not args.no_excel:
        print("Excel (hvis generert) i:", excel_dir)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
