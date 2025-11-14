# -*- coding: utf-8 -*-
"""
saft_pro_gui.py
----------------
GUI for SAF‑T‑kjøring (streamingparser + profil + rapporter).
- Navngir output-mappe med tidsstempel (eller versjonerer hvis bruker har valgt mappe)
- Mapping-probe -> csv/
- Profil
- Rapporter (inkl. GL-subledger)
- Enkel Trial Balance (trial_balance.xlsx) m/fallback
- Mapping/compliance-CSV
- Norsk Excel-format (visning)
- run_meta.json med start/slutt/durasjon

"""
from __future__ import annotations

import os
import sys
import time
import json
import shutil
import threading
from pathlib import Path
from typing import Optional, Callable

try:
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox
except Exception as e:
    raise RuntimeError("Tkinter er påkrevd for GUI.") from e


def _import_module(name: str):
    try:
        return __import__(name, fromlist=["*"])
    except Exception:
        return None

def _spec_from_file(name: str, path: Path):
    import importlib.util
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        return None
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    except Exception:
        return None
    return mod

def _open_dir(p: Path) -> None:
    try:
        if sys.platform.startswith("win"):
            os.startfile(str(p))  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            import subprocess; subprocess.Popen(["open", str(p)])
        else:
            import subprocess; subprocess.Popen(["xdg-open", str(p)])
    except Exception:
        pass

def _unique_dir(base: Path) -> Path:
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
    Determine a unique output directory based on the input file name and current
    timestamp.  To avoid extremely long Windows paths (which can exceed the
    platform's MAX_PATH limit and cause mysterious FileNotFoundError crashes),
    we construct a more concise directory name than the historical
    ``"<file> NY dd.mm.yyyy kl HHMM_SAFT_OUTPUT"`` pattern.

    If the user has provided an explicit output directory (``outdir``), we
    respect it but still version it if it already exists.  Otherwise, we use
    the stem of the input file (truncated to 40 characters to keep the path
    length manageable) and append a timestamp in ``YYYYMMDD_HHMMSS`` format
    followed by ``_SAFT_OUTPUT``.  Any spaces or other problematic characters
    in the stem are preserved, but the overall name is much shorter than
    before, greatly reducing the chance of hitting path length limits on
    Windows.  The ``_unique_dir`` helper will append ``(2)``, ``(3)`` etc. as
    needed if the generated directory already exists.
    """
    # If the user specified an output directory, version it if it exists
    if outdir:
        return _unique_dir(Path(outdir))
    # Build a compact timestamp and truncate long stems to avoid very long paths
    ts = time.localtime()
    stamp = time.strftime("%Y%m%d_%H%M%S", ts)
    # Limit the base of the directory name to at most 40 characters to reduce
    # the risk of exceeding Windows path length limits when combined with
    # parent directories, "csv"/"excel" subfolders, and file names.
    stem_trunc = input_path.stem[:40]
    name = f"{stem_trunc}_{stamp}_SAFT_OUTPUT"
    return _unique_dir(input_path.parent / name)


class Engine:
    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)
        # Collect detailed diagnostic information during parser discovery.
        # Populated by _find_parser() and consumed by run() for debug logging.
        self._parser_debug: list[str] = []

    def _find_parser(self):
        """
        Locate the streaming and fallback parser implementations.

        We first try to load ``saft_stream_parser.py`` and ``saft_parser_pro.py``
        from ``self.base_dir``.  If they are not found there (which happens
        when this script lives at a higher level than the parsers package),
        we also look in a ``parsers`` subfolder next to this file.  This makes
        it possible to run the GUI from the project root while still finding
        the local parser implementations.

        As a final fallback, we attempt to import the parsers via their package
        name (``app.parsers...``).  If your project uses a different top-level
        package than ``app``, the local lookup above avoids relying on the
        package name.
        """
        # Determine where the parser files might live.  In a typical layout the
        # base_dir is the parsers package itself.  If not, fall back to a
        # parsers/ subdirectory relative to base_dir.
        # Reset debug info each time we search for parsers
        self._parser_debug = []
        candidate_dirs = [self.base_dir]
        # If saft_stream_parser.py is not present in base_dir, check parsers/ subdir
        if not (self.base_dir / "saft_stream_parser.py").exists():
            cand = self.base_dir / "parsers"
            if cand.is_dir():
                candidate_dirs.append(cand)

        parse_stream = parse_pro = None
        ver_stream = ver_pro = None

        # Try to load from file for each candidate directory.  We manually import the modules
        # here to capture import exceptions for debug output.
        import importlib.util
        for d in candidate_dirs:
            # Attempt to load streaming parser from this directory
            if parse_stream is None:
                p_stream = d / "saft_stream_parser.py"
                if p_stream.exists():
                    try:
                        spec = importlib.util.spec_from_file_location("_local_stream", str(p_stream))
                        if spec and spec.loader:
                            mod = importlib.util.module_from_spec(spec)
                            spec.loader.exec_module(mod)  # type: ignore[attr-defined]
                            parse_stream = getattr(mod, "parse_saft", None)
                            ver_stream = getattr(mod, "__version__", None)
                            self._parser_debug.append(f"Fant streamingparser i {p_stream} (versjon {ver_stream or '?'})")
                        else:
                            self._parser_debug.append(f"Fant {p_stream}, men kunne ikke laste modulen (util.spec er None)")
                    except Exception as e:
                        # include type and message for easier diagnosis
                        self._parser_debug.append(f"Fant {p_stream}, men import feilet: {e!r}")
                else:
                    self._parser_debug.append(f"{p_stream} finnes ikke")
            # Attempt to load fallback parser from this directory
            if parse_pro is None:
                p_pro = d / "saft_parser_pro.py"
                if p_pro.exists():
                    try:
                        spec = importlib.util.spec_from_file_location("_local_pro", str(p_pro))
                        if spec and spec.loader:
                            mod = importlib.util.module_from_spec(spec)
                            spec.loader.exec_module(mod)  # type: ignore[attr-defined]
                            parse_pro = getattr(mod, "parse_saft", None)
                            ver_pro = getattr(mod, "__version__", None)
                            self._parser_debug.append(f"Fant fallbackparser i {p_pro} (versjon {ver_pro or '?'})")
                        else:
                            self._parser_debug.append(f"Fant {p_pro}, men kunne ikke laste modulen (util.spec er None)")
                    except Exception as e:
                        self._parser_debug.append(f"Fant {p_pro}, men import feilet: {e!r}")
                else:
                    self._parser_debug.append(f"{p_pro} finnes ikke")
            # If both parsers are found, stop searching
            if parse_stream is not None and parse_pro is not None:
                break

        # Fall back to package import only if necessary.  Keep the import path
        # consistent with historical behaviour, but don't fail if the package
        # name is missing.
        if parse_stream is None:
            m = _import_module("app.parsers.saft_stream_parser")
            if m:
                parse_stream = getattr(m, "parse_saft", None)
                ver_stream = getattr(m, "__version__", None)
                self._parser_debug.append("Importerte streamingparser via app.parsers.saft_stream_parser")
            else:
                self._parser_debug.append("Kunne ikke importere app.parsers.saft_stream_parser")
        if parse_pro is None:
            m = _import_module("app.parsers.saft_parser_pro")
            if m:
                parse_pro = getattr(m, "parse_saft", None)
                ver_pro = getattr(m, "__version__", None)
                self._parser_debug.append("Importerte fallbackparser via app.parsers.saft_parser_pro")
            else:
                self._parser_debug.append("Kunne ikke importere app.parsers.saft_parser_pro")

        return (parse_stream, ver_stream, parse_pro, ver_pro)

    def _safe_outdirs(self, input_path: Path, outdir: Optional[Path]):
        job_root = _make_job_root(input_path, outdir)
        csv_dir = job_root / "csv"; excel_dir = job_root / "excel"
        csv_dir.mkdir(parents=True, exist_ok=True); excel_dir.mkdir(parents=True, exist_ok=True)
        return job_root, csv_dir, excel_dir

    def _build_profile(self, csv_dir: Path, input_file: Path) -> Path:
        p = self.base_dir / "saft_profile_builder.py"
        mod = _spec_from_file("_local_profile_builder", p) if p.exists() else None
        if mod is None: mod = _import_module("app.parsers.saft_profile_builder")
        if not mod or not hasattr(mod, "build_profile"):
            raise RuntimeError("Finner ikke saft_profile_builder.build_profile")
        return mod.build_profile(csv_dir, input_file=input_file)  # type: ignore

    def _run_mapping_probe(self, input_file: Path, csv_dir: Path, log: Callable[[str], None]) -> None:
        probe = _spec_from_file("_local_probe", self.base_dir / "saft_mapping_probe.py")
        if not probe:
            probe = _import_module("app.parsers.saft_mapping_probe")
        if not probe or not hasattr(probe, "_probe"):
            log("[mapping] Fant ikke saft_mapping_probe"); return
        try:
            summary = probe._probe(input_file, csv_dir)  # type: ignore
            c = summary.get("counts", {})
            log(f"[mapping] accounts_with_grouping={c.get('accounts_with_grouping',0)} / accounts_total={c.get('accounts_total',0)}")
        except Exception as e:
            log(f"[mapping] Probe feilet: {e!r}")

    def _run_excel_pipeline_with_profile(self, csv_dir: Path, excel_dir: Path, profile_path: Path, log: Callable[[str], None], *, make_excel: bool) -> None:
        try:
            prof = json.loads(profile_path.read_text(encoding="utf-8"))
        except Exception as e:
            log(f"[excel] Kunne ikke lese profil: {e!r}"); prof = {}
        has_ar = bool(prof.get("presence", {}).get("has_ar_lines"))
        has_ap = bool(prof.get("presence", {}).get("has_ap_lines"))

        def call_if(mod, fname: str, *a):
            """
            Call a function on the given module if present.  Log the call and
            capture any unexpected exceptions for debug purposes.  Some functions
            accept fewer arguments than provided (e.g. make_subledger(csv_dir) vs
            make_subledger(csv_dir, "AR")), so a TypeError triggers a retry
            with only the first argument.  Any other exception is logged and
            swallowed to avoid aborting the entire pipeline.
            """
            fn = getattr(mod, fname, None)
            if callable(fn):
                # Describe the call for the log
                try:
                    descr = ", ".join([repr(x) for x in a])
                except Exception:
                    descr = ""
                log(f"[excel] Kaller {mod.__name__}.{fname}({descr})")
                try:
                    fn(*a)
                except TypeError:
                    # Retry with only the first argument if signature mismatch
                    try:
                        fn(*a[:1])
                    except Exception as e:
                        log(f"[debug] {mod.__name__}.{fname} feilet: {e!r}")
                except Exception as e:
                    # Log any unexpected exception and continue
                    log(f"[debug] {mod.__name__}.{fname} feilet: {e!r}")

        # A) GL-subledger
        gl_sub = _import_module("app.parsers.saft_subledger_from_gl")
        if not gl_sub:
            # Surface import failures so the user can see missing dependencies
            log("[debug] Kunne ikke importere app.parsers.saft_subledger_from_gl")
        else:
            if has_ar:
                try:
                    log("[excel] GL-subledger: AR"); gl_sub.make_subledger(csv_dir, "AR")  # type: ignore
                except Exception as e:
                    log(f"[excel] AR feilet: {e!r}")
            else:
                log("[excel] Hopper AR (ingen CustomerID på linjer)")
            if has_ap:
                try:
                    log("[excel] GL-subledger: AP"); gl_sub.make_subledger(csv_dir, "AP")  # type: ignore
                except Exception as e:
                    log(f"[excel] AP feilet: {e!r}")
            else:
                log("[excel] Hopper AP (ingen SupplierID på linjer)")

        # B) Øvrige rapporter
        for modname in ("app.parsers.saft_controls_and_exports",
                        "app.parsers.saft_reports",
                        "app.parsers.saft_full_run"):
            mod = _import_module(modname)
            if not mod:
                log(f"[debug] Kunne ikke importere {modname}")
                continue
            # General ledger and trial balance
            call_if(mod, "make_general_ledger", csv_dir)
            call_if(mod, "make_trial_balance", csv_dir)
            # Subledger (AR/AP) – skip if profile says no lines or catch missing functions
            if has_ar:
                call_if(mod, "make_subledger", csv_dir, "AR")
            else:
                log("[excel] Hopper over AR-subledger: ingen linjer med CustomerID.")
            if has_ap:
                call_if(mod, "make_subledger", csv_dir, "AP")
            else:
                log("[excel] Hopper over AP-subledger: ingen linjer med SupplierID.")
            call_if(mod, "export_reports", csv_dir)

        # B2) Enkel Trial Balance (robust)
        simple_tb = _import_module("app.parsers.saft_trial_balance_simple")
        if not simple_tb:
            log("[debug] Kunne ikke importere app.parsers.saft_trial_balance_simple")
        elif hasattr(simple_tb, "make_simple_trial_balance"):
            try:
                path = simple_tb.make_simple_trial_balance(csv_dir)  # type: ignore
                log(f"[excel] Skrev enkel Trial Balance: {path.name}")
                meta_p = csv_dir / "simple_trial_balance_meta.json"
                if meta_p.exists():
                    meta = json.loads(meta_p.read_text(encoding="utf-8"))
                    cnt = meta.get("counts", {})
                    log(f"[excel] TB-meta: totals={cnt.get('gl_totals_rows',0)}  "
                        f"tb_rows={cnt.get('trialbalance_rows',0)}  "
                        f"delim={meta.get('sources',{}).get('gl_totals_delimiter','?')}  "
                        f"fallback_used={meta.get('notes',{}).get('fallback_used', False)}")
            except Exception as e:
                log(f"[excel] Enkel Trial Balance feilet: {e!r}")

        # C) Flytt *.xls* til excel/
        moved = 0
        for pth in csv_dir.glob("*.xls*"):
            try: shutil.move(str(pth), str(excel_dir / pth.name)); moved += 1
            except Exception: pass
        log(f"[excel] Flyttet {moved} filer til {excel_dir}")

        # D) Mapping + compliance + Excel‑mapping (valgfri)
        mapping = _import_module("app.parsers.saft_mapping_report")
        if not mapping:
            log("[debug] Kunne ikke importere app.parsers.saft_mapping_report")
        elif hasattr(mapping, "generate"):
            try:
                acc_csv, tax_csv, findings_csv, findings_sum_csv, xlsx_path, stats = mapping.generate(csv_dir, excel_dir, make_excel=make_excel)  # type: ignore
                log(f"[excel] Skrev mapping CSV: {acc_csv.name}, {tax_csv.name}; compliance: {findings_csv.name}")
            except Exception as e:
                log(f"[excel] Mapping-rapport feilet: {e!r}")

        # E) Norsk visningsformat
        try:
            if make_excel:
                fmt = _import_module("app.parsers.excel_formatter")
                if not fmt:
                    log("[debug] Kunne ikke importere app.parsers.excel_formatter")
                elif hasattr(fmt, "format_all"):
                    n = fmt.format_all(excel_dir, verbose=False)  # type: ignore
                    log(f"[excel] Norsk format satt på {n} filer")
        except Exception as e:
            log(f"[excel] Format-feil: {e!r}")

    def run(self, input_file: Path, outdir: Optional[Path], *,
            use_stream: bool, write_raw: bool, do_excel: bool,
            progress_every: int, on_progress: Optional[Callable[[str, dict], None]],
            on_log: Callable[[str], None], cancel_flag: threading.Event) -> tuple[Path, Path, Optional[Path]]:

        job_root, csv_dir, excel_dir = self._safe_outdirs(input_file, outdir)
        os.environ["SAFT_WRITE_RAW"] = "1" if write_raw else "0"
        os.environ["SAFT_PROGRESS_EVENTS"] = str(int(progress_every))

        ps, vs, pp, vp = self._find_parser()
        on_log(f"[gui] Outputmappe: {job_root}")
        on_log(f"[gui] Parser(e): stream={bool(ps)}({vs}), fallback={bool(pp)}({vp})")
        # Dump parser discovery diagnostics for troubleshooting if parsers are missing
        if not ps or not pp:
            for msg in self._parser_debug:
                on_log(f"[debug] {msg}")
        progress_ts = 0.0

        started = time.time()
        parser_name = "unknown"

        def _cb(kind: str, payload: object):
            nonlocal progress_ts
            if cancel_flag.is_set():
                return False
            if on_progress and kind == "tick":
                now = time.perf_counter()
                if now - progress_ts >= 1.0:
                    progress_ts = now
                    on_progress(kind, payload if isinstance(payload, dict) else {})
            return True

        try:
            if use_stream and ps:
                on_log(f"[parse] Bruker STREAM‑parser (versjon {vs or '?'})")
                parser_name = f"stream:{vs or '?'}"
                ps(input_file, csv_dir, on_progress=_cb)  # type: ignore
            elif pp:
                on_log(f"[parse] Bruker FALLBACK‑parser (versjon {vp or '?'})")
                parser_name = f"fallback:{vp or '?'}"
                pp(input_file, csv_dir)  # type: ignore
            else:
                raise RuntimeError("Ingen parser tilgjengelig.")
        except Exception as e:
            on_log(f"[parse] FEIL: {e!r}"); raise

        # mapping-probe
        self._run_mapping_probe(input_file, csv_dir, on_log)

        # profil
        prof_path = self._build_profile(csv_dir, input_file)
        on_log(f"[profil] Skrevet: {prof_path}")

        # rapport/eksport
        if do_excel:
            self._run_excel_pipeline_with_profile(csv_dir, excel_dir, prof_path, on_log, make_excel=True)

        # run_meta
        finished = time.time()
        meta = {
            "input_file": str(input_file),
            "job_root": str(job_root),
            "started_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(started)),
            "finished_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(finished)),
            "duration_sec": round(finished - started, 3),
            "parser": parser_name,
        }
        (csv_dir / "run_meta.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")

        return csv_dir, excel_dir, prof_path


class App(tk.Tk):
    def __init__(self, base_dir: Path):
        super().__init__()
        self.title("SAF‑T Parser – Pro GUI")
        self.geometry("920x640")
        self.minsize(880, 600)

        self.engine = Engine(base_dir)
        self.cancel_flag = threading.Event()
        self.worker: Optional[threading.Thread] = None
        self.log_lines: list[str] = []

        self._build_ui()

    def _build_ui(self):
        nb = ttk.Notebook(self); nb.pack(fill="both", expand=True)

        tab1 = ttk.Frame(nb, padding=12); nb.add(tab1, text="Innstillinger")
        tab2 = ttk.Frame(nb, padding=8);  nb.add(tab2, text="Fremdrift")
        tab3 = ttk.Frame(nb, padding=12); nb.add(tab3, text="Profil")

        self.var_input = tk.StringVar(); self.var_out = tk.StringVar()
        self.var_stream = tk.BooleanVar(value=True)
        self.var_raw = tk.BooleanVar(value=False)
        self.var_excel = tk.BooleanVar(value=True)
        self.var_progress = tk.IntVar(value=50000)

        r = 0
        ttk.Label(tab1, text="Input-fil (.xml/.zip):").grid(column=0, row=r, sticky="w")
        ttk.Entry(tab1, textvariable=self.var_input, width=80).grid(column=1, row=r, sticky="we", padx=6)
        ttk.Button(tab1, text="Bla …", command=self._choose_file).grid(column=2, row=r, sticky="w"); r+=1

        ttk.Label(tab1, text="Output-mappe (valgfri):").grid(column=0, row=r, sticky="w", pady=(8,0))
        ttk.Entry(tab1, textvariable=self.var_out, width=80).grid(column=1, row=r, sticky="we", padx=6, pady=(8,0))
        ttk.Button(tab1, text="Velg …", command=self._choose_out).grid(column=2, row=r, sticky="w", pady=(8,0)); r+=1

        opts = ttk.Frame(tab1); opts.grid(column=0, row=r, columnspan=3, sticky="we", pady=10)
        ttk.Checkbutton(opts, text="Bruk streaming‑parser", variable=self.var_stream).pack(side="left")
        ttk.Checkbutton(opts, text="Skriv rådump (raw_elements.csv)", variable=self.var_raw).pack(side="left", padx=(16,0))
        ttk.Checkbutton(opts, text="Generer Excel/rapporter", variable=self.var_excel).pack(side="left", padx=(16,0)); r+=1

        ttk.Label(tab1, text="Progress-intervall (events):").grid(column=0, row=r, sticky="w")
        ttk.Entry(tab1, textvariable=self.var_progress, width=10).grid(column=1, row=r, sticky="w"); r+=1

        sep = ttk.Separator(tab1); sep.grid(column=0, row=r, columnspan=3, sticky="we", pady=8); r+=1
        btns = ttk.Frame(tab1); btns.grid(column=0, row=r, columnspan=3, sticky="e")
        self.btn_start = ttk.Button(btns, text="Start", command=self._start); self.btn_start.pack(side="left", padx=6)
        self.btn_cancel = ttk.Button(btns, text="Avbryt", command=self._cancel, state="disabled"); self.btn_cancel.pack(side="left")
        tab1.columnconfigure(1, weight=1)

        self.pb = ttk.Progressbar(tab2, mode="indeterminate"); self.pb.pack(fill="x", pady=4)
        self.txt = tk.Text(tab2, height=20); self.txt.pack(fill="both", expand=True)
        bar2 = ttk.Frame(tab2); bar2.pack(fill="x", pady=6)
        ttk.Button(bar2, text="Lagre logg …", command=self._save_log).pack(side="left")
        ttk.Button(bar2, text="Åpne CSV‑mappe", command=self._open_csv).pack(side="right")
        ttk.Button(bar2, text="Åpne Excel‑mappe", command=self._open_excel).pack(side="right", padx=(0,6))

        self.lbl_prof = tk.Text(tab3, height=20); self.lbl_prof.pack(fill="both", expand=True)

        self.csv_dir: Optional[Path] = None
        self.excel_dir: Optional[Path] = None
        self.prof_path: Optional[Path] = None

        self.after(250, self._poll)

    def _choose_file(self):
        p = filedialog.askopenfilename(
            title="Velg SAF‑T‑fil",
            filetypes=[("SAF‑T XML/ZIP", "*.xml *.zip"), ("Alle filer", "*.*")]
        )
        if p: self.var_input.set(p)

    def _choose_out(self):
        d = filedialog.askdirectory(title="Velg output‑mappe")
        if d: self.var_out.set(d)

    def _log(self, s: str):
        self.log_lines.append(s)
        try:
            self.txt.insert("end", s + "\n"); self.txt.see("end")
        except Exception:
            pass

    def _on_progress(self, _kind: str, payload: dict):
        rate = payload.get("rate_events_per_sec", 0.0)
        events = payload.get("events", 0)
        csv_rows = payload.get("csv_rows", {})
        top = payload.get("top_times", [])
        top_str = ", ".join([f"{k}:{v:.1f}s" for k, v in top[:3]])
        rows_str = ", ".join([f"{k}:{v}" for k, v in sorted(csv_rows.items())])
        self._log(f"[progress] events={events:,}  rate={rate:,.0f}/s  rows[{rows_str}]  top={top_str}")

    def _start(self):
        ip = self.var_input.get().strip()
        if not ip:
            messagebox.showwarning("Mangler fil", "Velg en SAF‑T‑fil (.xml/.zip) først."); return
        out = self.var_out.get().strip() or None

        self.cancel_flag.clear()
        self.btn_start.config(state="disabled"); self.btn_cancel.config(state="normal")
        self.pb.start(40); self.txt.delete("1.0", "end"); self.lbl_prof.delete("1.0", "end"); self.log_lines.clear()

        def _worker():
            try:
                eng = self.engine
                csv_dir, excel_dir, prof_path = eng.run(
                    Path(ip),
                    Path(out) if out else None,
                    use_stream=self.var_stream.get(),
                    write_raw=self.var_raw.get(),
                    do_excel=self.var_excel.get(),
                    progress_every=int(self.var_progress.get()),
                    on_progress=self._on_progress,
                    on_log=self._log,
                    cancel_flag=self.cancel_flag
                )
                self.csv_dir, self.excel_dir, self.prof_path = csv_dir, excel_dir, prof_path
                try:
                    d = json.loads(prof_path.read_text(encoding="utf-8"))
                    obs = []
                    obs.append(f"AuditFileVersion: {d.get('file_info',{}).get('audit_file_version','') or '(ukjent)'}")
                    cls = d.get("classification",{}).get("profile_id","(ukjent)")
                    obs.append(f"Klassifisering: {cls}")
                    counts = d.get("counts",{})
                    obs.append(f"AR-linjer: {counts.get('ar_line_count',0):,}  |  AP-linjer: {counts.get('ap_line_count',0):,}")
                    obs.append(f"Transaksjoner: {counts.get('transactions',0):,}  |  Kunder (master): {counts.get('customers',0):,}  |  Leverandører (master): {counts.get('suppliers',0):,}")
                    jdist = d.get("journal_type_distribution",{})
                    if jdist and not jdist.get("not_available"):
                        obs.append(f"Journal-typer: {jdist}")
                    findings = d.get("compliance_findings",[])
                    if findings:
                        obs.append("\nFunn:")
                        for f in findings: obs.append(f" - {f.get('severity')} {f.get('rule_id')}: {f.get('message')}")
                    self.lbl_prof.insert("end", "\n".join(obs))
                except Exception as e:
                    self.lbl_prof.insert("end", f"(Kunne ikke lese profil: {e!r})")
                self._log("[gui] Ferdig.")
            except Exception as e:
                self._log(f"[gui] STOPPET: {e!r}")
            finally:
                self.pb.stop(); self.btn_cancel.config(state="disabled"); self.btn_start.config(state="normal")

        self.worker = threading.Thread(target=_worker, daemon=True); self.worker.start()

    def _cancel(self):
        self.cancel_flag.set(); self._log("[gui] Avbryt forespurt …")

    def _save_log(self):
        if not self.log_lines:
            messagebox.showinfo("Ingen logg", "Ingen logglinjer å lagre ennå."); return
        p = filedialog.asksaveasfilename(title="Lagre logg", defaultextension=".txt", filetypes=[("Tekst","*.txt")])
        if p: Path(p).write_text("\n".join(self.log_lines), encoding="utf-8"); messagebox.showinfo("Lagret", f"Logg lagret til:\n{p}")

    def _open_csv(self):
        if self.csv_dir: _open_dir(self.csv_dir)

    def _open_excel(self):
        if self.excel_dir: _open_dir(self.excel_dir)

    def _poll(self):
        self.after(250, self._poll)


def launch():
    base = Path(__file__).resolve().parent
    app = App(base); app.mainloop()


if __name__ == "__main__":
    launch()
