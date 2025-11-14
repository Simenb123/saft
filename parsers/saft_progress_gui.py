# -*- coding: utf-8 -*-
"""
gui/saft_progress_gui.py – Enkel GUI for live visning av parsing

- Filvelger for .xml/.zip
- Start/Avbryt
- Live oppdatering av events/sek, topp tidskategorier og antall rader skrevet
- Flagg: Stream (på), Skriv rådump (default AV), Generer Excel (på)
"""
from __future__ import annotations
import threading, queue, os, sys
from pathlib import Path
from typing import Optional, Any

try:
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox
except Exception as e:
    raise RuntimeError("Tkinter er påkrevd for GUI.")

def _import_parsers(base_dir: Path):
    import importlib.util
    parse_stream = parse_pro = None
    ver_stream = ver_pro = None

    def _load(name: str, path: Path):
        spec = importlib.util.spec_from_file_location(name, str(path))
        if spec and spec.loader:
            m = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(m)  # type: ignore
            return m
        return None

    p_stream = base_dir / "saft_stream_parser.py"
    p_pro    = base_dir / "saft_parser_pro.py"
    if p_stream.exists():
        m = _load("_local_s_stream", p_stream)
        if m: parse_stream, ver_stream = getattr(m,"parse_saft",None), getattr(m,"__version__",None)
    if p_pro.exists():
        m = _load("_local_s_pro", p_pro)
        if m: parse_pro, ver_pro = getattr(m,"parse_saft",None), getattr(m,"__version__",None)

    if not parse_stream or not parse_pro:
        # pakkeimport
        try:
            import app.parsers.saft_stream_parser as m1  # type: ignore
            parse_stream, ver_stream = getattr(m1,"parse_saft",None), getattr(m1,"__version__",None)
        except Exception: pass
        try:
            import app.parsers.saft_parser_pro as m2  # type: ignore
            parse_pro, ver_pro = getattr(m2,"parse_saft",None), getattr(m2,"__version__",None)
        except Exception: pass

    return parse_stream, ver_stream, parse_pro, ver_pro

class ProgressWindow(tk.Tk):
    def __init__(self, base_dir: Path) -> None:
        super().__init__()
        self.title("SAF‑T Parser – Live")
        self.geometry("760x480")
        self.resizable(True, False)
        self.base_dir = base_dir

        self.parse_stream, self.ver_stream, self.parse_pro, self.ver_pro = _import_parsers(base_dir)

        self.input_path: Optional[Path] = None
        self.outdir: Optional[Path] = None
        self.cancelled = False
        self.q: "queue.Queue[tuple[str, Any]]" = queue.Queue()

        frm = ttk.Frame(self, padding=12); frm.pack(fill="both", expand=True)

        row = 0
        ttk.Label(frm, text="Input‑fil (.xml/.zip):").grid(column=0, row=row, sticky="w")
        self.ent_input = ttk.Entry(frm, width=80); self.ent_input.grid(column=1, row=row, sticky="we", padx=6)
        ttk.Button(frm, text="Bla...", command=self._choose_file).grid(column=2, row=row)
        row += 1

        ttk.Label(frm, text="Output‑mappe (valgfri):").grid(column=0, row=row, sticky="w")
        self.ent_out = ttk.Entry(frm, width=80); self.ent_out.grid(column=1, row=row, sticky="we", padx=6)
        ttk.Button(frm, text="Velg...", command=self._choose_folder).grid(column=2, row=row)
        row += 1

        self.var_stream = tk.BooleanVar(value=True)
        self.var_raw = tk.BooleanVar(value=False)   # default OFF
        self.var_excel = tk.BooleanVar(value=True)
        self.var_progress_every = tk.IntVar(value=50000)
        ttk.Checkbutton(frm, text="Bruk streaming‑parser", variable=self.var_stream).grid(column=0, row=row, sticky="w")
        ttk.Checkbutton(frm, text="Skriv rådump (raw_elements.csv)", variable=self.var_raw).grid(column=1, row=row, sticky="w")
        ttk.Checkbutton(frm, text="Kjør Excel/rapporter etter parsing", variable=self.var_excel).grid(column=2, row=row, sticky="w")
        row += 1

        ttk.Label(frm, text="Progress‑intervall (events):").grid(column=0, row=row, sticky="w")
        ttk.Entry(frm, textvariable=self.var_progress_every, width=12).grid(column=1, row=row, sticky="w")
        row += 1

        self.pb = ttk.Progressbar(frm, mode="indeterminate"); self.pb.grid(column=0, row=row, columnspan=3, sticky="we", pady=8)
        row += 1

        self.txt = tk.Text(frm, height=14); self.txt.grid(column=0, row=row, columnspan=3, sticky="we")
        row += 1

        btns = ttk.Frame(frm); btns.grid(column=0, row=row, columnspan=3, sticky="e", pady=8)
        self.btn_start = ttk.Button(btns, text="Start", command=self._start)
        self.btn_start.pack(side="left", padx=6)
        self.btn_cancel = ttk.Button(btns, text="Avbryt", command=self._cancel, state="disabled")
        self.btn_cancel.pack(side="left")
        row += 1

        frm.columnconfigure(1, weight=1)
        self.after(200, self._poll_queue)

    def _log(self, s: str): self.txt.insert("end", s + "\n"); self.txt.see("end")

    def _choose_file(self):
        p = filedialog.askopenfilename(title="Velg SAF‑T‑fil", filetypes=[("SAF‑T XML/ZIP","*.xml *.zip"),("Alle filer","*.*")])
        if p:
            self.ent_input.delete(0,"end"); self.ent_input.insert(0, p)

    def _choose_folder(self):
        d = filedialog.askdirectory(title="Velg output‑mappe")
        if d:
            self.ent_out.delete(0,"end"); self.ent_out.insert(0, d)

    def _progress_cb(self, kind: str, payload: object):
        if kind == "tick":
            d = payload if isinstance(payload, dict) else {}
            rate = d.get("rate_events_per_sec", 0.0)
            events = d.get("events", 0)
            rows = d.get("csv_rows", {})
            top = d.get("top_times", [])
            top_str = ", ".join([f"{k}:{v:.1f}s" for k, v in top[:3]])
            rows_str = ", ".join([f"{k}:{v}" for k,v in sorted(rows.items())])
            self.q.put(("log", f"[progress] events={events:,} rate={rate:,.0f}/s rows[{rows_str}] top={top_str}"))
        return (not self.cancelled)

    def _start(self):
        ip = self.ent_input.get().strip()
        if not ip:
            messagebox.showwarning("Mangler fil", "Velg en SAF‑T‑fil (.xml/.zip)"); return
        self.input_path = Path(ip)
        out = self.ent_out.get().strip()
        self.outdir = Path(out) if out else None

        os.environ["SAFT_WRITE_RAW"] = "1" if self.var_raw.get() else "0"
        os.environ["SAFT_PROGRESS_EVENTS"] = str(int(self.var_progress_every.get()))

        self.btn_start.config(state="disabled")
        self.btn_cancel.config(state="normal")
        self.pb.start(50)
        self._log("Starter parsing ...")

        import threading
        t = threading.Thread(target=self._worker, daemon=True)
        t.start()

    def _cancel(self):
        self.cancelled = True
        self._log("Avbryt forespurt – stopper ved neste milepæl ...")

    def _worker(self):
        try:
            csv_dir = None
            if self.var_stream.get() and self.parse_stream:
                self._log(f"Bruker streaming‑parser (versjon {self.ver_stream or '?'})")
                try:
                    self.parse_stream(self.input_path, self.outdir or (self.input_path.parent/"_SAFT_OUT"), on_progress=self._progress_cb)  # type: ignore
                    csv_dir = (self.outdir or (self.input_path.parent/"_SAFT_OUT"))
                except Exception as e:
                    self._log(f"Streaming‑parser feilet: {e!r}")
            if csv_dir is None and self.parse_pro:
                self._log(f"Bruker fallback‑parser (versjon {self.ver_pro or '?'})")
                self.parse_pro(self.input_path, self.outdir or (self.input_path.parent/"_SAFT_OUT"))  # type: ignore
                csv_dir = (self.outdir or (self.input_path.parent/"_SAFT_OUT"))
            if csv_dir and self.var_excel.get():
                try:
                    import importlib
                    mod = importlib.import_module("app.parsers.saft_controls_and_exports")
                    self._log("Genererer Excel/rapporter ...")
                    if hasattr(mod, "make_general_ledger"): mod.make_general_ledger(csv_dir)  # type: ignore
                    if hasattr(mod, "make_subledger"):
                        mod.make_subledger(csv_dir, "AR")  # type: ignore
                        mod.make_subledger(csv_dir, "AP")  # type: ignore
                    if hasattr(mod, "make_trial_balance"): mod.make_trial_balance(csv_dir)  # type: ignore
                except Exception as e:
                    self._log(f"Excel/rapporter feilet: {e!r}")
            self.q.put(("done", "ok"))
        except Exception as e:
            self.q.put(("done", f"error: {e!r}"))

    def _poll_queue(self):
        try:
            import queue as _q
            while True:
                kind, payload = self.q.get_nowait()
                if kind == "log":
                    self._log(str(payload))
                elif kind == "done":
                    self.pb.stop()
                    self.btn_cancel.config(state="disabled")
                    self._log("Ferdig." if payload=="ok" else f"Stoppet: {payload}")
        except Exception:
            pass
        self.after(250, self._poll_queue)

def launch():
    base = Path(__file__).resolve().parent
    app = ProgressWindow(base)
    app.mainloop()

if __name__ == "__main__":
    launch()