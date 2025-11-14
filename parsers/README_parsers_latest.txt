Dette er de siste .py-filene vi har jobbet med (klar for copy/paste til repo):

- src/app/parsers/saft_vat_report.py
  * VAT_By_Term og VAT_By_Term_Base (terminpivot), inkl. forsøk på å hente SAF-T mappingkode/ navn hvis en mapping-fil finnes.

- src/app/parsers/saft_gl_monthly.py
  * Pivotert hovedbok pr. måned til egen fil (gl_monthly.xlsx).

- src/app/parsers/saft_subledger.py
  * Robust kontrollkontodetektering (mapping -> gjetting -> prefiks), ingen MVA-faner, Accounting-format, SUM-rader 2 linjer under.

- src/app/parsers/saft_reports.py
  * Orkestrering: Etter AP genereres også vat_report.xlsx og gl_monthly.xlsx i try/except.