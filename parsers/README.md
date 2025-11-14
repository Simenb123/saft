
## AR/AP saldoliste
`ar_ap_saldolist.py` lager en samlet arbeidsbok **ar_ap_saldolist.xlsx** med ark:
- `Customers_UB` (hentet fra AR‑subledgerens `Balances`‑ark)
- `Suppliers_UB` (hentet fra AP‑subledgerens `Balances`‑ark)
- `Recon` (sammenligning av sum UB i subledger mot sum UB på kontrollkontoer i TB)

Den kaller fasaden `saft_reports` for å sikre at underliggende filer er generert.
