# -*- coding: utf-8 -*-
"""
Genererer dokumentasjon fra koden:
  python -m app.parsers.saft_parser_docs --out docs

Skriver docs/SAFT_PARSER_DESIGN.md med alias-tabell og designvalg.
"""
from __future__ import annotations
from pathlib import Path
import argparse, inspect, textwrap, json

try:
    from .saft_stream_parser import ALIAS, INDICATOR_ALIAS  # type: ignore
except Exception:
    try:
        from app.parsers.saft_stream_parser import ALIAS, INDICATOR_ALIAS  # type: ignore
    except Exception:
        ALIAS, INDICATOR_ALIAS = {}, []

DOC = """
# SAF‑T Parser – Design og “aldri glem”-regler

Denne dokumentasjonen er **generert fra kildekode** slik at den ikke går ut på dato.

## Arkitektur (streaming)

- Vi bruker `lxml.etree.iterparse()` med `events=("start","end")`.
- Minnehygiene: `el.clear()` + fjerning av tidligere søsken fra parent.
- CSV‑skriving med 1 MB buffer per fil for ytelse.
- ZIP dekomprimeres **on‑the‑fly** (vi streamer fra arkivet).

## Feltdeteksjon

Vi finner verdier som **tekst eller attributt** med BFS i begrenset dybde.

1. **ID‑er og metadata** (AccountID, CustomerID, SupplierID, DocumentNumber/ReferenceNumber, SourceDocumentID, …) – vi sjekker aliasnavn.
2. **Beløp**:
   - **Wrapped**: `<DebitAmount><Amount>…</Amount></DebitAmount>` og `<CreditAmount>…</CreditAmount>`.
   - **Separate**: `DebitAmount`/`CreditAmount` direkte (tekst/attrib).
   - **Enkeltbeløp**: `Amount` + `DebitCreditIndicator` (på Amount eller Line); uten indikator bruker vi fortegn (negativ = credit).
   - **Attrib**: Støtter `Debit`/`Credit`/`Amount` som **attributter** på `Line`.
3. **Dato**:
   - `PostingDate` fra `Transaction`, **fallback** til `ValueDate` på `Line` hvis tom.

## Alias (fra koden)

```json
{alias_json}
