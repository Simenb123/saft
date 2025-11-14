# -*- coding: utf-8 -*-
"""
Root-pakke for SAFT-prosjektet.

Denne fila skal **ikke** gjøre tunge imports, fordi den blir evaluert
hver gang noe importerer `saft` eller moduler under prosjektet.

All funksjonell logikk ligger i moduler som:
- parsers/
- saft_pro_gui.py
- ui_main.py
"""

from __future__ import annotations

# Ingen automatisk import her – vi holder pakken "lett".
__all__: list[str] = []
