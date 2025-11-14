# -*- coding: utf-8 -*-
from __future__ import annotations
"""
SAF‑T (NO) parser – v1.3‑klar, streaming fallback

Denne filen fungerer som en trygg "pro-fallback" dersom streamingparser feiler.
Den er konsistent med saft_stream_parser hva gjelder CSV‑kolonner, og inneholder
de viktigste feilrettingene:
- AnalysisAmount (v1.3): støtter DebitAnalysisAmount/CreditAnalysisAmount.
- gl_totals.csv skrives.
- missing_accounts.csv skrives.
- Fikser feilaktige feltnavn som tidligere var beskåret med "...".
"""
from pathlib import Path
from typing import Optional, Dict, Iterable, Tuple
from decimal import Decimal, InvalidOperation
import csv, io, os, json, zipfile

try:
    from lxml import etree
except Exception as e:
    raise RuntimeError("saft_parser_pro krever lxml. Installer: pip install lxml") from e

__version__ = "2025.10.22-pro-fallback"

def _lname(tag_or_el) -> str:
    t = tag_or_el.tag if hasattr(tag_or_el, "tag") else str(tag_or_el)
    return t.split('}', 1)[-1] if '}' in t else t

def _text(node) -> Optional[str]:
    if node is None or node.text is None: return None
    t = node.text.strip()
    return t if t else None

def _norm_amount_str(s: str) -> str:
    s = s.replace('\u00A0','').strip()
    if ',' in s and '.' in s:
        s = s.replace(',', '')
    elif ',' in s:
        s = s.replace('.', '').replace(',', '.')
    else:
        parts = s.split('.')
        if len(parts) > 2: s = s.replace('.', '')
    return s.replace(' ','')

def _to_dec(txt: Optional[str]) -> Optional[Decimal]:
    if not txt: return None
    try: return Decimal(_norm_amount_str(txt))
    except (InvalidOperation, ValueError): return None

def _amount_of(node, key: str) -> Optional[Decimal]:
    for ch in node.iter():
        if _lname(ch) == key:
            v = _text(ch)
            if v:
                d = _to_dec(v)
                if d is not None: return d
            for sub in ch.iter():
                if sub is not ch and _lname(sub) == "Amount":
                    v2 = _text(sub)
                    if v2:
                        d = _to_dec(v2)
                        if d is not None: return d
                    break
            av = ch.get("Amount")
            if av:
                d = _to_dec(av)
                if d is not None: return d
    return None

def _first(el, keys: Iterable[str]) -> Optional[str]:
    if isinstance(keys, (str, bytes)): keys = [keys]  # type: ignore[assignment]
    for k in keys:
        for ch in el.iter():
            if _lname(ch) == k:
                t = _text(ch)
                if t: return t
    return None

def _open_input(p: Path):
    p = Path(p)
    if p.suffix.lower() == ".zip":
        zf = zipfile.ZipFile(p, "r")
        names = [n for n in zf.namelist() if n.lower().endswith(".xml")]
        if not names:
            zf.close()
            raise RuntimeError("Ingen .xml i zip-arkivet")
        st = zf.open(names[0], "r")
        def closer():
            try: st.close()
            finally: zf.close()
        return st, closer
    fh = open(p, "rb")
    return fh, fh.close

def _open_writer(path: Path, headers: Iterable[str]):
    f = open(path, "w", newline="", encoding="utf-8")
    w = csv.DictWriter(f, fieldnames=list(headers))
    w.writeheader()
    return w, f

def parse_saft(input_path: Path, outdir: Path, *, on_progress=None) -> None:
    od = Path(outdir); od.mkdir(parents=True, exist_ok=True)
    write_raw = os.getenv("SAFT_WRITE_RAW", "1").strip().lower() not in ("0","false","no")

    w_header,f_header = _open_writer(od/"header.csv", (
        "CompanyName","CompanyID",
        "FunctionalCurrency","DefaultCurrencyCode",
        "FileCreationDate","AuditFileVersion",
        "SelectionStart","SelectionStartDate","SelectionEnd","SelectionEndDate",
        "StartDate","EndDate","ProductVersion","SoftwareCertificateNumber"
    ))
    w_accounts,f_accounts = _open_writer(od/"accounts.csv", (
        "AccountID","AccountDescription","AccountType","ParentAccountID",
        "GroupingCategory","GroupingCode",
        "OpeningDebit","OpeningCredit","ClosingDebit","ClosingCredit","TaxCode","TaxType"
    ))
    w_tax,f_tax = _open_writer(od/"tax_table.csv", (
        "TaxCode","StandardTaxCode","TaxType","TaxPercentage","TaxCountryRegion","Description"
    ))
    w_cust,f_cust = _open_writer(od/"customers.csv", (
        "CustomerID","Name","VATNumber","Country","City","PostalCode","Email","Telephone"
    ))
    w_supp,f_supp = _open_writer(od/"suppliers.csv", (
        "SupplierID","Name","VATNumber","Country","City","PostalCode","Email","Telephone"
    ))
    w_arap,f_arap = _open_writer(od/"arap_control_accounts.csv", (
        "PartyType","PartyID","AccountID","OpeningDebit","OpeningCredit","ClosingDebit","ClosingCredit"
    ))
    w_vouch,f_vouch = _open_writer(od/"vouchers.csv", (
        "VoucherID","VoucherNo","TransactionDate","PostingDate","Period","Year",
        "SourceDocumentID","JournalID","CurrencyCode",
        "VoucherType","VoucherDescription","ModificationDate",
        "DebitTotal","CreditTotal","Balanced"
    ))
    w_lines,f_lines = _open_writer(od/"transactions.csv", (
        "RecordID","VoucherID","VoucherNo","JournalID","TransactionDate","PostingDate","Period","Year",
        "SystemID","BatchID","DocumentNumber","SourceDocumentID",
        "AccountID","AccountDescription",
        "CustomerID","CustomerName","CustomerVATNumber",
        "SupplierID","SupplierName","SupplierVATNumber",
        "Description","Debit","Credit","Amount",
        "CurrencyCode","AmountCurrency","ExchangeRate",
        "TaxType","TaxCountryRegion","TaxCode","TaxPercentage",
        "DebitTaxAmount","CreditTaxAmount","TaxAmount",
        "IsGL","SourceType"
    ))
    w_anl,f_anl = _open_writer(od/"analysis_lines.csv", ("RecordID","Type","ID","Amount"))
    w_sinv,f_sinv = _open_writer(od/"sales_invoices.csv", (
        "InvoiceNo","InvoiceDate","TaxPointDate","GLPostingDate",
        "CustomerID","CustomerName","CustomerVATNumber",
        "CurrencyCode","NetTotal","TaxPayable","GrossTotal","SourceID","DocumentNumber","DueDate"
    ))
    w_pinv,f_pinv = _open_writer(od/"purchase_invoices.csv", (
        "InvoiceNo","InvoiceDate","TaxPointDate","GLPostingDate",
        "SupplierID","SupplierName","SupplierVATNumber",
        "CurrencyCode","NetTotal","TaxPayable","GrossTotal","SourceID","DocumentNumber","DueDate"
    ))
    w_raw,f_raw = (None, None)
    if write_raw:
        w_raw,f_raw = _open_writer(od/"raw_elements.csv", ("XPath","Tag","Text","Attributes"))
    w_jtot,f_jtot = _open_writer(od/"gl_totals.csv", ("JournalID","TotalDebit","TotalCredit"))

    accounts: Dict[str, Dict[str,str]] = {}
    customers: Dict[str, Dict[str,str]] = {}
    suppliers: Dict[str, Dict[str,str]] = {}
    customer_ctrl: Dict[str,str] = {}
    supplier_ctrl: Dict[str,str] = {}
    seen_accounts_in_lines = set()

    fh, closer = _open_input(Path(input_path))
    try:
        ctx = etree.iterparse(fh, events=("start","end"), huge_tree=True)
        cur_voucher = None
        for evt, el in ctx:
            tag = _lname(el)

            if write_raw and evt == "end" and w_raw is not None:
                try:
                    xp = el.getroottree().getpath(el)
                except Exception:
                    xp = f"/{tag}"
                w_raw.writerow({"XPath": xp, "Tag": tag, "Text": (_text(el) or ""), "Attributes": json.dumps(dict(el.attrib or {}), ensure_ascii=False)})

            if evt == "end" and tag == "Header":
                w_header.writerow({
                    "CompanyName": _first(el, ("CompanyName",)) or "",
                    "CompanyID": _first(el, ("CompanyID",)) or "",
                    "FunctionalCurrency": _first(el, ("FunctionalCurrency","DefaultCurrencyCode")) or "",
                    "DefaultCurrencyCode": _first(el, ("DefaultCurrencyCode",)) or "",
                    "FileCreationDate": _first(el, ("FileCreationDateTime","AuditFileDateCreated","FileCreationDate")) or "",
                    "AuditFileVersion": _first(el, ("AuditFileVersion",)) or "",
                    "SelectionStart": _first(el, ("SelectionStart","SelectionStartDate")) or "",
                    "SelectionStartDate": _first(el, ("SelectionStartDate",)) or "",
                    "SelectionEnd": _first(el, ("SelectionEnd","SelectionEndDate")) or "",
                    "SelectionEndDate": _first(el, ("SelectionEndDate",)) or "",
                    "StartDate": _first(el, ("StartDate",)) or "",
                    "EndDate": _first(el, ("EndDate",)) or "",
                    "ProductVersion": _first(el, ("ProductVersion",)) or "",
                    "SoftwareCertificateNumber": _first(el, ("SoftwareCertificateNumber",)) or "",
                })

            if evt == "end" and tag in ("Account","GeneralLedgerAccount"):
                acc_id = _first(el, ("AccountID","GLAccountID"))
                if acc_id:
                    acc_desc= _first(el, ("AccountDescription","Description"))
                    acc_type= _first(el, ("AccountType",))
                    parent  = _first(el, ("ParentAccountID","ParentID"))
                    group_cat = _first(el, ("GroupingCategory",))
                    group_code= _first(el, ("GroupingCode","GroupingCategoryCode"))
                    op_dr = _amount_of(el, "OpeningDebitBalance") or Decimal("0")
                    op_cr = _amount_of(el, "OpeningCreditBalance") or Decimal("0")
                    cl_dr = _amount_of(el, "ClosingDebitBalance") or Decimal("0")
                    cl_cr = _amount_of(el, "ClosingCreditBalance") or Decimal("0")
                    taxc  = _first(el, ("TaxCode",))
                    taxt  = _first(el, ("TaxType",))
                    accounts[acc_id] = {"AccountDescription": acc_desc or "", "TaxCode": taxc or ""}
                    w_accounts.writerow({
                        "AccountID": acc_id, "AccountDescription": acc_desc or "", "AccountType": acc_type or "",
                        "ParentAccountID": parent or "",
                        "GroupingCategory": group_cat or "", "GroupingCode": group_code or "",
                        "OpeningDebit": f"{op_dr}", "OpeningCredit": f"{op_cr}",
                        "ClosingDebit": f"{cl_dr}", "ClosingCredit": f"{cl_cr}",
                        "TaxCode": taxc or "", "TaxType": taxt or "",
                    })
                el.clear()
                while el.getprevious() is not None:
                    del el.getparent()[0]

            if evt == "end" and tag == "TaxTableEntry":
                w_tax.writerow({
                    "TaxCode": _first(el, ("TaxCode",)) or "",
                    "StandardTaxCode": _first(el, ("StandardTaxCode",)) or "",
                    "TaxType": _first(el, ("TaxType",)) or "",
                    "TaxPercentage": _first(el, ("TaxPercentage","Rate")) or "",
                    "TaxCountryRegion": _first(el, ("TaxCountryRegion","CountryRegion")) or "",
                    "Description": _first(el, ("Description",)) or "",
                })
                el.clear()
                while el.getprevious() is not None:
                    del el.getparent()[0]

            if evt == "end" and tag == "Customer":
                cid = _first(el, ("CustomerID","ID"))
                if cid:
                    name = _first(el, ("CompanyName","CustomerName","Name"))
                    customers[cid] = {"Name": name or "", "VATNumber": _first(el, ("VATNumber","VATRegistrationNumber")) or ""}
                    w_cust.writerow({
                        "CustomerID": cid, "Name": name or "", "VATNumber": customers[cid]["VATNumber"],
                        "Country": _first(el, ("Country",)) or "", "City": _first(el, ("City",)) or "",
                        "PostalCode": _first(el, ("PostalCode",)) or "",
                        "Email": _first(el, ("Email",)) or "", "Telephone": _first(el, ("Telephone","MobilePhone")) or ""
                    })
                    for b in el.findall(".//*"):
                        if _lname(b) == "BalanceAccountStructure":
                            acct = _first(b, ("AccountID",))
                            if acct:
                                customer_ctrl[cid] = acct
                            w_arap.writerow({
                                "PartyType":"Customer","PartyID":cid,
                                "AccountID": acct or "",
                                "OpeningDebit": f"{_amount_of(b,'OpeningDebitBalance') or Decimal('0')}",
                                "OpeningCredit": f"{_amount_of(b,'OpeningCreditBalance') or Decimal('0')}",
                                "ClosingDebit": f"{_amount_of(b,'ClosingDebitBalance') or Decimal('0')}",
                                "ClosingCredit": f"{_amount_of(b,'ClosingCreditBalance') or Decimal('0')}",
                            })
                el.clear()
                while el.getprevious() is not None:
                    del el.getparent()[0]

            if evt == "end" and tag == "Supplier":
                sid = _first(el, ("SupplierID","ID"))
                if sid:
                    name = _first(el, ("CompanyName","SupplierName","Name"))
                    suppliers[sid] = {"Name": name or "", "VATNumber": _first(el, ("VATNumber","VATRegistrationNumber")) or ""}
                    w_supp.writerow({
                        "SupplierID": sid, "Name": name or "", "VATNumber": suppliers[sid]["VATNumber"],
                        "Country": _first(el, ("Country",)) or "", "City": _first(el, ("City",)) or "",
                        "PostalCode": _first(el, ("PostalCode",)) or "",
                        "Email": _first(el, ("Email",)) or "", "Telephone": _first(el, ("Telephone","MobilePhone")) or ""
                    })
                    for b in el.findall(".//*"):
                        if _lname(b) == "BalanceAccountStructure":
                            acct = _first(b, ("AccountID",))
                            if acct:
                                supplier_ctrl[sid] = acct
                            w_arap.writerow({
                                "PartyType":"Supplier","PartyID":sid,
                                "AccountID": acct or "",
                                "OpeningDebit": f"{_amount_of(b,'OpeningDebitBalance') or Decimal('0')}",
                                "OpeningCredit": f"{_amount_of(b,'OpeningCreditBalance') or Decimal('0')}",
                                "ClosingDebit": f"{_amount_of(b,'ClosingDebitBalance') or Decimal('0')}",
                                "ClosingCredit": f"{_amount_of(b,'ClosingCreditBalance') or Decimal('0')}",
                            })
                el.clear()
                while el.getprevious() is not None:
                    del el.getparent()[0]

            if evt == "end" and tag == "Journal":
                j_id = _first(el, ("JournalID","Journal"))
                tdr  = _amount_of(el, "TotalDebit")  or Decimal("0")
                tcr  = _amount_of(el, "TotalCredit") or Decimal("0")
                if j_id or (tdr or tcr):
                    w_jtot.writerow({"JournalID": j_id or "", "TotalDebit": f"{tdr}", "TotalCredit": f"{tcr}"})
                el.clear()
                while el.getprevious() is not None:
                    del el.getparent()[0]

            if evt == "start" and tag == "Transaction":
                cur = {
                    "voucher": None
                }
                cur_voucher = {
                    "voucher_id": _first(el, ("TransactionID","TransactionNo","VoucherID","EntryID")),
                    "voucher_no": _first(el, ("VoucherNo","TransactionNo","TransactionID","EntryNumber")),
                    "transaction_date": _first(el, ("TransactionDate","EntryDate")),
                    "posting_date": _first(el, ("PostingDate","GLDate","ValueDate")),
                    "period": _first(el, ("Period",)),
                    "year": _first(el, ("FiscalYear","Year")),
                    "source_doc": _first(el, ("SourceDocumentID","SourceID","DocumentNumber","DocumentNo","ReferenceNumber")),
                    "journal_id": _first(el, ("JournalID","Journal","JournalNo","JournalCode")),
                    "currency_code": _first(el, ("CurrencyCode","TransactionCurrency")),
                    "voucher_type": _first(el, ("VoucherType",)),
                    "voucher_desc": _first(el, ("VoucherDescription","Description")),
                    "mod_date": _first(el, ("ModificationDate",)),
                    "debit": Decimal("0"),
                    "credit": Decimal("0"),
                }

            if evt == "end" and tag in ("Line","TransactionLine","JournalLine"):
                if cur_voucher is None:
                    el.clear()
                    while el.getprevious() is not None:
                        del el.getparent()[0]
                    continue

                record_id = _first(el, ("RecordID","LineID"))
                system_id = _first(el, ("SystemID",))
                batch_id  = _first(el, ("BatchID",))
                doc_no    = _first(el, ("DocumentNumber","DocumentNo","ReferenceNumber"))
                line_src  = _first(el, ("SourceDocumentID","SourceID"))
                acc_id = _first(el, ("AccountID","GLAccountID"))
                cust_id = _first(el, ("CustomerID","Customer"))
                sup_id  = _first(el, ("SupplierID","Supplier","VendorID","Vendor"))
                desc    = _first(el, ("Description","Narrative","LineDescription"))
                debit  = _amount_of(el, "DebitAmount")  or Decimal("0")
                credit = _amount_of(el, "CreditAmount") or Decimal("0")
                amount = debit - credit
                cur_voucher["debit"]  += debit
                cur_voucher["credit"] += credit

                if not acc_id:
                    if cust_id and cust_id in customer_ctrl: acc_id = customer_ctrl[cust_id]
                    elif sup_id and sup_id in supplier_ctrl: acc_id = supplier_ctrl[sup_id]
                if not acc_id: acc_id = "UNDEFINED"
                seen_accounts_in_lines.add(acc_id)

                amt_cur = _first(el, ("AmountCurrency","ForeignAmount"))
                ex_rate = _first(el, ("ExchangeRate",))
                tax_type = _first(el, ("TaxType",))
                tax_country = _first(el, ("TaxCountryRegion","CountryRegion"))
                tax_code = _first(el, ("TaxCode",))
                tax_perc = _first(el, ("TaxPercentage","Rate"))
                d_tax = _amount_of(el, "DebitTaxAmount") or Decimal("0")
                c_tax = _amount_of(el, "CreditTaxAmount") or Decimal("0")
                tax_amt = _amount_of(el, "TaxAmount") or (d_tax - c_tax)

                acc_desc = accounts.get(acc_id, {}).get("AccountDescription","")
                cust_name = customers.get(cust_id, {}).get("Name","") if cust_id else ""
                cust_vat  = customers.get(cust_id, {}).get("VATNumber","") if cust_id else ""
                sup_name  = suppliers.get(sup_id, {}).get("Name","") if sup_id else ""
                sup_vat   = suppliers.get(sup_id, {}).get("VATNumber","") if sup_id else ""

                w_lines.writerow({
                    "RecordID": record_id or "", "VoucherID": cur_voucher["voucher_id"] or "", "VoucherNo": cur_voucher["voucher_no"] or "",
                    "JournalID": cur_voucher["journal_id"] or "", "TransactionDate": cur_voucher["transaction_date"] or "", "PostingDate": cur_voucher["posting_date"] or "",
                    "Period": cur_voucher["period"] or "", "Year": cur_voucher["year"] or "",
                    "SystemID": system_id or "", "BatchID": batch_id or "",
                    "DocumentNumber": doc_no or "", "SourceDocumentID": line_src or "",
                    "AccountID": acc_id or "", "AccountDescription": acc_desc,
                    "CustomerID": cust_id or "", "CustomerName": cust_name, "CustomerVATNumber": cust_vat,
                    "SupplierID": sup_id or "", "SupplierName": sup_name, "SupplierVATNumber": sup_vat,
                    "Description": desc or "", "Debit": f"{debit}", "Credit": f"{credit}", "Amount": f"{amount}",
                    "CurrencyCode": cur_voucher["currency_code"] or "", "AmountCurrency": amt_cur or "", "ExchangeRate": ex_rate or "",
                    "TaxType": tax_type or "", "TaxCountryRegion": tax_country or "", "TaxCode": tax_code or "",
                    "TaxPercentage": tax_perc or "",
                    "DebitTaxAmount": f"{d_tax}", "CreditTaxAmount": f"{c_tax}", "TaxAmount": f"{tax_amt}",
                    "IsGL": "True", "SourceType": "GL",
                })
                el.clear()
                while el.getprevious() is not None:
                    del el.getparent()[0]

            if evt == "end" and tag == "Analysis":
                parent = el.getparent()
                while parent is not None and _lname(parent) not in ("Line","TransactionLine","JournalLine"):
                    parent = parent.getparent()
                rec_id = _first(parent, ("RecordID","LineID")) if parent is not None else None
                a_deb = _amount_of(el, "DebitAnalysisAmount") or Decimal("0")
                a_cred= _amount_of(el, "CreditAnalysisAmount") or Decimal("0")
                a_amt = _amount_of(el, "AnalysisAmount")
                if a_amt is None:
                    a_amt = a_deb - a_cred
                w_anl.writerow({
                    "RecordID": rec_id or "", "Type": _first(el, ("AnalysisType",)) or "",
                    "ID": _first(el, ("AnalysisID",)) or "", "Amount": f"{a_amt or Decimal('0')}"
                })
                el.clear()

            if evt == "end" and tag == "Invoice":
                parent = el.getparent()
                pname = _lname(parent) if parent is not None else ""
                inv = {
                    "InvoiceNo": _first(el, ("InvoiceNo","InvoiceNumber")) or "",
                    "InvoiceDate": _first(el, ("InvoiceDate",)) or "",
                    "TaxPointDate": _first(el, ("TaxPointDate",)) or "",
                    "GLPostingDate": _first(el, ("GLPostingDate",)) or "",
                    "CurrencyCode": _first(el, ("CurrencyCode","TransactionCurrency")) or "",
                    "NetTotal": _first(el, ("NetTotal","DocumentNetTotal")) or "",
                    "TaxPayable": _first(el, ("TaxPayable","DocumentTaxPayable")) or "",
                    "GrossTotal": _first(el, ("GrossTotal","DocumentGrossTotal")) or "",
                    "SourceID": _first(el, ("SourceID",)) or "",
                    "DocumentNumber": _first(el, ("DocumentNumber","DocumentNo","ReferenceNumber")) or "",
                    "DueDate": _first(el, ("DueDate",)) or "",
                }
                if pname == "SalesInvoices":
                    cid = _first(el, ("CustomerID",))
                    cname = customers.get(cid,{}).get("Name","") if cid else _first(el,("CustomerName",))
                    inv.update({"CustomerID": cid or "", "CustomerName": cname or "", "CustomerVATNumber": customers.get(cid,{}).get("VATNumber","")})
                    w_sinv.writerow(inv)
                elif pname == "PurchaseInvoices":
                    sid = _first(el, ("SupplierID",))
                    sname = suppliers.get(sid,{}).get("Name","") if sid else _first(el,("SupplierName",))
                    inv.update({"SupplierID": sid or "", "SupplierName": sname or "", "SupplierVATNumber": suppliers.get(sid,{}).get("VATNumber","")})
                    w_pinv.writerow(inv)

            if evt == "end" and tag == "Transaction":
                if cur_voucher is not None:
                    balanced = (cur_voucher["debit"] - cur_voucher["credit"]).copy_abs() <= Decimal("0.005")
                    w_vouch.writerow({
                        "VoucherID": cur_voucher["voucher_id"] or "", "VoucherNo": cur_voucher["voucher_no"] or "",
                        "TransactionDate": cur_voucher["transaction_date"] or "", "PostingDate": cur_voucher["posting_date"] or "",
                        "Period": cur_voucher["period"] or "", "Year": cur_voucher["year"] or "",
                        "SourceDocumentID": cur_voucher["source_doc"] or "", "JournalID": cur_voucher["journal_id"] or "",
                        "CurrencyCode": cur_voucher["currency_code"] or "",
                        "VoucherType": cur_voucher["voucher_type"] or "", "VoucherDescription": cur_voucher["voucher_desc"] or "",
                        "ModificationDate": cur_voucher["mod_date"] or "",
                        "DebitTotal": f"{cur_voucher['debit']}", "CreditTotal": f"{cur_voucher['credit']}",
                        "Balanced": "Y" if balanced else "N"
                    })
                    cur_voucher = None
                el.clear()
                while el.getprevious() is not None:
                    del el.getparent()[0]

        # Etter parse
        missing = [acc for acc in sorted(seen_accounts_in_lines) if acc and acc not in accounts]
        if missing:
            w, f = _open_writer(od/"missing_accounts.csv", ("AccountID",))
            for a in missing: w.writerow({"AccountID": a})
            f.close()

        meta = {"parser":"saft_parser_pro_fallback","parser_version":__version__}
        (od/"parser_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    finally:
        try: closer()
        except Exception: pass
        for fh in (f_header,f_accounts,f_tax,f_cust,f_supp,f_arap,f_vouch,f_lines,f_anl,f_sinv,f_pinv,f_jtot if 'f_jtot' in locals() else None, f_raw):
            try:
                if fh: fh.flush(); fh.close()
            except Exception: pass