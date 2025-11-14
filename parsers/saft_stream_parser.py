# -*- coding: utf-8 -*-
from __future__ import annotations
"""
SAF‑T (NO) Streaming Parser — v2025.10.22-stream-v8

Endringer i v8 (kompatibel med v7):
- Skriver *journals.csv* (JournalID, Type) for å muliggjøre profilering av journal-typer.
- Ellers identisk med v7: rådump default AV, progress/hotspots, unknown_summary, avbrudd.
"""
from pathlib import Path
from typing import Optional, Callable, Dict, Iterable, Set
from decimal import Decimal, InvalidOperation
from time import perf_counter
import csv, os, json, zipfile

try:
    from lxml import etree
except Exception as e:
    raise RuntimeError("saft_stream_parser krever lxml. Installer: pip install lxml") from e

__version__ = "2025.10.22-stream-v8"

ProgressCB = Optional[Callable[[str, object], Optional[bool]]]  # kan returnere False for å stoppe

def _lname(tag_or_el) -> str:
    t = tag_or_el.tag if hasattr(tag_or_el, "tag") else str(tag_or_el)
    return t.split('}', 1)[-1] if '}' in t else t

def _text(node) -> Optional[str]:
    if node is None or node.text is None:
        return None
    t = node.text.strip()
    return t if t else None

def _norm_amount_str(s: str) -> str:
    s = s.replace('\u00A0', '').strip()
    if ',' in s and '.' in s:
        s = s.replace(',', '')
    elif ',' in s and '.' not in s:
        s = s.replace('.', '').replace(',', '.')
    else:
        parts = s.split('.')
        if len(parts) > 2:
            s = s.replace('.', '')
    return s.replace(' ', '')

def _to_dec(txt: Optional[str]):
    if not txt: return None
    try: return Decimal(_norm_amount_str(txt))
    except (InvalidOperation, ValueError): return None

def _amount_of(node, key: str):
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

def _first(el, keys: Iterable[str]):
    if isinstance(keys, (str, bytes)): keys = [keys]
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
            zf.close(); raise RuntimeError("Ingen .xml i zip-arkivet")
        st = zf.open(names[0], "r")
        def closer():
            try: st.close()
            finally: zf.close()
        return st, closer
    fh = open(p, "rb")
    return fh, fh.close

def _open_writer(path: Path, headers):
    f = open(path, "w", newline="", encoding="utf-8", buffering=1024*1024)
    w = csv.DictWriter(f, fieldnames=list(headers), lineterminator="\n")
    w.writeheader()
    return w, f

class Stats:
    def __init__(self) -> None:
        self.started = perf_counter()
        self.events = 0
        self.counts: Dict[str,int] = {}
        self.times: Dict[str,float] = {}
        self.csv_rows: Dict[str,int] = {}

    def time_block(self, name: str):
        stats = self
        class Ctx:
            def __enter__(self_inner): self_inner.t = perf_counter()
            def __exit__(self_inner, a,b,c):
                dt = perf_counter() - self_inner.t
                stats.times[name] = stats.times.get(name, 0.0) + dt
                stats.counts[name] = stats.counts.get(name, 0) + 1
        return Ctx()
    def inc_csv(self, key: str, n: int=1): self.csv_rows[key]=self.csv_rows.get(key,0)+n
    def tick(self, n: int=1): self.events += n
    def snapshot(self)->Dict[str,object]:
        dur = perf_counter() - self.started
        rate = self.events/dur if dur>0 else 0.0
        top = sorted(self.times.items(), key=lambda kv: kv[1], reverse=True)[:6]
        return {"duration_sec": dur, "events": self.events, "rate_events_per_sec": rate,
                "counts": self.counts, "times_sec": self.times, "csv_rows": self.csv_rows, "top_times": top}

# kjente tagger for unknown_summary
KNOWN: Set[str] = set("""
AuditFile Header MasterFiles GeneralLedgerEntries SourceDocuments
GeneralLedgerAccounts TaxTable Customers Suppliers
Account GeneralLedgerAccount TaxTableEntry Customer Supplier
Journal Transaction Line TransactionLine JournalLine
Analysis AnalysisType AnalysisID AnalysisAmount DebitAnalysisAmount CreditAnalysisAmount
TransactionID TransactionDate PostingDate Period FiscalYear Year
DebitAmount CreditAmount TaxType TaxCountryRegion TaxCode TaxPercentage
DebitTaxAmount CreditTaxAmount TaxAmount
SalesInvoices PurchaseInvoices Invoice InvoiceNo InvoiceDate DueDate
FileCreationDateTime FileCreationDate AuditFileVersion SelectionStart SelectionEnd SelectionStartDate SelectionEndDate StartDate EndDate
CompanyName CompanyID FunctionalCurrency DefaultCurrencyCode ProductVersion SoftwareCertificateNumber
GroupingCategory GroupingCode ParentAccountID AccountType AccountDescription GLAccountID Type
""".split())
KNOWN_CONTAINERS: Set[str] = set("""
MasterFiles GeneralLedgerEntries SourceDocuments GeneralLedgerAccounts Customers Suppliers SalesInvoices PurchaseInvoices
""".split())

def parse_saft(input_path: Path, outdir: Path, *, on_progress: ProgressCB=None) -> None:
    cb = on_progress
    stats = Stats()

    od = Path(outdir); od.mkdir(parents=True, exist_ok=True)

    # Default OFF for rådump; respekter SAFT_WRITE_RAW hvis eksplisitt satt
    env_val = os.getenv("SAFT_WRITE_RAW")
    write_raw = False if env_val is None else (env_val.strip().lower() not in ("0","false","no"))
    progress_every = int(os.getenv("SAFT_PROGRESS_EVENTS","50000"))

    # writers
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
    w_raw=f_raw=None
    if write_raw:
        w_raw,f_raw = _open_writer(od/"raw_elements.csv", ("XPath","Tag","Text","Attributes"))
    w_jtot,f_jtot = _open_writer(od/"gl_totals.csv", ("JournalID","TotalDebit","TotalCredit"))
    w_jmeta,f_jmeta = _open_writer(od/"journals.csv", ("JournalID","Type"))  # NYTT

    accounts: Dict[str, Dict[str,str]] = {}
    customers: Dict[str, Dict[str,str]] = {}
    suppliers: Dict[str, Dict[str,str]] = {}
    customer_ctrl: Dict[str,str] = {}
    supplier_ctrl: Dict[str,str] = {}
    seen_accounts_in_lines = set()
    unknown_counts: Dict[str,int] = {}

    fh, closer = _open_input(Path(input_path))
    cancelled = False

    try:
        ctx = etree.iterparse(fh, events=("start","end"), huge_tree=True)
        root = None
        cur_voucher = None

        for evt, el in ctx:
            tag = _lname(el)

            if evt == "start" and root is None:
                root = el.getroottree().getroot()

            # unknown summary
            if evt == "end":
                if (tag not in KNOWN) and (tag not in KNOWN_CONTAINERS):
                    unknown_counts[tag] = unknown_counts.get(tag, 0) + 1

            # rådump
            if write_raw and evt == "end" and w_raw is not None:
                try:
                    xp = el.getroottree().getpath(el)
                except Exception:
                    xp = f"/{tag}"
                attrs = {k:v for k,v in (el.attrib or {}).items()}
                txt = (_text(el) or "")[:2000]
                w_raw.writerow({"XPath": xp, "Tag": tag, "Text": txt, "Attributes": json.dumps(attrs, ensure_ascii=False)})

            # Header
            if evt == "end" and tag == "Header":
                w_header.writerow({
                    "CompanyName": _first(el, ("CompanyName",)) or "",
                    "CompanyID": _first(el, ("CompanyID",)) or "",
                    "FunctionalCurrency": _first(el, ("FunctionalCurrency","DefaultCurrencyCode")) or "",
                    "DefaultCurrencyCode": _first(el, ("DefaultCurrencyCode","CurrencyCode")) or "",
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
                stats.inc_csv("header")
                el.clear()
                while el.getprevious() is not None:
                    del el.getparent()[0]

            # Account
            if evt == "end" and tag in ("Account","GeneralLedgerAccount"):
                acc_id  = _first(el, ("AccountID","GLAccountID"))
                if acc_id:
                    acc_desc= _first(el, ("AccountDescription","Description"))
                    acc_type= _first(el, ("AccountType",))
                    parent  = _first(el, ("ParentAccountID","ParentID"))
                    group_cat = _first(el, ("GroupingCategory",))
                    group_code= _first(el, ("GroupingCode","GroupingCategoryCode"))
                    op_dr   = _amount_of(el, "OpeningDebitBalance")  or Decimal("0")
                    op_cr   = _amount_of(el, "OpeningCreditBalance") or Decimal("0")
                    cl_dr   = _amount_of(el, "ClosingDebitBalance")  or Decimal("0")
                    cl_cr   = _amount_of(el, "ClosingCreditBalance") or Decimal("0")
                    taxc    = _first(el, ("TaxCode",))
                    taxt    = _first(el, ("TaxType",))
                    accounts[acc_id] = {"AccountDescription": acc_desc or "", "TaxCode": taxc or ""}
                    w_accounts.writerow({
                        "AccountID": acc_id, "AccountDescription": acc_desc or "", "AccountType": acc_type or "",
                        "ParentAccountID": parent or "",
                        "GroupingCategory": group_cat or "", "GroupingCode": group_code or "",
                        "OpeningDebit": f"{op_dr}", "OpeningCredit": f"{op_cr}",
                        "ClosingDebit": f"{cl_dr}", "ClosingCredit": f"{cl_cr}",
                        "TaxCode": taxc or "", "TaxType": taxt or "",
                    })
                    stats.inc_csv("accounts")
                el.clear()
                while el.getprevious() is not None:
                    del el.getparent()[0]

            # TaxTableEntry
            if evt == "end" and tag == "TaxTableEntry":
                w_tax.writerow({
                    "TaxCode": _first(el, ("TaxCode",)) or "",
                    "StandardTaxCode": _first(el, ("StandardTaxCode",)) or "",
                    "TaxType": _first(el, ("TaxType",)) or "",
                    "TaxPercentage": _first(el, ("TaxPercentage","Rate")) or "",
                    "TaxCountryRegion": _first(el, ("TaxCountryRegion","CountryRegion")) or "",
                    "Description": _first(el, ("Description",)) or "",
                })
                stats.inc_csv("tax_table")
                el.clear()
                while el.getprevious() is not None:
                    del el.getparent()[0]

            # Customer
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
                    stats.inc_csv("customers")
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
                            stats.inc_csv("arap_control_accounts")
                el.clear()
                while el.getprevious() is not None:
                    del el.getparent()[0]

            # Supplier
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
                    stats.inc_csv("suppliers")
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
                            stats.inc_csv("arap_control_accounts")
                el.clear()
                while el.getprevious() is not None:
                    del el.getparent()[0]

            # Journal (meta + totals)
            if evt == "end" and tag == "Journal":
                j_id = _first(el, ("JournalID","Journal"))
                j_type = _first(el, ("Type",)) or ""
                tdr  = _amount_of(el, "TotalDebit")  or Decimal("0")
                tcr  = _amount_of(el, "TotalCredit") or Decimal("0")
                if j_id:
                    w_jmeta.writerow({"JournalID": j_id or "", "Type": j_type})
                    stats.inc_csv("journals")
                if j_id or (tdr or tcr):
                    w_jtot.writerow({"JournalID": j_id or "", "TotalDebit": f"{tdr}", "TotalCredit": f"{tcr}"})
                    stats.inc_csv("gl_totals")
                el.clear()
                while el.getprevious() is not None:
                    del el.getparent()[0]

            # Transaction start
            if evt == "start" and tag == "Transaction":
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

            # Line
            if evt == "end" and tag in ("Line","TransactionLine","JournalLine"):
                if cur_voucher is not None:
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
                    cust_name = customers.get(cust_id,{}).get("Name","") if cust_id else ""
                    cust_vat  = customers.get(cust_id,{}).get("VATNumber","") if cust_id else ""
                    sup_name  = suppliers.get(sup_id,{}).get("Name","") if sup_id else ""
                    sup_vat   = suppliers.get(sup_id,{}).get("VATNumber","") if sup_id else ""
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
                    stats.inc_csv("transactions")
                el.clear()
                while el.getprevious() is not None:
                    del el.getparent()[0]

            # Analysis
            if evt == "end" and tag == "Analysis":
                parent = el.getparent()
                while parent is not None and _lname(parent) not in ("Line","TransactionLine","JournalLine"):
                    parent = parent.getparent()
                rec_id = _first(parent, ("RecordID","LineID")) if parent is not None else None
                a_deb = _amount_of(el, "DebitAnalysisAmount") or Decimal("0")
                a_cred= _amount_of(el, "CreditAnalysisAmount") or Decimal("0")
                a_amt = _amount_of(el, "AnalysisAmount")
                if a_amt is None: a_amt = a_deb - a_cred
                w_anl.writerow({
                    "RecordID": rec_id or "",
                    "Type": _first(el, ("AnalysisType",)) or "",
                    "ID": _first(el, ("AnalysisID",)) or "",
                    "Amount": f"{a_amt or Decimal('0')}"
                })
                stats.inc_csv("analysis_lines")
                el.clear()

            # Invoice
            if evt == "end" and tag == "Invoice":
                parent = el.getparent()
                parent_name = _lname(parent) if parent is not None else ""
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
                if parent_name == "SalesInvoices":
                    cid = _first(el, ("CustomerID",))
                    cname = customers.get(cid,{}).get("Name","") if cid else _first(el,("CustomerName",))
                    inv.update({"CustomerID": cid or "", "CustomerName": cname or "", "CustomerVATNumber": customers.get(cid,{}).get("VATNumber","")})
                    w_sinv.writerow(inv); stats.inc_csv("sales_invoices")
                elif parent_name == "PurchaseInvoices":
                    sid = _first(el, ("SupplierID",))
                    sname = suppliers.get(sid,{}).get("Name","") if sid else _first(el,("SupplierName",))
                    inv.update({"SupplierID": sid or "", "SupplierName": sname or "", "SupplierVATNumber": suppliers.get(sid,{}).get("VATNumber","")})
                    w_pinv.writerow(inv); stats.inc_csv("purchase_invoices")

            # Transaction end
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
                    stats.inc_csv("vouchers")
                    cur_voucher = None
                el.clear()
                while el.getprevious() is not None:
                    del el.getparent()[0]

            # progress & avbrudd
            if evt == "end":
                stats.tick(1)
                if cb and (stats.events % progress_every == 0):
                    res = cb("tick", stats.snapshot())
                    if res is False:
                        cancelled = True
                        break

        # Etter parse
        missing = [acc for acc in sorted(seen_accounts_in_lines) if acc and acc not in accounts]
        if missing:
            w, f = _open_writer(od/"missing_accounts.csv", ("AccountID",))
            for a in missing: w.writerow({"AccountID": a})
            f.close()

        if unknown_counts:
            w, f = _open_writer(od/"unknown_summary.csv", ("Tag","Count"))
            for k,v in sorted(unknown_counts.items(), key=lambda kv: kv[1], reverse=True):
                w.writerow({"Tag": k, "Count": v})
            f.close()

        (od/"parser_meta.json").write_text(json.dumps({
            "parser": "saft_stream_parser",
            "parser_version": __version__,
            "wrote_raw": bool(write_raw),
            "cancelled": bool(cancelled),
            "files": [p.name for p in od.glob("*.csv")],
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        (od/"parse_stats.json").write_text(json.dumps(stats.snapshot(), ensure_ascii=False, indent=2), encoding="utf-8")

    finally:
        try: closer()
        except Exception: pass
        for fh in (f_header,f_accounts,f_tax,f_cust,f_supp,f_arap,f_vouch,f_lines,f_anl,f_sinv,f_pinv,f_jtot if 'f_jtot' in locals() else None, f_jmeta if 'f_jmeta' in locals() else None, f_raw):
            try:
                if fh: fh.flush(); fh.close()
            except Exception: pass