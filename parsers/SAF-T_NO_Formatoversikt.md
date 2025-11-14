# SAF‑T Regnskap (NO) – Formatoversikt for utviklere

> **Kort om dette dokumentet**  
Denne filen gir en praktisk, teknisk oversikt over **SAF‑T Regnskap (Financial) for Norge** – struktur, kodenavn (XML‑elementer), krav vs. valgfritt, versjoner og vanlige fallgruver. Målet er at nye bidragsytere raskt forstår hva en SAF‑T‑fil **er**, **hvordan den er bygget opp**, og **hvilke felter som betyr hva** når vi parser og analyserer filene.

⚠️ **Status pr. v1.3 (2024/2025)**  
- **V1.3** kan brukes nå og **er eneste gyldige format fra 1. januar 2025**. **V1.2** kan brukes ut regnskapsår **2024**.  
- Norsk SAF‑T er **GL‑fokusert** (hovedbok + reskontro). **SourceDocuments** (detaljerte fakturaer) er **ikke i bruk i Norge**.
- **DueDate** (forfallsdato) finnes som **valgfritt felt på linjenivå** i **GeneralLedgerEntries/Journal/Transaction/Line**. Mange systemer lar den stå tom.
- **Utlevering** skjer på forespørsel (revisjon/kontroll), filens **periode er maks ett regnskapsår**, og filen pakkes ofte i **ZIP** ved innsending.

> Referanser: Se lenkene i kapittelet **Kilder** nederst.

---

## 1) Hva er SAF‑T Regnskap (Financial)?
**SAF‑T** (*Standard Audit File – Tax*) er en **XML**‑basert standard for utveksling av regnskapsdata. Den norske varianten bygger på OECD‑anbefaling og er forvaltet av Skatteetaten i samarbeid med bransjen. Hensikten er å forenkle utlevering og kontroll, samt flytting/analyse av data mellom systemer.

**Hvem må kunne levere?** Virksomheter med bokføringsplikt og elektronisk regnskapssystem må kunne produsere filen på forespørsel fra Skatteetaten. Det finnes enkelte terskler/unngåelser (små virksomheter mv.), se **Kilder**.

---

## 2) Versjoner og viktige endringer
- **v1.3 (1.30)** — innført 2024 og **obligatorisk fra 2025**. Viktige endringer:
  - `SelectionCriteria` i **Header** er **obligatorisk**.
  - `StandardTaxCode` i **TaxTable** er **obligatorisk**.
  - Mapping: **`GroupingCategory`** og **`GroupingCode`** i **GeneralLedgerAccounts** er **obligatorisk** (erstatter tidligere valg).  
  - Nye valgfrie elementer i **Transaction**: `VoucherType`, `VoucherDescription`, `ModificationDate`.
  - I **TaxInformationStructure** er `TaxAmount` erstattet av `DebitTaxAmount`/`CreditTaxAmount`.
  - I **AnalysisStructure** er `AnalysisAmount` erstattet av `DebitAnalysisAmount`/`CreditAnalysisAmount`.
  - **Customers**/**Suppliers** har fått **`BalanceAccountStructure`** (støtter én/flere reskontro‑kontoer).

- **v1.2** — gyldig til og med regnskapsår **2024**.

> **Navnerom (namespace)**: *urn:StandardAuditFile‑Taxation‑Financial:NO* (oppgis i XSD og brukes i valideringsfeil mv.).

---

## 3) Filnavn, pakking og omfang
- **Periode**: **maks ett regnskapsår** per fil (eller kortere).  
- **Navngivning (anbefalt)**:  
  `SAF-T Financial_<orgnr>_<YYYYMMDDhhmmss>_<filnr>.xml`  
  Eksempel: `SAF-T Financial_999999999_20160401235911_1_12.xml`  
- **Størrelsesgrenser (Altinn)**: inntil **2 GB per XML**; **ZIP** komprimering anbefales; inntil **10 vedlegg per innsending**.  
- **Kan splittes** på perioder/utvalg (Skatteetaten sammenslår ved behov).

---

## 4) Overordnet struktur (rot: `AuditFile`)
En norsk SAF‑T‑fil har alltid tre hovedseksjoner:

1. **`Header`** – metadata om fil, programvare og selskap (+ utvalgsperiode: `SelectionStartDate`/`SelectionEndDate` eller `PeriodStart`/`PeriodEnd`).  
2. **`MasterFiles`** – stamdata og kodeverk brukt i transaksjoner:  
   - `GeneralLedgerAccounts` (kontoplan med `GroupingCategory`/`GroupingCode`),  
   - `Customers`, `Suppliers` (inkl. `BalanceAccountStructure`),  
   - `TaxTable` (VAT‑koder, **`StandardTaxCode`**),  
   - `AnalysisTypeTable` (dimensjoner som avdeling/prosjekt osv.),  
   - flere tabeller er definert i OECD‑modellen, men flere er **“Not in use”** i Norge (f.eks. `Products`, `Assets`, `UOMTable`).  
3. **`GeneralLedgerEntries`** – selve **transaksjonsdataene** (journaler, bilag, linjer).

> **Merk:** **`SourceDocuments`** (fakturaer, leveranser osv.) er **ikke i bruk i Norge**. Norsk SAF‑T skal derfor ikke inneholde egne fakturamoduler, og felt som peker dit er eksplisitt merket med *“Not in use”*. I stedet brukes bl.a. **`ReferenceNumber`** på linjenivå for faktura/nota‑referanse.

---

## 5) Detaljert datastruktur (kodenavn per nivå)

### 5.1 `Header` (utdrag)
- `AuditFileVersion`, `AuditFileCountry`, `AuditFileDateCreated`
- Programvare: `SoftwareCompanyName`, `SoftwareID`, `SoftwareVersion`
- Selskap (`Company`): `RegistrationNumber` (orgnr – påkrevd hvis MVA‑registrert), `Name`, `Address`, `Contact`, `TaxRegistration`
- Valuta og utvalg: `DefaultCurrencyCode`, **`SelectionCriteria`** (`SelectionStartDate`/`SelectionEndDate` *eller* perioder `PeriodStart/PeriodEnd` + tilhørende år), `TaxAccountingBasis`
- Avsender (`AuditFileSender`) for tredjepartsutlevering

### 5.2 `MasterFiles` (utdrag)
- **`GeneralLedgerAccounts/Account`**: `AccountID`, `AccountDescription`, **`GroupingCategory`**, **`GroupingCode`**  
  (mapping mot næringsoppgave/resultat/evt. KOSTRA der relevant).
- **`TaxTable/TaxCodeDetails`**: `TaxType` (ofte `MVA`), `TaxCode`, `TaxPercentage`, **`StandardTaxCode`**, `BaseRate` (for delvis fradrag), `Compensation` (true/false).  
- **`Customers` / `Suppliers`**: `CustomerID`/`SupplierID`, `Name`, `Address`, `PartyInfo` (kan inneholde `PaymentTerms`), **`BalanceAccountStructure`** med `AccountID` og `Opening/Closing*Balance`.
- **`AnalysisTypeTable`**: definisjon av dimensjoner (f.eks. `A` = avdeling, `P` = prosjekt), med gyldige `AnalysisID`-verdier.

> **“Not in use” i Norge**: bl.a. `Products`, `PhysicalStock`, `Assets`, `UOMTable`, noen adressefelt (`Building`) m.fl.

### 5.3 `GeneralLedgerEntries`
**Hierarki:** `Journal` → `Transaction` → `Line`

**`Journal` (utdrag):** `JournalID`, `Description`  
**`Transaction` (utdrag):**  
- Datoer: `TransactionDate` (dokumentdato), `GLPostingDate` (bokføringsdato), `SystemEntryDate` (registrert i systemet), `ModificationDate` (v1.3, valgfri)  
- Bilagsinfo (v1.3): `VoucherType`, `VoucherDescription`, `VoucherNo`/`VoucherID` (navngivning varierer mellom systemer)  
- Eventuelt `SystemID`, `SourceID`  
- *Obs:* `CustomerID`/`SupplierID` **på transaksjonsnivå** er merket **Not in use** i spesifikasjonen

**`Line` (viktigste felter):**  
- Identifikatorer: `RecordID`, `AccountID`, `Analysis` (dimensjoner)  
- **Datoer**: `ValueDate` (valør), **`DueDate` (forfallsdato, valgfri)**  
- **Knytning til underliggende dokument**: `SourceDocumentID` (ikke i bruk i NO) → bruk **`ReferenceNumber`** (f.eks. fakturanr/kreditnota)  
- **Reskontro**: `CustomerID` *eller* `SupplierID` (kun på linjer som tilhører reskontro)  
- Tekst og beløp: `Description`, `DebitAmount`, `CreditAmount`  
- MVA: `TaxInformation` med `TaxType`/`TaxCode`/`TaxPercentage`/`TaxBase` og (v1.3) **`DebitTaxAmount`/`CreditTaxAmount`**  
- Andre: `CID` (customer identification number brukt av banker), `Quantity` (valgfri)

**Eksempel – én GL‑linje (utdrag, for illustrasjon):**
```xml
<Line>
  <RecordID>2025-0001-1</RecordID>
  <AccountID>1500</AccountID>
  <CustomerID>10023</CustomerID>
  <ReferenceNumber>SI-102345</ReferenceNumber>
  <Description>Salg av varer</Description>
  <DebitAmount>1250.00</DebitAmount>
  <TaxInformation>
    <TaxType>MVA</TaxType>
    <TaxCode>1</TaxCode>
    <TaxPercentage>25.00</TaxPercentage>
    <TaxBase>1000.00</TaxBase>
    <DebitTaxAmount>250.00</DebitTaxAmount>
  </TaxInformation>
  <TransactionDate>2025-01-05</TransactionDate>
  <GLPostingDate>2025-01-06</GLPostingDate>
  <DueDate>2025-02-04</DueDate> <!-- valgfritt -->
</Line>
```

---

## 6) Krav vs. valgfritt, datatyper og validering
- **Kravmerking** følger tabellene i spesifikasjonen: **M/O** (Mandatory/Optional) + repetisjon (**1..1**, **0..1**, **0..U** osv.).  
- **Datatyper** følger XML Schema (`xs:date`, `xs:dateTime`, `xs:decimal`, `xs:string`) og **norske “SAF*”‑typedefs** (`SAFcodeType`, `SAFmiddle1textType`, `SAFlongtextType` osv.).  
- **Validering**: Altinn validerer **mot XSD** ved innsending. **Key/keyref** (referanse‑integritet) sjekkes ikke automatisk i Altinn – men spesifikasjonen beskriver referansekrav (f.eks. at brukte `AnalysisID` skal finnes i `MasterFiles`).

---

## 7) Norske særtrekk som påvirker parser/rapporter
- **SourceDocuments** (fakturaer) er **ikke i bruk** → du finner ikke egen fakturatabell i norsk SAF‑T. Bruk `ReferenceNumber` på linjenivå for å koble mot fakturasystem ved behov.
- **DueDate** er **valgfri** på linjenivå. Noen ERP fyller ikke dette → en **aldersfordelt saldoliste bør bruke DueDate når den finnes,** men ha robust **fallback** (bokføringsdato/dokumentdato) for poster uten forfallsdato.
- **v1.3** krever at `GroupingCategory`/`GroupingCode` er satt for konti og at `StandardTaxCode` er satt for MVA‑koder.  
- **Customers/Suppliers** bruker `BalanceAccountStructure` (åpner for flere reskontro‑kontoer per part).

---

## 8) Minimumssjekkliste for en “god” norsk SAF‑T‑fil
- [ ] **Header** har `SelectionCriteria` (v1.3) og korrekt **periode ≤ 1 år**.  
- [ ] **Company.RegistrationNumber** er satt (for MVA‑subjekter).  
- [ ] **MasterFiles** inneholder **alle konti** i bruk (inkl. historiske) og korrekt mapping (`GroupingCategory`/`GroupingCode`).  
- [ ] **TaxTable** har **StandardTaxCode** pr. mva‑kode og eventuelle `BaseRate`‑varianter.  
- [ ] **Customers/Suppliers** har navn/adresse (tillat med “NA”/“NotAvailable” hvis ikke registrert i systemet), og **BalanceAccountStructure**.  
- [ ] **Transactions** har **TransactionDate**, **GLPostingDate** (og helst **ReferenceNumber** på relevante linjer).  
- [ ] **Lines** har korrekte **Debit/Credit** og **TaxInformation** kun på grunnlagslinjer (ikke både grunnlag og mva‑linje).  
- [ ] **DueDate** settes når systemet kjenner forfall (ellers tomt).

---

## 9) Vanlige fallgruver
- **Forfallsdato mangler** → aldersanalyser blir “dager siden bokføring”. Løs med join mot fakturakilde (via `ReferenceNumber`) eller beregn v/ betalingsbetingelser (med tydelig merking som estimat).  
- **Feil mapping** (`GroupingCategory`/`GroupingCode`) → avvik mot krav i v1.3.  
- **MVA‑detaljer**: bruk `DebitTaxAmount`/`CreditTaxAmount` i v1.3; ikke bland grunnlag og mva i samme linje.  
- **Key‑referanser** (Analysis, kunder/leverandører) finnes ikke i MasterFiles → filen kan validere i Altinn, men er dårlig datakvalitet for analyser.

---

## 10) Kilder (offisielle og primære)
- **Skatteetaten – Dokumentasjon (v1.3 og v1.2), inkl. skjematiske oversikter og XSD/codelists**  
  – *“Version 1.3 … will be the only valid format from 1 January 2025 … There are three files: Header, Masterfiles, GeneralLedgerEntries”*  
- **Skatteetaten – Technical description** (v1.5, 01.03.2024)  
  – tabeller for **Header**, **MasterFiles**, **GeneralLedgerEntries**, merking *“Not in use”*, typetabeller, m.m.  
  – på **Line**: `ReferenceNumber` og **`DueDate` (O 0..1)**.  
  – eksplisitt merknad: **`SourceDocuments` er ikke i bruk** i NO – bruk `ReferenceNumber`.  
- **Skatteetaten – General documentation** (v1.6, 01.03.2024)  
  – endringer i **v1.3** (obligatoriske felt), **filnavn**, **periode ≤ 1 år**, Altinn‑validering og **størrelsesgrenser**.  
- **Skatteetatens GitHub – SAF‑T**  
  – XSD‑skjema, eksempelfiler og kode‑lister (StandardTaxCode, GroupingCategory m.m.).  
- **Namespace‑bruk i praksis**  
  – valideringsfeil fra SAP‑løsning som refererer navnerommet *urn:StandardAuditFile‑Taxation‑Financial:NO*.

---

## 11) Ord- og feltnøkkel (hurtigreferanse)
- **AuditFile** – rot‑element i XML.  
- **Header** – metadata, selskap, utvalg (`SelectionCriteria`).  
- **MasterFiles** – stamdata: `GeneralLedgerAccounts`, `Customers`, `Suppliers`, `TaxTable`, `AnalysisTypeTable` m.fl.  
- **GeneralLedgerEntries** – `Journal` → `Transaction` → `Line`.  
- **ReferenceNumber** – dokumentreferanse (faktura/kreditnota o.l.).  
- **DueDate** – forfallsdato (valgfri på linjenivå).  
- **ValueDate** – valørdato (kan avvike fra dokumentdato).  
- **GroupingCategory/GroupingCode** – obligatorisk mapping av konti (v1.3).  
- **StandardTaxCode** – obligatorisk standard MVA‑kode pr. mva‑kode (v1.3).  
- **Analysis** – dimensjoner (avdeling, prosjekt osv.).  
- **BalanceAccountStructure** – reskontro‑konto(er) og åpnings-/sluttbalanser per kunde/leverandør.

---

## 12) Små eksempler

**Header/SelectionCriteria (datoer):**
```xml
<Header>
  <AuditFileVersion>1.30</AuditFileVersion>
  <AuditFileCountry>NO</AuditFileCountry>
  <DefaultCurrencyCode>NOK</DefaultCurrencyCode>
  <SelectionCriteria>
    <SelectionStartDate>2025-01-01</SelectionStartDate>
    <SelectionEndDate>2025-12-31</SelectionEndDate>
  </SelectionCriteria>
</Header>
```

**MasterFiles/TaxTable (v1.3):**
```xml
<TaxTableEntry>
  <TaxType>MVA</TaxType>
  <TaxCodeDetails>
    <TaxCode>1</TaxCode>
    <TaxPercentage>25.00</TaxPercentage>
    <StandardTaxCode>121</StandardTaxCode>
    <BaseRate>100.00</BaseRate>
  </TaxCodeDetails>
</TaxTableEntry>
```

**GeneralLedgerEntries/Transaction (nye felter i v1.3):**
```xml
<Transaction>
  <TransactionDate>2025-02-15</TransactionDate>
  <GLPostingDate>2025-02-16</GLPostingDate>
  <VoucherType>AR</VoucherType>
  <VoucherDescription>Fakturajournal</VoucherDescription>
  <ModificationDate>2025-02-18</ModificationDate>
  <!-- Lines ... -->
</Transaction>
```

---

## 13) Bidragsnotat
Dersom du oppdager felter vi ikke parser (eller endringer i v1.3+) – opprett et issue/PR i repo. Nyttige ting å overvåke:
- Bruk av **`DueDate`** i GL‑linjer fra ulike ERP‑leverandører.
- Endringer i **codelists** (StandardTaxCode / GroupingCategory) på Skatteetatens GitHub.
- Evt. fremtidig norsk bruk av tidligere *“Not in use”*‑områder.

---

### Kilder (klikkbare):
- Skatteetaten – **Documentation** (v1.3/v1.2), oversikter & skjematiske view:  
  https://www.skatteetaten.no/en/business-and-organisation/start-and-run/best-practices-accounting-and-cash-register-systems/saf-t-financial/documentation/
- Skatteetaten – **Technical description** (v1.5, 01.03.2024):  
  https://www.skatteetaten.no/globalassets/bedrift-og-organisasjon/starte-og-drive/rutiner-regnskap-og-kassasystem/saf-t-regnskap/oppdateringer-mars-2024/norwegian-saf-t-financial-data---technical-description.pdf
- Skatteetaten – **General documentation** (v1.6, 01.03.2024):  
  https://www.skatteetaten.no/globalassets/bedrift-og-organisasjon/starte-og-drive/rutiner-regnskap-og-kassasystem/saf-t-regnskap/oppdateringer-mars-2024/norwegian-saf-t-financial-data---documentation.pdf
- Skatteetaten – **GitHub (XSD, kodeverk, eksempler)**:  
  https://github.com/Skatteetaten/saf-t
- SAP KBA – navnerom i valideringsfeil (`urn:StandardAuditFile-Taxation-Financial:NO`):  
  https://userapps.support.sap.com/sap/support/knowledge/en/2924953

