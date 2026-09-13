# CKMS — Customer Key Management System

Automated monthly data-cleaning pipeline for credit bureau subscriber
submissions. Carries forward previously-cleaned identity/contact data for
records proven unchanged from last month, so subscribers don't get
re-flagged for manual cleaning every cycle.

## Setup

```bash
pip install -r requirements.txt
```

## Run

```bash
streamlit run app.py
```

This opens a local web page (default: http://localhost:8501). Upload the
three input files, click **Run Pipeline**, then download the four output
files.

## Input Files

1. **Current Month Raw File** — this month's subscriber submission
2. **Previous Month Raw File** — last month's submission, used for hash comparison
3. **Previous Month Cleaned File** — last cycle's cleaned identity/contact data

CSV or XLSX accepted for all three.

## Pipeline Steps

1. **Deduplicate** — on Facility Account Number + Customer ID + Disbursement
   Date, keeping the row with the lowest current balance. Ties are broken by
   first-occurrence in file order. Dropped rows are logged, not discarded.
2. **Date-Mismatch Check** — same Facility + Customer ID as last month, but a
   different disbursement date. Pulled into its own file for institution
   review (disbursement dates aren't expected to change).
3. **Composite Key Match** — matches remaining records to last month's raw
   file on the full 3-key.
4. **Hash Comparison** — hashes the identity/contact fields (national ID,
   voter ID, driver's licence, passport, SSN, Ezwich, other ID type/number,
   TIN, home/mobile1/mobile2/work phone) and classifies each match as
   `Exact match`, `Different`, or `No previous-month match`.
   - Within `Different`, records where the SAME set of ID values is present
     across the identity fields — just recorded under different field
     labels (e.g. an ID under Passport this month that was under Driver's
     Licence last month) — are detected as a **rearrangement**, not a real
     change. This check spans ALL identity ID fields, not just one subgroup.
5. **Clean-File Cross-Check** — run separately for exact matches AND for
   rearrangements: confirms the record also exists in last cycle's cleaned
   file.
6. **Cleaned + Rearranged Outputs** — exact matches and provable
   rearrangements are BOTH auto-processed and carried forward from the
   clean file, but kept in two separate files for audit transparency:
   - **Cleaned Output** — genuine exact matches
   - **Rearranged Output** — same values, different field order, auto-resolved
   Nothing else on the row is touched, and the raw file is never overwritten.
7. **Exception Output** — everything that still needs a human look, labeled
   by category:
   - `Different hash` (a real change, not a rearrangement)
   - `No previous-month match`
   - `Same hash, not found in cleaned file`
   - `Rearranged, not found in cleaned file`
8. **Date-Mismatch Output** — standalone file from step 2.

## Output Files

Five CSVs: Dedup Log, Date Mismatch, Cleaned Output, Rearranged Output,
Exception File. An optional **combined workbook** (one .xlsx with each of
these as a labeled sheet, plus a Summary sheet) can be generated on demand
from the app — kept separate from the default CSV downloads since building
the workbook is meaningfully slower at large row counts (~14s at 140K rows
vs under a second for a CSV of the same size).

## Column Mapping

Default column names match the standard subscriber file layout. If a
specific subscriber's file uses different headers, adjust them in the
sidebar before running — nothing is hardcoded in `pipeline.py` itself.

## Performance

Tested at 150,000 records: full pipeline runs in ~3 seconds. All matching,
hashing, and dedup logic is vectorized with pandas rather than row-by-row
loops, so it scales comfortably to the larger institution file sizes.

## Files

- `app.py` — Streamlit interface
- `pipeline.py` — core processing logic (framework-independent, testable on its own)
- `config.py` — default column names and field groupings
- `test_pipeline.py` — sanity test suite covering every classification branch
- `requirements.txt` — dependencies