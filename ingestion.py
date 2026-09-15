"""
CKMS Ingestion — Registry-backed replacement for the 3-file-upload flow.
--------------------------------------------------------------------------
Filename convention (confirmed): <SUBSCRIBERCODE><MMYY>_<TYPE>[_<SEQ>]
    e.g. CPOINT0626_IND        -> CPOINT, June 2026, IND, sequence 1 (default)
         CPOINT0626_BUS_2      -> CPOINT, June 2026, BUS, sequence 2

Ingestion is INCREMENTAL per file (confirmed): insert this file's rows, then
run dedup/match against everything already in MasterRawRegistry for that
SubscriberID+ReportingPeriod+SubmissionType. We do not wait for every
sequence file in a month to arrive.

*** CONNECTION STRING PLACEHOLDER ***
Fill in DB_CONNECTION_STRING below for your local environment before running.
This targets the NEW registry database, kept separate from Cleanser's
database (confirmed — merge is a future build, not this one).
"""

import re
import os
import json
import uuid
import hashlib
import urllib.parse

import pandas as pd
import pyodbc  # noqa: F401 -- imported for ODBC driver registration, not used directly
from sqlalchemy import create_engine

import config
import pipeline

# ---------------------------------------------------------------------------
# Fill in / verify these for your local environment. Prefer overriding via
# the CKMS_REGISTRY_DB_CONN environment variable instead of editing this file
# directly, so nothing environment-specific gets committed to git.
#
# Confirmed target: SQL Server Express, Windows Authentication,
# instance AB3K33\AB3EXPRESS. Replace CKMS_REGISTRY_DB below with whatever
# you actually named the database when you ran schema.sql (that script only
# creates tables -- it assumes the database itself already exists).
#
# Before running: confirm the exact ODBC driver name installed on this
# machine -- it varies (17 vs 18, "SQL Server" vs "ODBC Driver 17 for SQL
# Server"). Check with:
#   python3 -c "import pyodbc; print(pyodbc.drivers())"
# and use whichever "...for SQL Server" entry appears in that list.
# ---------------------------------------------------------------------------
SQL_SERVER_INSTANCE = r"AB3K33\AB3EXPRESS"
SQL_DATABASE_NAME = "CKMS_Registry"          # <-- confirm/replace with your actual DB name
SQL_ODBC_DRIVER = "ODBC Driver 17 for SQL Server"

_odbc_connect_str = (
    f"DRIVER={{{SQL_ODBC_DRIVER}}};"
    f"SERVER={SQL_SERVER_INSTANCE};"
    f"DATABASE={SQL_DATABASE_NAME};"
    f"Trusted_Connection=yes;"
)
_default_connection_string = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(_odbc_connect_str)

# Environment variable wins if set, otherwise falls back to the built string
# above -- lets you override per-machine without touching this file.
DB_CONNECTION_STRING = os.environ.get("CKMS_REGISTRY_DB_CONN", _default_connection_string)

FILENAME_PATTERN = re.compile(
    r"^(?P<subscriber>[A-Za-z0-9]+?)(?P<mmyy>\d{4})_(?P<type>IND|BUS)(?:_(?P<seq>\d+))?",
    re.IGNORECASE,
)


def parse_filename(filename):
    """
    Parses '<CODE><MMYY>_<TYPE>[_<SEQ>]' -> dict with subscriber_code,
    reporting_period ('YYYY-MM'), submission_type, sequence_number.

    Raises ValueError with a clear message on anything that doesn't match --
    per the earlier decision to fail loudly on structural mismatches rather
    than guess. (Confirmed: naming convention is consistent, but this still
    shouldn't fail silently if an unexpected file shows up.)
    """
    stem = filename.rsplit(".", 1)[0]
    m = FILENAME_PATTERN.match(stem)
    if not m:
        raise ValueError(
            f"Filename '{filename}' doesn't match the expected "
            f"<SUBSCRIBERCODE><MMYY>_<IND|BUS>[_<SEQ>] pattern."
        )

    mm = int(m.group("mmyy")[:2])
    yy = int(m.group("mmyy")[2:])
    if not (1 <= mm <= 12):
        raise ValueError(f"Filename '{filename}': month portion '{mm:02d}' is not valid (MMYY expected).")
    year = 2000 + yy  # confirmed: MMYY, e.g. 0626 = June 2026

    return {
        "subscriber_code": m.group("subscriber").upper(),
        "reporting_period": f"{year:04d}-{mm:02d}",
        "submission_type": m.group("type").upper(),
        "sequence_number": int(m.group("seq")) if m.group("seq") else 1,
    }


def get_engine():
    return create_engine(DB_CONNECTION_STRING, fast_executemany=True)


# ---------------------------------------------------------------------------
# Normalization placeholder — port Cleanser's CompareNameTokens /
# NormalizeCustomerId logic here so both systems use identical rules. This
# MUST run before IdentityKey / ContentHash are computed, or formatting
# drift (leading zeros, casing, spacing) between months will look like new
# or changed records when nothing actually changed.
# ---------------------------------------------------------------------------

def normalize_dataframe(df, submission_type):
    """
    TODO: replace with a real port of Cleanser's normalization functions.
    Placeholder does the minimum safe thing: trims whitespace and upper-cases
    identity fields, which is NOT equivalent to Cleanser's fuzzy token
    matching but prevents the most obvious false mismatches until the real
    port is done.
    """
    df = df.copy()
    for f in config.ALL_HASH_FIELDS[submission_type]:
        if f in df.columns:
            df[f] = df[f].fillna("").astype(str).str.strip().str.upper()
    return df


def compute_content_hash(df, submission_type):
    hash_fields = config.ALL_HASH_FIELDS[submission_type]
    concat = pipeline._row_hash(df, hash_fields)
    return concat.apply(lambda s: hashlib.sha256(s.encode("utf-8")).hexdigest())


def build_identity_fields_json(df, submission_type):
    hash_fields = config.ALL_HASH_FIELDS[submission_type]
    present = [f for f in hash_fields if f in df.columns]
    return df[present].to_dict(orient="records")


# ---------------------------------------------------------------------------
# Registry reads
# ---------------------------------------------------------------------------

def get_or_create_subscriber(conn, subscriber_code):
    row = conn.exec_driver_sql(
        "SELECT SubscriberID FROM Subscribers WHERE SubscriberCode = ?", (subscriber_code,)
    ).fetchone()
    if row:
        return row[0]
    result = conn.exec_driver_sql(
        "INSERT INTO Subscribers (SubscriberCode) OUTPUT INSERTED.SubscriberID VALUES (?)",
        (subscriber_code,),
    )
    return result.fetchone()[0]


def create_submission(conn, subscriber_id, meta, row_count):
    result = conn.exec_driver_sql(
        """INSERT INTO Submissions
           (SubscriberID, ReportingPeriod, SubmissionType, SequenceNumber, FileName, RecordCount)
           OUTPUT INSERTED.SubmissionID
           VALUES (?, ?, ?, ?, ?, ?)""",
        (subscriber_id, meta["reporting_period"], meta["submission_type"],
         meta["sequence_number"], meta.get("filename", ""), row_count),
    )
    return result.fetchone()[0]


def previous_period(reporting_period):
    y, m = map(int, reporting_period.split("-"))
    if m == 1:
        return f"{y - 1:04d}-12"
    return f"{y:04d}-{m - 1:02d}"


def load_previous_registry_data(conn, subscriber_id, submission_type, reporting_period):
    """
    Pulls the PRIOR period's raw/clean/unl rows for this subscriber+type,
    reconstituting the flat columns match_and_hash()/track_date_changes()
    need from IdentityFieldsJSON. This is the direct replacement for
    uploading 'previous month raw file' / 'previous month cleaned file'.
    Reads through the SAME connection/transaction as the rest of
    process_file() -- prior periods are already committed from earlier
    runs, so this doesn't depend on that, but keeping one connection for
    the whole call avoids opening multiple pool connections per file.
    """
    prev_period = previous_period(reporting_period)

    prev_raw = pd.read_sql(
        """SELECT IdentityKey, FacilityAccNum, CustomerID, DisbursementDate,
                  CreditFacilityType, IdentityFieldsJSON
           FROM MasterRawRegistry
           WHERE SubscriberID = ? AND SubmissionType = ? AND ReportingPeriod = ?
             AND IsDuplicate = 0""",
        conn, params=(subscriber_id, submission_type, prev_period),
    )
    if not prev_raw.empty:
        expanded = pd.json_normalize(prev_raw["IdentityFieldsJSON"].apply(json.loads))
        prev_raw = pd.concat([prev_raw.drop(columns=["IdentityFieldsJSON"]), expanded], axis=1)

    prev_clean = pd.read_sql(
        """SELECT IdentityKey, IdentityFieldsJSON
           FROM MasterCleanRegistry
           WHERE ReportingPeriod = ? AND IdentityKey LIKE ?""",
        conn, params=(prev_period, f"{subscriber_id}|{submission_type}|%"),
    )
    if not prev_clean.empty:
        expanded = pd.json_normalize(prev_clean["IdentityFieldsJSON"].apply(json.loads))
        prev_clean = pd.concat([prev_clean.drop(columns=["IdentityFieldsJSON"]), expanded], axis=1)

    # "Previously routed to UNL, never resolved" means ANY point in history
    # for this subscriber+type, not just last period -- a record UNL'd 6
    # months ago and still unresolved is exactly the carryover case we want
    # to catch. Scoped via the IdentityKey prefix since MasterUNLRegistry
    # doesn't carry SubscriberID/SubmissionType columns directly.
    prev_unl = pd.read_sql(
        """SELECT IdentityKey, Resolved FROM MasterUNLRegistry
           WHERE IdentityKey LIKE ? AND Resolved = 0""",
        conn, params=(f"{subscriber_id}|{submission_type}|%",),
    )

    return prev_raw, prev_clean, prev_unl


# ---------------------------------------------------------------------------
# Registry writes
# ---------------------------------------------------------------------------

def bulk_insert_raw(conn, df, submission_id, subscriber_id, submission_type, reporting_period):
    """
    Bulk insert into MasterRawRegistry using fast_executemany. At 70-150k
    rows/file (confirmed volume), row-by-row executemany would be far too
    slow -- fast_executemany batches the ODBC calls.

    Returns df with a new 'RawRecordID' column merged on -- NOT the raw_ids
    lookup table separately. Downstream (write_unl, upsert_clean_registry)
    reads RawRecordID directly off each row as it flows through
    pipeline.run_pipeline(), so this merge has to happen correctly before
    that call, not after.

    The merge uses a per-row GUID generated in Python BEFORE insert, not
    IdentityKey. IdentityKey alone is NOT reliably unique within one file --
    an overdraft facility can legitimately report several rows with the
    same FacilityAccNum+CustomerID but different dates in one submission.
    A GUID generated per physical row and round-tripped through the insert
    sidesteps that ambiguity entirely, at the cost of one extra SELECT.
    """
    df = df.copy()
    df["_IngestRowGUID"] = [str(uuid.uuid4()) for _ in range(len(df))]

    identity_json = build_identity_fields_json(df, submission_type)
    records = []
    for i, row in df.iterrows():
        records.append({
            "SubmissionID": submission_id,
            "SubscriberID": subscriber_id,
            "SubmissionType": submission_type,
            "ReportingPeriod": reporting_period,
            "FacilityAccNum": row.get(config.KEY_FACILITY, ""),
            "CustomerID": row.get(config.KEY_CUSTOMER, ""),
            "BranchCode": row.get(config.BRANCH_COL, None),
            "CreditFacilityType": row.get(config.FACILITY_TYPE_COL, None),
            "DisbursementDate": pipeline.parse_date_value(row.get(config.KEY_DATE)),
            "CurBal": pipeline.parse_decimal_value(row.get(config.BALANCE_COL)),
            "ContentHash": row["_ContentHash"],
            "IdentityFieldsJSON": json.dumps(identity_json[i] if i < len(identity_json) else {}),
            "IngestRowGUID": row["_IngestRowGUID"],
        })

    out_df = pd.DataFrame(records)
    # chunksize=100 (not 1000): with 12 columns/row, 1000 would bind 12,000
    # parameters in one statement -- SQL Server's hard limit is 2100 per
    # statement. 100 keeps this at 1,200, safely under.
    out_df.to_sql("MasterRawRegistry", conn, if_exists="append", index=False, method="multi", chunksize=100)

    raw_ids = pd.read_sql(
        "SELECT RawRecordID, IngestRowGUID FROM MasterRawRegistry WHERE SubmissionID = ?",
        conn, params=(submission_id,),
    )
    df = df.merge(raw_ids, left_on="_IngestRowGUID", right_on="IngestRowGUID", how="left")
    df = df.drop(columns=["_IngestRowGUID", "IngestRowGUID"])
    return df


def mark_duplicates(conn, dedup_log_df):
    """
    dedup_log_df carries RawRecordID directly now (it rides along from
    bulk_insert_raw's merge, through pipeline.deduplicate() unchanged) --
    target it directly rather than re-matching on IdentityKey+ContentHash,
    which could ambiguously hit more than one row if two genuinely
    different rows happened to share both values.
    """
    if dedup_log_df.empty:
        return
    for _, r in dedup_log_df.iterrows():
        conn.exec_driver_sql(
            "UPDATE MasterRawRegistry SET IsDuplicate = 1, DedupReason = ? WHERE RawRecordID = ?",
            (r["Dedup Reason"], r["RawRecordID"]),
        )


def write_date_change_log(conn, date_change_log_df):
    if date_change_log_df.empty:
        return
    for _, r in date_change_log_df.iterrows():
        conn.exec_driver_sql(
            """INSERT INTO DateChangeLog
               (IdentityKey, ReportingPeriod, PreviousDate, CurrentDate, CreditFacilityType)
               VALUES (?, ?, ?, ?, ?)""",
            (r["IdentityKey"], r.get("ReportingPeriod", ""), pipeline.parse_date_value(r.get("PreviousDate")),
             pipeline.parse_date_value(r.get("CurrentDate")), r.get("CreditFacilityType", None)),
        )


def upsert_clean_registry(conn, df, submission_type, reporting_period, match_category):
    if df.empty:
        return
    hash_fields = config.ALL_HASH_FIELDS[submission_type]
    for _, r in df.iterrows():
        payload = json.dumps({f: r.get(f, "") for f in hash_fields})
        conn.exec_driver_sql(
            """MERGE MasterCleanRegistry AS tgt
               USING (SELECT ? AS IdentityKey, ? AS ReportingPeriod) AS src
               ON tgt.IdentityKey = src.IdentityKey AND tgt.ReportingPeriod = src.ReportingPeriod
               WHEN MATCHED THEN UPDATE SET IdentityFieldsJSON = ?, MatchCategory = ?
               WHEN NOT MATCHED THEN INSERT (IdentityKey, SubmissionType, ReportingPeriod,
                    SourceRawRecordID, IdentityFieldsJSON, MatchCategory)
                    VALUES (?, ?, ?, ?, ?, ?);""",
            (r["IdentityKey"], reporting_period, payload, match_category,
             r["IdentityKey"], submission_type, reporting_period,
             r.get("RawRecordID"), payload, match_category),
        )


def write_unl(conn, unl_df, reporting_period):
    """
    reporting_period is passed explicitly -- unl_df never actually carried
    a 'ReportingPeriod' column (nothing in pipeline.py's classification
    functions sets one), so r.get("ReportingPeriod", "") was silently
    writing blank periods into every UNL record. Matches the pattern
    upsert_clean_registry already used correctly.
    """
    if unl_df.empty:
        return
    for _, r in unl_df.iterrows():
        conn.exec_driver_sql(
            """INSERT INTO MasterUNLRegistry
               (RawRecordID, IdentityKey, ReportingPeriod, ExceptionCategory, Details)
               VALUES (?, ?, ?, ?, ?)""",
            (r.get("RawRecordID"), r["IdentityKey"], reporting_period,
             r["Exception Category"], r.get("Field Differences", None)),
        )


def get_submission_history(engine, subscriber_code, submission_type=None, limit=24):
    """
    Lists past Submissions for a subscriber, most recent first. New
    capability enabled by the registry -- there was no equivalent under the
    old file-diffing model, since nothing about past runs was persisted.
    """
    query = """
        SELECT s.ReportingPeriod, s.SubmissionType, s.SequenceNumber,
               s.FileName, s.RecordCount, s.UploadedAt
        FROM Submissions s
        JOIN Subscribers sub ON sub.SubscriberID = s.SubscriberID
        WHERE sub.SubscriberCode = ?
    """
    params = [subscriber_code]
    if submission_type:
        query += " AND s.SubmissionType = ?"
        params.append(submission_type)
    query += " ORDER BY s.ReportingPeriod DESC, s.SequenceNumber DESC"

    df = pd.read_sql(query, engine, params=tuple(params))
    return df.head(limit)


# ---------------------------------------------------------------------------
# End-to-end: process one uploaded file
# ---------------------------------------------------------------------------

def process_file(engine, file_path_or_buffer, filename):
    """
    engine is passed in rather than created here -- a caller running many
    uploads in one session (e.g. the Streamlit app) should create ONE engine
    (cached) and reuse it, rather than opening a new connection pool per file.

    Everything from the subscriber lookup through the final UNL write runs
    inside ONE transaction (single `with engine.begin()` block below). This
    matters for the backfill/resume checkpoint (already_processed(), which
    checks for a Submissions row): if a failure partway through used to
    leave an already-committed Submissions row with no actual data behind
    it, the checkpoint would wrongly treat that file as done on retry. With
    one transaction, either the ENTIRE file's processing commits together --
    Submissions row, raw rows, dedup marks, clean/unl writes, all of it --
    or a failure anywhere rolls back everything, leaving nothing behind to
    falsely check as "already loaded."
    """
    meta = parse_filename(filename)
    meta["filename"] = filename
    submission_type = meta["submission_type"]

    # Loading, validating, normalizing, and hashing the file is pure
    # Python/pandas work with no DB dependency -- fine to do before opening
    # the transaction.
    df = pipeline.load_file(file_path_or_buffer, submission_type)
    required = list(dict.fromkeys(
        config.THREE_KEY + config.ALL_HASH_FIELDS[submission_type] + [config.BALANCE_COL]
    ))
    pipeline.validate_columns(df, required, filename)

    df = normalize_dataframe(df, submission_type)
    df["IdentityKey"] = pipeline.build_identity_key(df, meta["subscriber_code"], submission_type)
    df["_ContentHash"] = compute_content_hash(df, submission_type)

    with engine.begin() as conn:
        subscriber_id = get_or_create_subscriber(conn, meta["subscriber_code"])
        submission_id = create_submission(conn, subscriber_id, meta, len(df))
        # df now carries a real RawRecordID per row (merged in via GUID) --
        # this is the df that flows into run_pipeline, not the pre-insert one.
        df = bulk_insert_raw(conn, df, submission_id, subscriber_id, submission_type, meta["reporting_period"])

        prev_raw, prev_clean, prev_unl = load_previous_registry_data(
            conn, subscriber_id, submission_type, meta["reporting_period"]
        )

        outputs, summary = pipeline.run_pipeline(df, prev_raw, prev_clean, submission_type, prev_unl)

        mark_duplicates(conn, outputs["dedup_log"])
        write_date_change_log(conn, outputs["date_change_log"])
        upsert_clean_registry(conn, outputs["cleaned_output"], submission_type, meta["reporting_period"], "Exact")
        upsert_clean_registry(conn, outputs["rearranged_output"], submission_type, meta["reporting_period"], "Rearranged")
        upsert_clean_registry(conn, outputs["enriched_output"], submission_type, meta["reporting_period"], "Enriched")
        write_unl(conn, outputs["unl_output"], meta["reporting_period"])
        # Reaching here without an exception means every step above
        # succeeded -- `with engine.begin()` commits on clean exit. Any
        # exception anywhere above rolls back the WHOLE block instead.

    return outputs, summary, meta
