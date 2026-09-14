"""
CKMS Pipeline — Core Processing Logic (v2)
--------------------------------------------
Changes from v1, per design discussion:

  1. Matching/lookup now uses a 4-part IdentityKey (SubscriberCode +
     SubmissionType + FacilityAccNum + CustomerID) instead of a 3-key that
     included DisbursementDate. Dedup is UNCHANGED (still 3-key,
     keep-lowest-balance) -- that's a same-month duplicate-row problem, not
     a cross-month identity problem, and including date there is correct.

  2. DisbursementDate is no longer part of the content hash. A date change
     alone can no longer produce a "Different" classification. Date changes
     are logged to their own informational report (track_date_changes) for
     every facility type -- never blocks a match, never routes to UNL.

  3. New 'Enriched' classification: a record whose only field differences
     are ADDED values (blank last month, populated now) is auto-processed
     like Exact/Rearranged, but keeps its OWN current-month values rather
     than being overwritten by the carry-forward substitution -- otherwise
     the new information would be silently discarded. Any record with even
     one Changed or Removed field is NOT Enriched; it stays in 'Different'
     and goes to manual review.

  4. IND and BUS have different identity/contact field sets (see config.py),
     so every function that touches hash fields takes submission_type and
     looks up the right field list.

All functions remain pure (DataFrames in, DataFrames out).
"""

import io
import datetime as _dt
import pandas as pd
import numpy as np
import config


def _safe_stringify(value):
    """
    Converts an openpyxl-parsed cell value to text WITHOUT going through
    pandas' default float->str cast, which produces scientific notation for
    large numbers (e.g. a phone number stored as an Excel "Number" cell
    becomes "2.33554e+11" instead of the original digits). Whole-number
    floats are rendered as plain integer digits instead.

    This does NOT recover a leading zero that was already lost because the
    source cell was genuinely stored as a number in Excel (that information
    is gone before openpyxl ever sees it) -- it only stops pandas from
    further mangling the value on top of that.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if isinstance(value, float):
        if value == int(value):
            return str(int(value))
        return repr(value)  # avoids scientific notation for non-integer floats
    return str(value).strip()


def parse_date_value(value):
    """
    Parses a DisbursementDate-style value into a python date object (or
    None). Source files use compact YYYYMMDD with no separators (e.g.
    '20260115') -- passing that raw string straight to a SQL DATE column
    fails ('Invalid character value for cast specification'), since ODBC
    parameter binding expects either a real date object or 'YYYY-MM-DD',
    not the compact form. Never raises -- one unparseable date returns None
    rather than crashing an entire batch insert.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", "nat"):
        return None
    try:
        return _dt.datetime.strptime(s, "%Y%m%d").date()
    except ValueError:
        pass
    try:
        parsed = pd.to_datetime(s, errors="coerce")
        return parsed.date() if pd.notna(parsed) else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def load_file(path_or_buffer, submission_type):
    """
    Load a CSV or Excel file. Forces identity/contact/key columns to string
    dtype so leading zeros and long numeric IDs are never mangled.
    submission_type selects which field list ('IND' or 'BUS') to force.
    """
    force_string_cols = config.FORCE_STRING_COLS[submission_type]

    name = getattr(path_or_buffer, "name", str(path_or_buffer))
    if str(name).lower().endswith(".csv"):
        # CSV has no per-cell numeric typing ambiguity -- dtype=str is
        # reliable here, values are text on disk already.
        df = pd.read_csv(path_or_buffer, dtype=str, keep_default_na=False)
        for col in force_string_cols:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str).str.strip()
    else:
        # dtype=str on read_excel does NOT stop openpyxl from parsing a
        # numeric-formatted cell as float FIRST -- by the time pandas casts
        # to str, large numbers (phone numbers, IDs stored as Excel
        # "Number" cells) are already corrupted into scientific notation.
        # Reading as dtype=object preserves openpyxl's native per-cell type
        # so we can stringify large numbers safely ourselves.
        df = pd.read_excel(path_or_buffer, dtype=object)
        for col in df.columns:
            if col in force_string_cols:
                df[col] = df[col].apply(_safe_stringify)
            else:
                df[col] = df[col].apply(lambda v: "" if (v is None or (isinstance(v, float) and pd.isna(v))) else v)

    df.columns = df.columns.astype(str).str.strip()

    return df


def validate_columns(df, required_cols, df_label):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        available = ", ".join(sorted(df.columns.astype(str)))
        raise ValueError(
            f"{df_label} is missing required column(s): {missing}. "
            f"Columns found in this file: [{available}]. "
            f"Check the sidebar column settings match this file's actual headers "
            f"(names are case-sensitive and whitespace-sensitive)."
        )


def _concat_cols(df, cols, fields_source=None):
    source = fields_source if fields_source is not None else df
    parts = []
    for c in cols:
        if c in source.columns:
            parts.append(source[c].astype(str))
        else:
            parts.append(pd.Series([""] * len(df), index=df.index))
    result = parts[0]
    for p in parts[1:]:
        result = result.str.cat(p, sep=config.HASH_DELIMITER)
    return result


def _make_key(df, cols):
    return _concat_cols(df, cols)


def build_identity_key(df, subscriber_code, submission_type,
                        facility_col=None, customer_col=None):
    """
    4-part IdentityKey: SubscriberCode|SubmissionType|FacilityAccNum|CustomerID.
    subscriber_code/submission_type are constants for the whole file (one
    subscriber, one type per submission), so broadcast rather than looked up
    per-row.
    """
    facility_col = facility_col or config.KEY_FACILITY
    customer_col = customer_col or config.KEY_CUSTOMER
    fac = df[facility_col].astype(str) if facility_col in df.columns else pd.Series([""] * len(df), index=df.index)
    cust = df[customer_col].astype(str) if customer_col in df.columns else pd.Series([""] * len(df), index=df.index)
    prefix = f"{subscriber_code}{config.IDENTITY_KEY_DELIMITER}{submission_type}{config.IDENTITY_KEY_DELIMITER}"
    return prefix + fac.str.cat(cust, sep=config.IDENTITY_KEY_DELIMITER)


# ---------------------------------------------------------------------------
# Step 1: Deduplicate current-month raw file — UNCHANGED (3-key, incl. date)
# ---------------------------------------------------------------------------

def deduplicate(df, key_cols=None, balance_col=None):
    """
    Same-month duplicate-row detection. Deliberately still 3-key (incl.
    DisbursementDate): within a single submission, two rows for the same
    facility+customer with genuinely different dates (e.g. overdraft
    drawdowns) are real, distinct records and must NOT be collapsed.
    """
    key_cols = key_cols or config.THREE_KEY
    balance_col = balance_col or config.BALANCE_COL

    df = df.copy()
    df["_CKMS_KEY"] = _make_key(df, key_cols)
    df["_CKMS_BAL"] = pd.to_numeric(df[balance_col], errors="coerce")
    df["_CKMS_ORDER"] = range(len(df))

    df_sorted = df.sort_values(
        by=["_CKMS_KEY", "_CKMS_BAL", "_CKMS_ORDER"],
        ascending=[True, True, True],
        na_position="last",
    )

    is_first = ~df_sorted["_CKMS_KEY"].duplicated(keep="first")
    kept = df_sorted[is_first].copy()
    dropped = df_sorted[~is_first].copy()

    if not dropped.empty:
        dropped["Dedup Reason"] = "Duplicate key — higher or tied balance, not kept"
        dropped["Kept Balance"] = kept.set_index("_CKMS_KEY")["_CKMS_BAL"].reindex(
            dropped["_CKMS_KEY"]
        ).values
        dropped = dropped.rename(columns={"_CKMS_BAL": "This Row Balance"})

    kept = kept.drop(columns=["_CKMS_ORDER", "_CKMS_BAL"], errors="ignore")
    kept = kept.rename(columns={"_CKMS_KEY": "Composite Key (3-key)"})

    if not dropped.empty:
        dropped = dropped.drop(columns=["_CKMS_ORDER"], errors="ignore")
        dropped = dropped.rename(columns={"_CKMS_KEY": "Composite Key (3-key)"})

    return kept.reset_index(drop=True), dropped.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Date tracking — informational only. Never removes rows from the working
# set, never affects classification. Runs for every facility type.
# ---------------------------------------------------------------------------

def track_date_changes(current_df, previous_raw_df, identity_key_col="IdentityKey",
                        date_col=None, facility_type_col=None):
    """
    Logs every case where DisbursementDate differs from last month's raw
    record with the same IdentityKey. Returns a log DataFrame only --
    current_df is NOT filtered or modified. This is DateChangeLog's source.
    """
    date_col = date_col or config.KEY_DATE
    facility_type_col = facility_type_col or config.FACILITY_TYPE_COL

    cur = current_df.copy()
    prev = previous_raw_df.copy()

    prev_dates = (
        prev.drop_duplicates(subset=identity_key_col, keep="first")
        .set_index(identity_key_col)[date_col]
    )
    cur["_PREV_DATE"] = cur[identity_key_col].map(prev_dates)

    has_prev = cur["_PREV_DATE"].notna()
    date_differs = has_prev & (cur["_PREV_DATE"] != cur[date_col])

    log = cur[date_differs].copy()
    if not log.empty:
        cols = [identity_key_col, "_PREV_DATE", date_col]
        if facility_type_col in log.columns:
            cols.append(facility_type_col)
        log = log[cols].rename(columns={
            "_PREV_DATE": "PreviousDate",
            date_col: "CurrentDate",
            facility_type_col: "CreditFacilityType",
        })

    return log.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Field-level diff classification: Added / Changed / Removed
# ---------------------------------------------------------------------------

def _classify_field_diffs(cur_vals, prev_vals, fields):
    """
    For one record, compare current vs previous values field-by-field.
    Returns (added, changed, removed) — each a list of field names.
      Added:   prev blank, current has a value
      Changed: prev had a value, current has a DIFFERENT value
      Removed: prev had a value, current is blank
    """
    added, changed, removed = [], [], []
    for f in fields:
        pv = (prev_vals.get(f) or "").strip()
        cv = (cur_vals.get(f) or "").strip()
        if pv == cv:
            continue
        if pv == "" and cv != "":
            added.append(f)
        elif pv != "" and cv == "":
            removed.append(f)
        else:
            changed.append(f)
    return added, changed, removed


# ---------------------------------------------------------------------------
# Steps 3+4: Identity match + content hash + classification
# ---------------------------------------------------------------------------

def _row_hash(df, fields):
    return _concat_cols(df, fields)


def _rearranged_but_same(current_vals, previous_vals):
    if current_vals == previous_vals:
        return False
    cur_set = sorted([v for v in current_vals if v != ""])
    prev_set = sorted([v for v in previous_vals if v != ""])
    return cur_set == prev_set and cur_set != []


def match_and_hash(working_df, previous_raw_df, submission_type,
                    identity_key_col="IdentityKey"):
    """
    Match on IdentityKey (4-part, date-independent). Classify each record:

        Exact match             — content hash identical to last month
        Rearranged              — same ID values, moved between fields
        Enriched                — differences exist, but ALL are Added-type
                                   (no Changed/Removed) — auto-processed,
                                   keeps CURRENT values (see build note below)
        Different                — at least one Changed or Removed field —
                                   needs manual review
        No previous-month match — IdentityKey not seen last month
    """
    hash_fields = config.ALL_HASH_FIELDS[submission_type]
    rearrangement_group = config.REARRANGEMENT_GROUP[submission_type]

    cur = working_df.copy()
    prev = previous_raw_df.copy()

    cur["_CUR_HASH"] = _row_hash(cur, hash_fields)
    prev["_PREV_HASH"] = _row_hash(prev, hash_fields)

    prev_lookup = prev.drop_duplicates(subset=identity_key_col, keep="first").set_index(identity_key_col)

    cur["_PREV_HASH_LOOKUP"] = cur[identity_key_col].map(prev_lookup["_PREV_HASH"])

    has_prev = cur["_PREV_HASH_LOOKUP"].notna()
    is_same = has_prev & (cur["_CUR_HASH"] == cur["_PREV_HASH_LOOKUP"])
    cur["Match Flag"] = np.select(
        [~has_prev, is_same],
        ["No previous-month match", "Exact match"],
        default="Different",
    )

    cur["Rearranged"] = False
    cur["Field Differences"] = ""
    cur["Added Fields"] = ""
    cur["Changed Fields"] = ""
    cur["Removed Fields"] = ""

    diff_mask = cur["Match Flag"] == "Different"
    n_diff = int(diff_mask.sum())

    if n_diff > 0:
        diff_subset = cur.loc[diff_mask, [identity_key_col] + hash_fields].copy()

        prev_field_vals = {}
        for f in hash_fields:
            if f in prev_lookup.columns:
                prev_field_vals[f] = diff_subset[identity_key_col].map(prev_lookup[f]).fillna("")
            else:
                prev_field_vals[f] = pd.Series([""] * len(diff_subset), index=diff_subset.index)

        field_diff_bool = pd.DataFrame(
            {f: diff_subset[f].astype(str) != prev_field_vals[f].astype(str) for f in hash_fields},
            index=diff_subset.index,
        )

        diff_strings, rearranged_vals = [], []
        added_strs, changed_strs, removed_strs = [], [], []

        for idx in diff_subset.index:
            diffing_fields = [f for f in hash_fields if field_diff_bool.at[idx, f]]
            diffs = [
                f"{f}: '{prev_field_vals[f].loc[idx]}' -> '{diff_subset.at[idx, f]}'"
                for f in diffing_fields
            ]
            diff_strings.append("; ".join(diffs))

            cur_vals = {f: diff_subset.at[idx, f] for f in hash_fields}
            prev_vals = {f: prev_field_vals[f].loc[idx] for f in hash_fields}
            added, changed, removed = _classify_field_diffs(cur_vals, prev_vals, diffing_fields)
            added_strs.append(", ".join(added))
            changed_strs.append(", ".join(changed))
            removed_strs.append(", ".join(removed))

            all_diffs_in_group = len(diffing_fields) > 0 and all(f in rearrangement_group for f in diffing_fields)
            if all_diffs_in_group:
                cur_group = [diff_subset.at[idx, f] for f in rearrangement_group]
                prev_group = [prev_field_vals[f].loc[idx] for f in rearrangement_group]
                rearranged_vals.append(_rearranged_but_same(cur_group, prev_group))
            else:
                rearranged_vals.append(False)

        cur.loc[diff_mask, "Field Differences"] = diff_strings
        cur.loc[diff_mask, "Rearranged"] = rearranged_vals
        cur.loc[diff_mask, "Added Fields"] = added_strs
        cur.loc[diff_mask, "Changed Fields"] = changed_strs
        cur.loc[diff_mask, "Removed Fields"] = removed_strs

    # Enriched: classified as "Different" but has Added fields only, no
    # Changed/Removed, and is not already claimed by Rearranged.
    is_enriched = (
        (cur["Match Flag"] == "Different")
        & (~cur["Rearranged"])
        & (cur["Added Fields"] != "")
        & (cur["Changed Fields"] == "")
        & (cur["Removed Fields"] == "")
    )
    cur.loc[is_enriched, "Match Flag"] = "Enriched"

    cur = cur.drop(columns=["_CUR_HASH", "_PREV_HASH_LOOKUP"], errors="ignore")
    return cur.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step 5: Clean-file cross-check (keyed on IdentityKey now, not 3-key)
# ---------------------------------------------------------------------------

def clean_file_cross_check(subset_df, previous_clean_df, identity_key_col="IdentityKey",
                            not_found_label="Not found in cleaned file"):
    clean_keys = set(previous_clean_df[identity_key_col])
    subset_df = subset_df.copy()
    found_mask = subset_df[identity_key_col].isin(clean_keys)
    eligible = subset_df[found_mask].copy()
    not_found = subset_df[~found_mask].copy()
    if not not_found.empty:
        not_found["Exception Category"] = not_found_label
    return eligible.reset_index(drop=True), not_found.reset_index(drop=True)


def check_previously_unl(not_found_df, previous_unl_df, identity_key_col="IdentityKey"):
    """
    Sharpens the vague 'not found in cleaned file' case: if the IdentityKey
    also appears in a PRIOR unresolved UNL record, this isn't a new problem
    -- it's a known carry-over that was never resolved. Split accordingly so
    triage can tell the two apart immediately.
    """
    if not_found_df.empty or previous_unl_df.empty:
        return not_found_df.copy(), pd.DataFrame()

    prior_unresolved_keys = set(
        previous_unl_df.loc[previous_unl_df.get("Resolved", 0) == 0, identity_key_col]
    )
    is_carryover = not_found_df[identity_key_col].isin(prior_unresolved_keys)

    carryover = not_found_df[is_carryover].copy()
    genuinely_new = not_found_df[~is_carryover].copy()

    if not carryover.empty:
        carryover["Exception Category"] = (
            "Matched in Raw History - Previously Routed to UNL, Never Resolved in Clean"
        )

    return genuinely_new.reset_index(drop=True), carryover.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step 6: Build outputs
# ---------------------------------------------------------------------------

def build_carry_forward_output(eligible_df, previous_clean_df, submission_type,
                                identity_key_col="IdentityKey"):
    """
    For Exact/Rearranged records: overwrite hash fields with the PREVIOUS
    clean values (this is the whole point -- carry forward what was already
    verified clean). Does NOT touch DisbursementDate or any other raw field.
    """
    hash_fields = config.ALL_HASH_FIELDS[submission_type]
    clean_lookup = previous_clean_df.drop_duplicates(subset=identity_key_col, keep="first").set_index(identity_key_col)

    out = eligible_df.copy()
    for f in hash_fields:
        if f in clean_lookup.columns:
            out[f] = out[identity_key_col].map(clean_lookup[f]).fillna(out.get(f, ""))

    out["Source"] = "Carried forward from previous cleaned file"
    return out.reset_index(drop=True)


def build_enriched_output(enriched_df):
    """
    For Enriched records: do NOT overwrite with previous clean values --
    that would discard the newly-added information. Current-month values
    are kept exactly as submitted; only the Source label marks how this
    record was auto-resolved.
    """
    out = enriched_df.copy()
    out["Source"] = "Auto-processed: new identity data added, no conflicts"
    return out.reset_index(drop=True)


def build_unl_output(true_different_df, no_match_df, not_found_exact_df,
                      not_found_rearranged_df, previously_unl_carryover_df=None):
    frames = []

    if true_different_df is not None and not true_different_df.empty:
        d = true_different_df.copy()
        d["Exception Category"] = "Different hash"
        frames.append(d)

    if no_match_df is not None and not no_match_df.empty:
        d = no_match_df.copy()
        d["Exception Category"] = "No previous-month match"
        frames.append(d)

    if not_found_exact_df is not None and not not_found_exact_df.empty:
        frames.append(not_found_exact_df)

    if not_found_rearranged_df is not None and not not_found_rearranged_df.empty:
        frames.append(not_found_rearranged_df)

    if previously_unl_carryover_df is not None and not previously_unl_carryover_df.empty:
        frames.append(previously_unl_carryover_df)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True, sort=False)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def build_workbook(outputs, summary=None):
    """
    Bundle all output DataFrames into a single .xlsx workbook, one sheet per
    category, with a Summary sheet first. Uses xlsxwriter so it stays
    fast/lightweight even at 100-150K+ rows per sheet.

    Returns raw bytes, ready for a Streamlit download_button.
    """
    sheet_names = {
        "dedup_log": "Dedup Log",
        "date_change_log": "Date Changes (Info)",
        "cleaned_output": "Cleaned Output",
        "rearranged_output": "Rearranged (Auto)",
        "enriched_output": "Enriched (Auto)",
        "unl_output": "UNL",
    }

    buffer = io.BytesIO()
    with pd.ExcelWriter(
        buffer,
        engine="xlsxwriter",
        engine_kwargs={"options": {"strings_to_numbers": False}},
    ) as writer:
        if summary:
            summary_df = pd.DataFrame(list(summary.items()), columns=["Metric", "Count"])
            summary_df.to_excel(writer, sheet_name="Summary", index=False)

        for key, sheet_name in sheet_names.items():
            df = outputs.get(key)
            (df if df is not None else pd.DataFrame()).to_excel(
                writer, sheet_name=sheet_name, index=False
            )

    buffer.seek(0)
    return buffer.getvalue()


def run_pipeline(current_raw_df, previous_raw_df, previous_clean_df, submission_type,
                  previous_unl_df=None):
    """
    Runs one submission through the full pipeline against registry-sourced
    previous-period data (previous_raw_df/previous_clean_df/previous_unl_df
    are expected to already be filtered to the prior ReportingPeriod +
    SubmissionType by the caller/ingestion layer, and all three -- plus
    current_raw_df -- must already have an 'IdentityKey' column).
    """
    summary = {}
    previous_unl_df = previous_unl_df if previous_unl_df is not None else pd.DataFrame()

    # Step 1 — dedup (unchanged, 3-key)
    deduped_df, dedup_log_df = deduplicate(current_raw_df)
    summary["Input records"] = len(current_raw_df)
    summary["Duplicates removed"] = len(dedup_log_df)
    summary["After dedup"] = len(deduped_df)

    # Date tracking — informational only, does not filter working set
    date_change_log_df = track_date_changes(deduped_df, previous_raw_df)
    summary["Date changes logged (informational)"] = len(date_change_log_df)

    working_df = deduped_df  # nothing removed for date reasons

    # Steps 3+4 — match + classify
    matched_df = match_and_hash(working_df, previous_raw_df, submission_type)

    exact_df = matched_df[matched_df["Match Flag"] == "Exact match"].copy()
    rearranged_df = matched_df[matched_df["Match Flag"] == "Rearranged"].copy()
    enriched_df = matched_df[matched_df["Match Flag"] == "Enriched"].copy()
    true_different_df = matched_df[matched_df["Match Flag"] == "Different"].copy()
    no_match_df = matched_df[matched_df["Match Flag"] == "No previous-month match"].copy()

    summary["Exact match"] = len(exact_df)
    summary["Rearranged (auto-processed)"] = len(rearranged_df)
    summary["Enriched (auto-processed)"] = len(enriched_df)
    summary["Different (needs review)"] = len(true_different_df)
    summary["No previous-month match"] = len(no_match_df)

    # Step 5 — clean-file cross-check for Exact + Rearranged
    # (Enriched skips this: it never had prior clean data to reconcile
    # against in the way Exact/Rearranged do; it is auto-published as-is.)
    eligible_exact_df, not_found_exact_df = clean_file_cross_check(
        exact_df, previous_clean_df, not_found_label="Same hash, not found in cleaned file"
    )
    eligible_rearranged_df, not_found_rearranged_df = clean_file_cross_check(
        rearranged_df, previous_clean_df, not_found_label="Rearranged, not found in cleaned file"
    )

    # Sharpen "not found" into genuinely-new vs known-carryover-from-UNL
    not_found_exact_df, carryover_exact_df = check_previously_unl(not_found_exact_df, previous_unl_df)
    not_found_rearranged_df, carryover_rearranged_df = check_previously_unl(not_found_rearranged_df, previous_unl_df)
    carryover_df = pd.concat([carryover_exact_df, carryover_rearranged_df], ignore_index=True, sort=False) \
        if not (carryover_exact_df.empty and carryover_rearranged_df.empty) else pd.DataFrame()

    summary["Eligible for carry-forward (exact)"] = len(eligible_exact_df)
    summary["Eligible for carry-forward (rearranged)"] = len(eligible_rearranged_df)
    summary["Same hash, not found in cleaned file"] = len(not_found_exact_df)
    summary["Rearranged, not found in cleaned file"] = len(not_found_rearranged_df)
    summary["Previously UNL'd, still unresolved (carryover)"] = len(carryover_df)

    # Step 6 — build outputs
    cleaned_output_df = build_carry_forward_output(eligible_exact_df, previous_clean_df, submission_type)
    rearranged_output_df = build_carry_forward_output(eligible_rearranged_df, previous_clean_df, submission_type)
    if not rearranged_output_df.empty:
        rearranged_output_df["Source"] = "Auto-processed: same ID values, different field order"
    enriched_output_df = build_enriched_output(enriched_df)

    # Step 7 — UNL output
    unl_output_df = build_unl_output(
        true_different_df, no_match_df, not_found_exact_df, not_found_rearranged_df, carryover_df
    )

    outputs = {
        "dedup_log": dedup_log_df,
        "date_change_log": date_change_log_df,          # informational only
        "cleaned_output": cleaned_output_df,
        "rearranged_output": rearranged_output_df,
        "enriched_output": enriched_output_df,           # new
        "unl_output": unl_output_df,
    }

    return outputs, summary
