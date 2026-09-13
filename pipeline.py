"""
CKMS Pipeline — Core Processing Logic
--------------------------------------
Implements the 8-step monthly data cleaning process:

1. Deduplicate current-month raw file (3-key, keep lowest balance)
2. Date-mismatch check (2-key vs previous-month raw)
3. Composite key match (3-key, working file vs previous-month raw)
4. Hash comparison on identity/contact fields
5. Clean-file cross-check (exact matches only, 3-key vs previous-month cleaned)
6. Build Cleaned Output File
7. Build Exception Output File
8. Date-Mismatch Output File (produced in step 2, listed last for output naming)

All functions are pure (take DataFrames, return DataFrames) so they can be
tested independently of the Streamlit UI and swapped/extended later.
"""

import io
import pandas as pd
import numpy as np
import config


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def load_file(path_or_buffer, force_string_cols=None):
    """
    Load a CSV or Excel file. Forces identity/contact/key columns to string
    dtype so leading zeros and long numeric IDs are never mangled.

    IMPORTANT: this does NOT silently create missing columns. A configured
    field that isn't actually present in the file is a real problem (usually
    a column-name mismatch between config.py's defaults and this subscriber's
    actual headers) and must surface as a clear error via validate_columns(),
    not get quietly papered over with a blank column — that previously
    caused real identity data to be silently dropped/misrouted without any
    error at all.
    """
    force_string_cols = force_string_cols or config.FORCE_STRING_COLS

    name = getattr(path_or_buffer, "name", str(path_or_buffer))
    if str(name).lower().endswith(".csv"):
        df = pd.read_csv(path_or_buffer, dtype=str, keep_default_na=False)
    else:
        df = pd.read_excel(path_or_buffer, dtype=str)

    # Trim whitespace from headers — a common source of "column not found"
    # errors when a subscriber's file has trailing/leading spaces in headers.
    df.columns = df.columns.astype(str).str.strip()

    # Force known columns to clean string values, but ONLY if they actually
    # exist. Missing columns are left for validate_columns() to catch loudly.
    for col in force_string_cols:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    return df


def _concat_cols(df, cols, fields_source=None):
    """
    Vectorized pipe-delimited concatenation of columns. Much faster than
    df.agg(join, axis=1) at scale (avoids a Python-level call per row).
    """
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


def validate_columns(df, required_cols, df_label):
    """
    Raises a clear, actionable error if any required column is missing from
    df, instead of letting a raw KeyError surface deep inside a pipeline step.
    """
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        available = ", ".join(sorted(df.columns.astype(str)))
        raise ValueError(
            f"{df_label} is missing required column(s): {missing}. "
            f"Columns found in this file: [{available}]. "
            f"Check the sidebar column settings match this file's actual headers "
            f"(names are case-sensitive and whitespace-sensitive)."
        )


def _make_key(df, cols):
    """Build a pipe-delimited composite key column from the given columns."""
    return _concat_cols(df, cols)


# ---------------------------------------------------------------------------
# Step 1: Deduplicate current-month raw file
# ---------------------------------------------------------------------------

def deduplicate(df, key_cols=None, balance_col=None):
    """
    Deduplicate on the composite key, keeping the row with the LOWEST balance.
    Ties are broken by first-occurrence in file order (deterministic).

    Returns:
        deduped_df   - one row per unique key
        dedup_log_df - every dropped row, with a reason and the key it was
                       deduplicated on, for audit purposes
    """
    key_cols = key_cols or config.THREE_KEY
    balance_col = balance_col or config.BALANCE_COL

    df = df.copy()
    df["_CKMS_KEY"] = _make_key(df, key_cols)
    df["_CKMS_BAL"] = pd.to_numeric(df[balance_col], errors="coerce")
    df["_CKMS_ORDER"] = range(len(df))

    # Sort so the row we want to KEEP is first within each key group:
    # lowest balance first, then first-occurrence as tiebreaker.
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

    drop_cols = ["_CKMS_ORDER"]
    kept = kept.drop(columns=drop_cols + ["_CKMS_BAL"], errors="ignore")
    kept = kept.rename(columns={"_CKMS_KEY": "Composite Key (3-key)"})

    if not dropped.empty:
        dropped = dropped.drop(columns=drop_cols, errors="ignore")
        dropped = dropped.rename(columns={"_CKMS_KEY": "Composite Key (3-key)"})

    return kept.reset_index(drop=True), dropped.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step 2: Date-mismatch check (2-key vs previous-month raw)
# ---------------------------------------------------------------------------

def date_mismatch_check(current_df, previous_raw_df, two_key_cols=None, date_col=None):
    """
    Compare 2-key (facility+customer) records against previous-month raw.
    Where the 2-key matches but the disbursement date differs, pull that
    record out into its own file.

    Returns:
        remaining_df       - current_df minus the flagged date-mismatch rows
        date_mismatch_df   - flagged rows, with old/new date shown side by side
    """
    two_key_cols = two_key_cols or config.TWO_KEY
    date_col = date_col or config.KEY_DATE

    cur = current_df.copy()
    prev = previous_raw_df.copy()

    cur["_CKMS_2KEY"] = _make_key(cur, two_key_cols)
    prev["_CKMS_2KEY"] = _make_key(prev, two_key_cols)

    # One previous date per 2-key (first occurrence if somehow duplicated)
    prev_dates = (
        prev.drop_duplicates(subset="_CKMS_2KEY", keep="first")
        .set_index("_CKMS_2KEY")[date_col]
    )

    cur["_CKMS_PREV_DATE"] = cur["_CKMS_2KEY"].map(prev_dates)

    has_prev = cur["_CKMS_PREV_DATE"].notna()
    date_differs = has_prev & (cur["_CKMS_PREV_DATE"] != cur[date_col])

    mismatch = cur[date_differs].copy()
    remaining = cur[~date_differs].copy()

    if not mismatch.empty:
        mismatch = mismatch.rename(
            columns={
                date_col: "Current Month Disbursement Date",
                "_CKMS_PREV_DATE": "Previous Month Disbursement Date",
            }
        )
        mismatch["Flag"] = "2-key match, disbursement date differs"

    remaining = remaining.drop(columns=["_CKMS_PREV_DATE"], errors="ignore")

    for d in (mismatch, remaining):
        if "_CKMS_2KEY" in d.columns:
            d.rename(columns={"_CKMS_2KEY": "Composite Key (2-key)"}, inplace=True)

    return remaining.reset_index(drop=True), mismatch.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step 3 + 4: Composite key match + hash comparison
# ---------------------------------------------------------------------------

def _row_hash(df, fields):
    """Vectorized pipe-delimited concatenation of the given fields per row."""
    return _concat_cols(df, fields)


def _rearranged_but_same(current_vals, previous_vals):
    """
    True only if the group's values were actually reordered (not identical
    position-for-position) AND the same set of values is present in both.
    Being unchanged does NOT count as "rearranged".
    """
    if current_vals == previous_vals:
        return False  # unchanged, not a rearrangement
    cur_set = sorted([v for v in current_vals if v != ""])
    prev_set = sorted([v for v in previous_vals if v != ""])
    return cur_set == prev_set and cur_set != []


def match_and_hash(working_df, previous_raw_df, three_key_cols=None,
                    hash_fields=None, rearrangement_group=None):
    """
    Match working_df to previous_raw_df on the 3-key, hash the identity/
    contact fields, and classify each record.

    Adds columns:
        Composite Key (3-key)
        Match Flag: 'Exact match' | 'Different' | 'No previous-month match'
        Rearranged: True/False (only meaningful when Match Flag == 'Different')
        Field Differences: human-readable string of which fields differ
    """
    three_key_cols = three_key_cols or config.THREE_KEY
    hash_fields = hash_fields or config.ALL_HASH_FIELDS
    rearrangement_group = rearrangement_group or config.REARRANGEMENT_GROUP

    cur = working_df.copy()
    prev = previous_raw_df.copy()

    cur["Composite Key (3-key)"] = _make_key(cur, three_key_cols)
    prev["Composite Key (3-key)"] = _make_key(prev, three_key_cols)

    cur["_CUR_HASH"] = _row_hash(cur, hash_fields)
    prev["_PREV_HASH"] = _row_hash(prev, hash_fields)

    prev_lookup = prev.drop_duplicates(subset="Composite Key (3-key)", keep="first").set_index(
        "Composite Key (3-key)"
    )

    cur["_PREV_HASH_LOOKUP"] = cur["Composite Key (3-key)"].map(prev_lookup["_PREV_HASH"])

    # Vectorized classification (no row-wise apply over the full frame)
    has_prev = cur["_PREV_HASH_LOOKUP"].notna()
    is_same = has_prev & (cur["_CUR_HASH"] == cur["_PREV_HASH_LOOKUP"])
    cur["Match Flag"] = np.select(
        [~has_prev, is_same],
        ["No previous-month match", "Exact match"],
        default="Different",
    )

    # Default columns for every row; only "Different" rows get real values,
    # and that subset is normally a small fraction of the file.
    cur["Rearranged"] = False
    cur["Field Differences"] = ""

    diff_mask = cur["Match Flag"] == "Different"
    n_diff = int(diff_mask.sum())

    if n_diff > 0:
        diff_subset = cur.loc[diff_mask, ["Composite Key (3-key)"] + hash_fields].copy()

        # Vectorized per-field previous-value lookup for just this subset
        prev_field_vals = {}
        for f in hash_fields:
            if f in prev_lookup.columns:
                prev_field_vals[f] = diff_subset["Composite Key (3-key)"].map(prev_lookup[f]).fillna("")
            else:
                prev_field_vals[f] = pd.Series([""] * len(diff_subset), index=diff_subset.index)

        # Per-field diff boolean matrix (vectorized column-by-column, not row-by-row)
        field_diff_bool = pd.DataFrame(
            {f: diff_subset[f].astype(str) != prev_field_vals[f].astype(str) for f in hash_fields},
            index=diff_subset.index,
        )

        diff_strings = []
        rearranged_vals = []
        for idx in diff_subset.index:
            diffing_fields = [f for f in hash_fields if field_diff_bool.at[idx, f]]
            diffs = [
                f"{f}: '{prev_field_vals[f].loc[idx]}' -> '{diff_subset.at[idx, f]}'"
                for f in diffing_fields
            ]
            diff_strings.append("; ".join(diffs))

            all_diffs_in_group = len(diffing_fields) > 0 and all(f in rearrangement_group for f in diffing_fields)
            if all_diffs_in_group:
                cur_group = [diff_subset.at[idx, f] for f in rearrangement_group]
                prev_group = [prev_field_vals[f].loc[idx] for f in rearrangement_group]
                rearranged_vals.append(_rearranged_but_same(cur_group, prev_group))
            else:
                rearranged_vals.append(False)

        cur.loc[diff_mask, "Field Differences"] = diff_strings
        cur.loc[diff_mask, "Rearranged"] = rearranged_vals

    cur = cur.drop(columns=["_CUR_HASH", "_PREV_HASH_LOOKUP"], errors="ignore")
    return cur.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step 5: Clean-file cross-check
# ---------------------------------------------------------------------------

def clean_file_cross_check(subset_df, previous_clean_df, three_key_cols=None, not_found_label="Not found in cleaned file"):
    """
    For a given subset of matched records (exact matches OR provable
    rearrangements — anything considered safe to carry forward), check
    whether the 3-key exists in the previous-month cleaned file.

    Returns:
        eligible_df   - found in the clean file (ready to carry forward)
        not_found_df  - NOT found in the clean file (goes to exceptions)
    """
    three_key_cols = three_key_cols or config.THREE_KEY

    clean = previous_clean_df.copy()
    clean["Composite Key (3-key)"] = _make_key(clean, three_key_cols)
    clean_keys = set(clean["Composite Key (3-key)"])

    subset_df = subset_df.copy()
    found_mask = subset_df["Composite Key (3-key)"].isin(clean_keys)
    eligible = subset_df[found_mask].copy()
    not_found = subset_df[~found_mask].copy()

    if not not_found.empty:
        not_found["Exception Category"] = not_found_label

    return eligible.reset_index(drop=True), not_found.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step 6: Build Cleaned Output File
# ---------------------------------------------------------------------------

def build_cleaned_output(eligible_df, previous_clean_df, three_key_cols=None, hash_fields=None):
    """
    For each eligible record, copy the full current-month row and replace
    only the identity/contact fields with values from the previous-month
    cleaned file. All other current-month fields are left untouched.
    """
    three_key_cols = three_key_cols or config.THREE_KEY
    hash_fields = hash_fields or config.ALL_HASH_FIELDS

    clean = previous_clean_df.copy()
    clean["Composite Key (3-key)"] = _make_key(clean, three_key_cols)
    clean_lookup = clean.drop_duplicates(subset="Composite Key (3-key)", keep="first").set_index(
        "Composite Key (3-key)"
    )

    out = eligible_df.copy()
    for f in hash_fields:
        if f in clean_lookup.columns:
            out[f] = out["Composite Key (3-key)"].map(clean_lookup[f]).fillna(out.get(f, ""))

    out["Source"] = "Carried forward from previous cleaned file"
    return out.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step 7: Build Exception Output File
# ---------------------------------------------------------------------------

def build_exception_output(true_different_df, no_match_df, not_found_exact_df, not_found_rearranged_df):
    """
    Combine everything that still needs a human look into one labeled file:
      - true 'Different hash' (real changes, NOT provable rearrangements)
      - 'No previous-month match'
      - 'Same hash, not found in cleaned file' (exact match, no clean record)
      - 'Rearranged, not found in cleaned file' (rearranged, no clean record)
    Provable rearrangements that DID find a clean-file match are handled
    separately — see build_cleaned_output / the rearranged output file.
    """
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

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True, sort=False)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def build_workbook(outputs, summary=None):
    """
    Bundle all output DataFrames into a single .xlsx workbook, one sheet per
    category, with a Summary sheet first. Uses xlsxwriter in constant_memory
    mode so it stays fast/lightweight even at 100-150K+ rows per sheet.

    Returns raw bytes, ready for a Streamlit download_button.
    """
    sheet_names = {
        "dedup_log": "Dedup Log",
        "date_mismatch": "Date Mismatch",
        "cleaned_output": "Cleaned Output",
        "rearranged_output": "Rearranged (Auto)",
        "exception_output": "Exception File",
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
            if df is None or df.empty:
                # Still create the sheet, with headers if available, so the
                # workbook's tab list matches expectations even when a
                # category had zero rows this run.
                (df if df is not None else pd.DataFrame()).to_excel(
                    writer, sheet_name=sheet_name, index=False
                )
            else:
                df.to_excel(writer, sheet_name=sheet_name, index=False)

    buffer.seek(0)
    return buffer.getvalue()


def run_pipeline(current_raw_df, previous_raw_df, previous_clean_df):
    """
    Runs the full pipeline and returns a dict of output DataFrames plus a
    summary of counts at each stage.

    Output files:
        dedup_log         - duplicates removed
        date_mismatch      - 2-key match, disbursement date differs
        cleaned_output      - exact matches, carried forward from clean file
        rearranged_output   - provable rearrangements, auto-processed and
                              carried forward from clean file (own file, for
                              transparency/audit — not silently merged into
                              cleaned_output)
        exception_output    - everything still needing a human look
    """
    summary = {}

    # Step 1
    deduped_df, dedup_log_df = deduplicate(current_raw_df)
    summary["Input records"] = len(current_raw_df)
    summary["Duplicates removed"] = len(dedup_log_df)
    summary["After dedup"] = len(deduped_df)

    # Step 2
    working_df, date_mismatch_df = date_mismatch_check(deduped_df, previous_raw_df)
    summary["Date mismatches pulled out"] = len(date_mismatch_df)
    summary["Working set"] = len(working_df)

    # Step 3 + 4
    matched_df = match_and_hash(working_df, previous_raw_df)

    exact_df = matched_df[matched_df["Match Flag"] == "Exact match"].copy()
    rearranged_df = matched_df[(matched_df["Match Flag"] == "Different") & (matched_df["Rearranged"])].copy()
    true_different_df = matched_df[(matched_df["Match Flag"] == "Different") & (~matched_df["Rearranged"])].copy()
    no_match_df = matched_df[matched_df["Match Flag"] == "No previous-month match"].copy()

    summary["Exact match"] = len(exact_df)
    summary["Rearranged (auto-processed)"] = len(rearranged_df)
    summary["Different (real change)"] = len(true_different_df)
    summary["No previous-month match"] = len(no_match_df)

    # Step 5 — run the clean-file cross-check for both exact and rearranged
    eligible_exact_df, not_found_exact_df = clean_file_cross_check(
        exact_df, previous_clean_df, not_found_label="Same hash, not found in cleaned file"
    )
    eligible_rearranged_df, not_found_rearranged_df = clean_file_cross_check(
        rearranged_df, previous_clean_df, not_found_label="Rearranged, not found in cleaned file"
    )
    summary["Eligible for carry-forward (exact)"] = len(eligible_exact_df)
    summary["Eligible for carry-forward (rearranged)"] = len(eligible_rearranged_df)
    summary["Same hash, not found in cleaned file"] = len(not_found_exact_df)
    summary["Rearranged, not found in cleaned file"] = len(not_found_rearranged_df)

    # Step 6 — two carry-forward outputs, kept separate for audit clarity
    cleaned_output_df = build_cleaned_output(eligible_exact_df, previous_clean_df)
    rearranged_output_df = build_cleaned_output(eligible_rearranged_df, previous_clean_df)
    if not rearranged_output_df.empty:
        rearranged_output_df["Source"] = "Auto-processed: same ID values, different field order"

    # Step 7
    exception_output_df = build_exception_output(
        true_different_df, no_match_df, not_found_exact_df, not_found_rearranged_df
    )

    outputs = {
        "dedup_log": dedup_log_df,
        "date_mismatch": date_mismatch_df,
        "cleaned_output": cleaned_output_df,
        "rearranged_output": rearranged_output_df,
        "exception_output": exception_output_df,
    }

    return outputs, summary