"""
CKMS — Customer Key Management System
Streamlit interface for the automated monthly data-cleaning pipeline.
"""

import io
import pandas as pd
import streamlit as st

import config
import pipeline

st.set_page_config(page_title="CKMS — Monthly Cleaning Pipeline", layout="wide")

st.title("CKMS — Automated Monthly Data Cleaning")
st.caption(
    "Carries forward previously-cleaned identity/contact data for records "
    "proven unchanged from last month. Never overwrites raw files."
)

# ---------------------------------------------------------------------------
# Sidebar: column mapping (override defaults per-subscriber if needed)
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("Column Settings")
    st.caption("Defaults match the standard subscriber layout. Adjust if this subscriber's file differs.")

    with st.expander("Composite key fields", expanded=False):
        key_facility = st.text_input("Facility Account Number column", config.KEY_FACILITY)
        key_customer = st.text_input("Customer ID column", config.KEY_CUSTOMER)
        key_date = st.text_input("Disbursement Date column", config.KEY_DATE)

    with st.expander("Balance field", expanded=False):
        balance_col = st.text_input("Current Balance column", config.BALANCE_COL)

    with st.expander("Identity / contact fields", expanded=False):
        identity_text = st.text_area(
            "One field name per line",
            "\n".join(config.IDENTITY_FIELDS + config.CONTACT_FIELDS),
            height=220,
        )

    with st.expander("Rearrangement group", expanded=False):
        rearr_text = st.text_area(
            "Fields checked for 'same values, different order'",
            "\n".join(config.REARRANGEMENT_GROUP),
            height=80,
        )

three_key = [key_facility, key_customer, key_date]
hash_fields = [f.strip() for f in identity_text.splitlines() if f.strip()]
rearrangement_group = [f.strip() for f in rearr_text.splitlines() if f.strip()]
force_string_cols = list(set(hash_fields + three_key))

# ---------------------------------------------------------------------------
# File uploads
# ---------------------------------------------------------------------------

st.subheader("1. Upload Files")
col1, col2, col3 = st.columns(3)

with col1:
    current_file = st.file_uploader("Current Month Raw File", type=["csv", "xlsx"], key="current")
with col2:
    previous_raw_file = st.file_uploader("Previous Month Raw File", type=["csv", "xlsx"], key="prev_raw")
with col3:
    previous_clean_file = st.file_uploader("Previous Month Cleaned File", type=["csv", "xlsx"], key="prev_clean")

run_clicked = st.button("Run Pipeline", type="primary", disabled=not (current_file and previous_raw_file and previous_clean_file))

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if run_clicked:
    status = st.status("Running pipeline...", expanded=True)

    try:
        status.update(label="Loading files...")
        current_df = pipeline.load_file(current_file, force_string_cols)
        previous_raw_df = pipeline.load_file(previous_raw_file, force_string_cols)
        previous_clean_df = pipeline.load_file(previous_clean_file, force_string_cols)
        status.write(
            f"Loaded: current month ({len(current_df):,} rows), "
            f"previous month ({len(previous_raw_df):,} rows), "
            f"previous cleaned ({len(previous_clean_df):,} rows)"
        )

        status.update(label="Validating column names against configured settings...")
        required_raw = list(dict.fromkeys(three_key + hash_fields + [balance_col]))
        required_clean = list(dict.fromkeys(three_key + hash_fields))
        pipeline.validate_columns(current_df, required_raw, "Current Month Raw File")
        pipeline.validate_columns(previous_raw_df, required_raw, "Previous Month Raw File")
        pipeline.validate_columns(previous_clean_df, required_clean, "Previous Month Cleaned File")
        status.write("All required columns found.")

        status.update(label="Step 1: Deduplicating current month file...")
        deduped_df, dedup_log_df = pipeline.deduplicate(current_df, three_key, balance_col)
        status.write(f"Removed {len(dedup_log_df):,} duplicate row(s). {len(deduped_df):,} rows remain.")

        status.update(label="Step 2: Checking for disbursement date mismatches...")
        working_df, date_mismatch_df = pipeline.date_mismatch_check(deduped_df, previous_raw_df, [key_facility, key_customer], key_date)
        status.write(f"Pulled out {len(date_mismatch_df):,} date-mismatch row(s). {len(working_df):,} rows remain in the working set.")

        status.update(label="Step 3 & 4: Matching keys and comparing identity/contact hashes...")
        matched_df = pipeline.match_and_hash(working_df, previous_raw_df, three_key, hash_fields, rearrangement_group)

        exact_df = matched_df[matched_df["Match Flag"] == "Exact match"].copy()
        rearranged_df = matched_df[(matched_df["Match Flag"] == "Different") & (matched_df["Rearranged"])].copy()
        true_different_df = matched_df[(matched_df["Match Flag"] == "Different") & (~matched_df["Rearranged"])].copy()
        no_match_df = matched_df[matched_df["Match Flag"] == "No previous-month match"].copy()

        status.write(
            f"Exact match: {len(exact_df):,} | Rearranged (auto-processed): {len(rearranged_df):,} | "
            f"Different (real change): {len(true_different_df):,} | No previous-month match: {len(no_match_df):,}"
        )

        status.update(label="Step 5: Cross-checking exact + rearranged matches against previous cleaned file...")
        eligible_exact_df, not_found_exact_df = pipeline.clean_file_cross_check(
            exact_df, previous_clean_df, three_key, not_found_label="Same hash, not found in cleaned file"
        )
        eligible_rearranged_df, not_found_rearranged_df = pipeline.clean_file_cross_check(
            rearranged_df, previous_clean_df, three_key, not_found_label="Rearranged, not found in cleaned file"
        )
        status.write(
            f"Eligible for carry-forward — exact: {len(eligible_exact_df):,}, rearranged: {len(eligible_rearranged_df):,} | "
            f"Not found in cleaned file — exact: {len(not_found_exact_df):,}, rearranged: {len(not_found_rearranged_df):,}"
        )

        status.update(label="Step 6: Building cleaned + rearranged outputs...")
        cleaned_output_df = pipeline.build_cleaned_output(eligible_exact_df, previous_clean_df, three_key, hash_fields)
        rearranged_output_df = pipeline.build_cleaned_output(eligible_rearranged_df, previous_clean_df, three_key, hash_fields)
        if not rearranged_output_df.empty:
            rearranged_output_df["Source"] = "Auto-processed: same ID values, different field order"

        status.update(label="Step 7: Building exception file...")
        exception_output_df = pipeline.build_exception_output(
            true_different_df, no_match_df, not_found_exact_df, not_found_rearranged_df
        )

        status.update(label="Done", state="complete", expanded=False)

        st.session_state.pop("workbook_bytes", None)  # clear any stale workbook from a previous run

        st.session_state["outputs"] = {
            "dedup_log": dedup_log_df,
            "date_mismatch": date_mismatch_df,
            "cleaned_output": cleaned_output_df,
            "rearranged_output": rearranged_output_df,
            "exception_output": exception_output_df,
        }
        st.session_state["summary"] = {
            "Input records": len(current_df),
            "Duplicates removed": len(dedup_log_df),
            "Date mismatches pulled out": len(date_mismatch_df),
            "Exact match": len(exact_df),
            "Rearranged (auto-processed)": len(rearranged_df),
            "Different (real change)": len(true_different_df),
            "No previous-month match": len(no_match_df),
            "Eligible for carry-forward (exact)": len(eligible_exact_df),
            "Eligible for carry-forward (rearranged)": len(eligible_rearranged_df),
            "Same hash, not found in cleaned file": len(not_found_exact_df),
            "Rearranged, not found in cleaned file": len(not_found_rearranged_df),
        }

    except ValueError as e:
        status.update(label="Column mismatch — see details below", state="error")
        st.error(str(e))
    except Exception as e:
        status.update(label="Pipeline failed", state="error")
        st.exception(e)

# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------

if "outputs" in st.session_state:
    st.subheader("2. Summary")
    summary = st.session_state["summary"]
    total_cleaned = summary["Eligible for carry-forward (exact)"] + summary["Eligible for carry-forward (rearranged)"]
    total_exception = (
        summary["Different (real change)"]
        + summary["No previous-month match"]
        + summary["Same hash, not found in cleaned file"]
        + summary["Rearranged, not found in cleaned file"]
    )
    cols = st.columns(4)
    cols[0].metric("Input records", f"{summary['Input records']:,}")
    cols[1].metric("Cleaned automatically", f"{total_cleaned:,}")
    cols[2].metric("Sent to exception file", f"{total_exception:,}")
    cols[3].metric("Date mismatches", f"{summary['Date mismatches pulled out']:,}")

    with st.expander("Full summary"):
        st.table(pd.DataFrame(summary.items(), columns=["Metric", "Count"]))

    st.subheader("3. Download Outputs")
    outputs = st.session_state["outputs"]

    def to_csv_bytes(df):
        return df.to_csv(index=False).encode("utf-8")

    labels = {
        "dedup_log": ("Dedup Log File", "Duplicate rows removed, with balances and which row was kept"),
        "date_mismatch": ("Date Mismatch File", "Same facility+customer, but disbursement date changed"),
        "cleaned_output": ("Cleaned Output File", "Exact-match records with identity/contact fields carried forward"),
        "rearranged_output": ("Rearranged Output File", "Same ID values found under different fields — auto-processed"),
        "exception_output": ("Exception File", "Real changes, new records, and not-found-in-clean cases needing review"),
    }

    dl_cols = st.columns(5)
    for i, key in enumerate(["dedup_log", "date_mismatch", "cleaned_output", "rearranged_output", "exception_output"]):
        df = outputs[key]
        label, desc = labels[key]
        with dl_cols[i]:
            st.markdown(f"**{label}**")
            st.caption(f"{len(df):,} rows")
            st.caption(desc)
            st.download_button(
                f"Download",
                data=to_csv_bytes(df) if not df.empty else b"",
                file_name=f"{key}.csv",
                mime="text/csv",
                disabled=df.empty,
                key=f"dl_{key}",
            )

    st.divider()
    st.caption("Prefer one file with tabs instead of five separate CSVs?")
    if st.button("Prepare combined workbook (.xlsx)"):
        with st.spinner("Building workbook... this takes longer than the CSVs at large file sizes."):
            st.session_state["workbook_bytes"] = pipeline.build_workbook(outputs, summary)

    if "workbook_bytes" in st.session_state:
        st.download_button(
            "Download Combined Workbook (.xlsx)",
            data=st.session_state["workbook_bytes"],
            file_name="ckms_output.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_workbook",
        )

    st.subheader("4. Preview")
    preview_choice = st.selectbox("Preview a file", ["Cleaned Output", "Rearranged Output", "Exception File", "Date Mismatch", "Dedup Log"])
    preview_map = {
        "Cleaned Output": "cleaned_output",
        "Rearranged Output": "rearranged_output",
        "Exception File": "exception_output",
        "Date Mismatch": "date_mismatch",
        "Dedup Log": "dedup_log",
    }
    st.dataframe(outputs[preview_map[preview_choice]].head(200), use_container_width=True)
else:
    st.info("Upload all three files above, then click **Run Pipeline**.")