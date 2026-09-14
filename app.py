"""
CKMS — Customer Key Management System
Streamlit interface for the registry-backed monthly data-cleaning pipeline.

Replaces the old 3-4 file upload/diff flow: you now upload ONE file (this
month's submission). Subscriber, reporting period, and submission type are
parsed from the filename (<CODE><MMYY>_<IND|BUS>[_<SEQ>]); "previous month"
raw/clean/UNL data comes from the registry database instead of a second and
third upload. Ingestion is incremental -- each file is matched against
everything already in the registry for that subscriber+period+type, without
waiting for other sequence files in the same month.
"""

import pandas as pd
import streamlit as st
from sqlalchemy import text

import pipeline
import ingestion

st.set_page_config(page_title="CKMS — Registry-Backed Cleaning Pipeline", layout="wide")

st.title("CKMS — Automated Monthly Data Cleaning")
st.caption(
    "Upload this month's submission file. Subscriber, period, and type are read "
    "from the filename; matching runs against the registry, not a second upload."
)


# ---------------------------------------------------------------------------
# One cached DB connection per session, reused across every upload/run.
# ---------------------------------------------------------------------------

@st.cache_resource
def get_cached_engine():
    return ingestion.get_engine()


with st.sidebar:
    st.header("Database Connection")
    st.caption("Registry database — separate from Cleanser's database (merge deferred to a future build).")
    if st.button("Test Connection"):
        try:
            engine = get_cached_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            st.success("Connected.")
        except Exception as e:
            st.error(f"Connection failed: {e}")

    st.divider()
    st.caption(
        "⚠️ Per-subscriber column-name overrides (e.g. a subscriber using "
        "DRIVERLICNUM instead of DriverLicNum) are not yet supported here — "
        "ingestion uses the fixed field names in config.py for every "
        "subscriber. Flagging this as an open gap, not a silent limitation."
    )

# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

st.subheader("1. Upload This Month's Submission")
uploaded_file = st.file_uploader(
    "Filename must follow <SUBSCRIBERCODE><MMYY>_<IND|BUS>[_<SEQ>], e.g. CPOINT0626_IND",
    type=["csv", "xlsx"],
)

parsed_meta = None
if uploaded_file is not None:
    try:
        parsed_meta = ingestion.parse_filename(uploaded_file.name)
        st.info(
            f"**Subscriber:** {parsed_meta['subscriber_code']}  |  "
            f"**Period:** {parsed_meta['reporting_period']}  |  "
            f"**Type:** {parsed_meta['submission_type']}  |  "
            f"**Sequence:** {parsed_meta['sequence_number']}"
        )
    except ValueError as e:
        st.error(str(e))

run_clicked = st.button("Run Pipeline", type="primary", disabled=parsed_meta is None)

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if run_clicked:
    status = st.status("Running pipeline...", expanded=True)
    try:
        engine = get_cached_engine()

        status.update(label=f"Ingesting {uploaded_file.name} and matching against the registry...")
        outputs, summary, meta = ingestion.process_file(engine, uploaded_file, uploaded_file.name)

        status.update(label="Done", state="complete", expanded=False)

        st.session_state.pop("workbook_bytes", None)
        st.session_state["outputs"] = outputs
        st.session_state["summary"] = summary
        st.session_state["meta"] = meta

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
    summary = st.session_state["summary"]
    meta = st.session_state["meta"]

    st.subheader("2. Summary")
    total_auto = (
        summary["Eligible for carry-forward (exact)"]
        + summary["Eligible for carry-forward (rearranged)"]
        + summary["Enriched (auto-processed)"]
    )
    total_unl = (
        summary["Different (needs review)"]
        + summary["No previous-month match"]
        + summary["Same hash, not found in cleaned file"]
        + summary["Rearranged, not found in cleaned file"]
        + summary["Previously UNL'd, still unresolved (carryover)"]
    )
    cols = st.columns(4)
    cols[0].metric("Input records", f"{summary['Input records']:,}")
    cols[1].metric("Auto-cleaned", f"{total_auto:,}")
    cols[2].metric("Sent to UNL", f"{total_unl:,}")
    cols[3].metric("Date changes (info only)", f"{summary['Date changes logged (informational)']:,}")

    with st.expander("Full summary"):
        st.table(pd.DataFrame(summary.items(), columns=["Metric", "Count"]))

    st.subheader("3. Download Outputs")
    outputs = st.session_state["outputs"]

    def to_csv_bytes(df):
        return df.to_csv(index=False).encode("utf-8")

    labels = {
        "dedup_log": ("Dedup Log", "Duplicate rows removed (3-key), with balances and which row was kept"),
        "date_change_log": ("Date Changes (Info)", "Disbursement date differed from last period — informational only"),
        "cleaned_output": ("Cleaned Output", "Exact-match records, identity/contact fields carried forward"),
        "rearranged_output": ("Rearranged Output", "Same ID values under different fields — auto-processed"),
        "enriched_output": ("Enriched Output", "New identity data added, no conflicts — auto-processed"),
        "unl_output": ("UNL File", "Genuine conflicts, new records, and not-found cases needing review"),
    }

    dl_cols = st.columns(len(labels))
    for i, key in enumerate(labels.keys()):
        df = outputs[key]
        label, desc = labels[key]
        with dl_cols[i]:
            st.markdown(f"**{label}**")
            st.caption(f"{len(df):,} rows")
            st.caption(desc)
            st.download_button(
                "Download",
                data=to_csv_bytes(df) if not df.empty else b"",
                file_name=f"{key}.csv",
                mime="text/csv",
                disabled=df.empty,
                key=f"dl_{key}",
            )

    st.divider()
    if st.button("Prepare combined workbook (.xlsx)"):
        with st.spinner("Building workbook..."):
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
    preview_choice = st.selectbox("Preview a file", list(l[0] for l in labels.values()))
    preview_map = {v[0]: k for k, v in labels.items()}
    st.dataframe(outputs[preview_map[preview_choice]].head(200), use_container_width=True)

    # -----------------------------------------------------------------
    # Submission history — new capability the registry enables that the
    # old 3-file-diff model had no equivalent for.
    # -----------------------------------------------------------------
    st.subheader("5. Submission History")
    st.caption(f"Past submissions for {meta['subscriber_code']} ({meta['submission_type']}).")
    try:
        history_df = ingestion.get_submission_history(
            get_cached_engine(), meta["subscriber_code"], meta["submission_type"]
        )
        st.dataframe(history_df, use_container_width=True)
    except Exception as e:
        st.warning(f"Couldn't load submission history: {e}")

else:
    st.info("Upload this month's submission file above, then click **Run Pipeline**.")
