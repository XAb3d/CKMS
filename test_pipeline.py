"""
Quick sanity test of pipeline.py using synthetic data that exercises every
branch: dedup, date-mismatch, exact match, different hash, rearrangement,
no-match, and not-found-in-clean-file.
"""
import pandas as pd
import pipeline
import config

# --- Previous month RAW file ---
prev_raw = pd.DataFrame([
    # F001/C001: baseline, will be an exact match this month
    dict(FACILITYACCNUM="F001", CUSTOMERID="C001", DISBURSEMENTDATE="20250101",
         CURBALANCE="1000", NATIDNUM="GHA-1", OTHERID="TYPE1", OTHERIDNUM="NUM1", TINUM="TIN1",
         HOMETEL="", MOBILE1="0201111111", MOBILE2="", WORKTEL="", VOTERSIDNUM="", DRIVERSLICENUM="",
         PASSPORTNUM="", SSNNUM="", EZWICHNUM=""),
    # F002/C002: identity will change (real different hash)
    dict(FACILITYACCNUM="F002", CUSTOMERID="C002", DISBURSEMENTDATE="20250102",
         CURBALANCE="2000", NATIDNUM="GHA-2", OTHERID="TYPE2", OTHERIDNUM="NUM2", TINUM="TIN2",
         HOMETEL="", MOBILE1="0202222222", MOBILE2="", WORKTEL="", VOTERSIDNUM="", DRIVERSLICENUM="",
         PASSPORTNUM="", SSNNUM="", EZWICHNUM=""),
    # F003/C003: values will be REARRANGED this month (same values, shifted cols)
    dict(FACILITYACCNUM="F003", CUSTOMERID="C003", DISBURSEMENTDATE="20250103",
         CURBALANCE="3000", NATIDNUM="GHA-3", OTHERID="STAFF", OTHERIDNUM="1532196", TINUM="GHA-716155030-2",
         HOMETEL="", MOBILE1="0203333333", MOBILE2="", WORKTEL="", VOTERSIDNUM="", DRIVERSLICENUM="",
         PASSPORTNUM="", SSNNUM="", EZWICHNUM=""),
    # F004/C004: date will differ this month (2-key matches, date doesn't)
    dict(FACILITYACCNUM="F004", CUSTOMERID="C004", DISBURSEMENTDATE="20250104",
         CURBALANCE="4000", NATIDNUM="GHA-4", OTHERID="TYPE4", OTHERIDNUM="NUM4", TINUM="TIN4",
         HOMETEL="", MOBILE1="0204444444", MOBILE2="", WORKTEL="", VOTERSIDNUM="", DRIVERSLICENUM="",
         PASSPORTNUM="", SSNNUM="", EZWICHNUM=""),
    # F005/C005: exact match but missing from clean file (not-found case)
    dict(FACILITYACCNUM="F005", CUSTOMERID="C005", DISBURSEMENTDATE="20250105",
         CURBALANCE="5000", NATIDNUM="GHA-5", OTHERID="TYPE5", OTHERIDNUM="NUM5", TINUM="TIN5",
         HOMETEL="", MOBILE1="0205555555", MOBILE2="", WORKTEL="", VOTERSIDNUM="", DRIVERSLICENUM="",
         PASSPORTNUM="", SSNNUM="", EZWICHNUM=""),
    # F007/C007: ID rearranged ACROSS DIFFERENT FIELDS (Driver's Licence <-> Passport).
    # This is the exact case reported: same two values present, just swapped
    # between fields relative to what current month will show below.
    dict(FACILITYACCNUM="F007", CUSTOMERID="C007", DISBURSEMENTDATE="20250107",
         CURBALANCE="7000", NATIDNUM="GHA-7", OTHERID="TYPE7", OTHERIDNUM="NUM7", TINUM="TIN7",
         HOMETEL="", MOBILE1="0207777777", MOBILE2="", WORKTEL="", VOTERSIDNUM="",
         DRIVERSLICENUM="XYZ99", PASSPORTNUM="ABC12",
         SSNNUM="", EZWICHNUM=""),
])

# --- Previous month CLEANED file ---
prev_clean = pd.DataFrame([
    dict(FACILITYACCNUM="F001", CUSTOMERID="C001", DISBURSEMENTDATE="20250101",
         NATIDNUM="GHA-1-CLEAN", OTHERID="TYPE1", OTHERIDNUM="NUM1", TINUM="TIN1",
         HOMETEL="", MOBILE1="0201111111", MOBILE2="", WORKTEL="", VOTERSIDNUM="", DRIVERSLICENUM="",
         PASSPORTNUM="", SSNNUM="", EZWICHNUM=""),
    dict(FACILITYACCNUM="F003", CUSTOMERID="C003", DISBURSEMENTDATE="20250103",
         NATIDNUM="GHA-3-CLEAN", OTHERID="STAFF", OTHERIDNUM="1532196", TINUM="GHA-716155030-2",
         HOMETEL="", MOBILE1="0203333333", MOBILE2="", WORKTEL="", VOTERSIDNUM="", DRIVERSLICENUM="",
         PASSPORTNUM="", SSNNUM="", EZWICHNUM=""),
    # F005 intentionally NOT present -> should trigger "not found in clean file"
    dict(FACILITYACCNUM="F007", CUSTOMERID="C007", DISBURSEMENTDATE="20250107",
         NATIDNUM="GHA-7-CLEAN", OTHERID="TYPE7", OTHERIDNUM="NUM7", TINUM="TIN7",
         HOMETEL="", MOBILE1="0207777777", MOBILE2="", WORKTEL="", VOTERSIDNUM="",
         DRIVERSLICENUM="XYZ99-CLEAN", PASSPORTNUM="ABC12-CLEAN",
         SSNNUM="", EZWICHNUM=""),
])

# --- Current month RAW file ---
current_raw = pd.DataFrame([
    # F001/C001 duplicated with two balances -> dedup should keep the LOWER (900)
    dict(FACILITYACCNUM="F001", CUSTOMERID="C001", DISBURSEMENTDATE="20250101",
         CURBALANCE="900", NATIDNUM="GHA-1", OTHERID="TYPE1", OTHERIDNUM="NUM1", TINUM="TIN1",
         HOMETEL="", MOBILE1="0201111111", MOBILE2="", WORKTEL="", VOTERSIDNUM="", DRIVERSLICENUM="",
         PASSPORTNUM="", SSNNUM="", EZWICHNUM=""),
    dict(FACILITYACCNUM="F001", CUSTOMERID="C001", DISBURSEMENTDATE="20250101",
         CURBALANCE="1500", NATIDNUM="GHA-1", OTHERID="TYPE1", OTHERIDNUM="NUM1", TINUM="TIN1",
         HOMETEL="", MOBILE1="0201111111", MOBILE2="", WORKTEL="", VOTERSIDNUM="", DRIVERSLICENUM="",
         PASSPORTNUM="", SSNNUM="", EZWICHNUM=""),
    # F002/C002 real identity change
    dict(FACILITYACCNUM="F002", CUSTOMERID="C002", DISBURSEMENTDATE="20250102",
         CURBALANCE="2100", NATIDNUM="GHA-2-CHANGED", OTHERID="TYPE2", OTHERIDNUM="NUM2", TINUM="TIN2",
         HOMETEL="", MOBILE1="0202222222", MOBILE2="", WORKTEL="", VOTERSIDNUM="", DRIVERSLICENUM="",
         PASSPORTNUM="", SSNNUM="", EZWICHNUM=""),
    # F003/C003 rearranged: OTHERID/OTHERIDNUM/TINUM values rotated
    dict(FACILITYACCNUM="F003", CUSTOMERID="C003", DISBURSEMENTDATE="20250103",
         CURBALANCE="3100", NATIDNUM="GHA-3", OTHERID="GHA-716155030-2", OTHERIDNUM="STAFF", TINUM="1532196",
         HOMETEL="", MOBILE1="0203333333", MOBILE2="", WORKTEL="", VOTERSIDNUM="", DRIVERSLICENUM="",
         PASSPORTNUM="", SSNNUM="", EZWICHNUM=""),
    # F004/C004 same 2-key, DIFFERENT date -> date mismatch file
    dict(FACILITYACCNUM="F004", CUSTOMERID="C004", DISBURSEMENTDATE="20250199",
         CURBALANCE="4100", NATIDNUM="GHA-4", OTHERID="TYPE4", OTHERIDNUM="NUM4", TINUM="TIN4",
         HOMETEL="", MOBILE1="0204444444", MOBILE2="", WORKTEL="", VOTERSIDNUM="", DRIVERSLICENUM="",
         PASSPORTNUM="", SSNNUM="", EZWICHNUM=""),
    # F005/C005 exact match, but missing from clean file
    dict(FACILITYACCNUM="F005", CUSTOMERID="C005", DISBURSEMENTDATE="20250105",
         CURBALANCE="5100", NATIDNUM="GHA-5", OTHERID="TYPE5", OTHERIDNUM="NUM5", TINUM="TIN5",
         HOMETEL="", MOBILE1="0205555555", MOBILE2="", WORKTEL="", VOTERSIDNUM="", DRIVERSLICENUM="",
         PASSPORTNUM="", SSNNUM="", EZWICHNUM=""),
    # F006/C006 brand new, no previous month record at all
    dict(FACILITYACCNUM="F006", CUSTOMERID="C006", DISBURSEMENTDATE="20250606",
         CURBALANCE="6000", NATIDNUM="GHA-6", OTHERID="TYPE6", OTHERIDNUM="NUM6", TINUM="TIN6",
         HOMETEL="", MOBILE1="0206666666", MOBILE2="", WORKTEL="", VOTERSIDNUM="", DRIVERSLICENUM="",
         PASSPORTNUM="", SSNNUM="", EZWICHNUM=""),
    # F007/C007: Driver's Licence and Passport SWAPPED vs previous month
    # (prev_raw had DRIVERSLICENUM=XYZ99, PASSPORTNUM=ABC12).
    # Same two values, just moved to the other field -> should be
    # auto-processed into the rearranged output, not the exception file.
    dict(FACILITYACCNUM="F007", CUSTOMERID="C007", DISBURSEMENTDATE="20250107",
         CURBALANCE="7100", NATIDNUM="GHA-7", OTHERID="TYPE7", OTHERIDNUM="NUM7", TINUM="TIN7",
         HOMETEL="", MOBILE1="0207777777", MOBILE2="", WORKTEL="", VOTERSIDNUM="",
         DRIVERSLICENUM="ABC12", PASSPORTNUM="XYZ99",
         SSNNUM="", EZWICHNUM=""),
])

outputs, summary = pipeline.run_pipeline(current_raw, prev_raw, prev_clean)

print("=== SUMMARY ===")
for k, v in summary.items():
    print(f"{k}: {v}")

print("\n=== DEDUP LOG (expect F001 dup with balance 1500 dropped) ===")
print(outputs["dedup_log"][["FACILITYACCNUM", "CUSTOMERID", "This Row Balance", "Kept Balance", "Dedup Reason"]] if not outputs["dedup_log"].empty else "EMPTY")

print("\n=== DATE MISMATCH (expect F004/C004) ===")
print(outputs["date_mismatch"][["FACILITYACCNUM", "CUSTOMERID", "Current Month Disbursement Date", "Previous Month Disbursement Date"]] if not outputs["date_mismatch"].empty else "EMPTY")

print("\n=== CLEANED OUTPUT (exact matches only - expect just C001) ===")
print(outputs["cleaned_output"][["FACILITYACCNUM", "CUSTOMERID", "NATIDNUM", "Source"]] if not outputs["cleaned_output"].empty else "EMPTY")

print("\n=== REARRANGED OUTPUT (auto-processed - expect C003 and C007) ===")
rearr_cols = ["FACILITYACCNUM", "CUSTOMERID", "DRIVERSLICENUM", "PASSPORTNUM", "OTHERID", "OTHERIDNUM", "TINUM", "Source"]
print(outputs["rearranged_output"][rearr_cols] if not outputs["rearranged_output"].empty else "EMPTY")

print("\n=== EXCEPTION OUTPUT (expect C002 Different, C006 No-match, C005 not-found) ===")
cols = ["FACILITYACCNUM", "CUSTOMERID", "Match Flag", "Exception Category", "Field Differences"]
print(outputs["exception_output"][cols] if not outputs["exception_output"].empty else "EMPTY")

# --- Assertions ---
assert summary["Duplicates removed"] == 1, "Dedup should remove exactly 1 row"
kept_bal = outputs["dedup_log"]["Kept Balance"].iloc[0]
assert float(kept_bal) == 900, f"Should keep lowest balance (900), got {kept_bal}"

assert len(outputs["date_mismatch"]) == 1, "Should have exactly 1 date mismatch"
assert outputs["date_mismatch"]["CUSTOMERID"].iloc[0] == "C004"

# Only C001 is a true exact-match -> goes to Cleaned Output.
cleaned_ids = set(outputs["cleaned_output"]["CUSTOMERID"])
assert cleaned_ids == {"C001"}, f"Expected only C001 in cleaned_output, got {cleaned_ids}"
f001_row = outputs["cleaned_output"][outputs["cleaned_output"]["CUSTOMERID"] == "C001"].iloc[0]
assert f001_row["NATIDNUM"] == "GHA-1-CLEAN"

# C003 (OTHERID/OTHERIDNUM/TINUM rotated) and C007 (Driver's Licence <->
# Passport swapped) are BOTH provable rearrangements -> auto-processed into
# rearranged_output, carrying forward the clean file's field arrangement.
rearranged_ids = set(outputs["rearranged_output"]["CUSTOMERID"])
assert rearranged_ids == {"C003", "C007"}, f"Expected C003 and C007 in rearranged_output, got {rearranged_ids}"

f007_row = outputs["rearranged_output"][outputs["rearranged_output"]["CUSTOMERID"] == "C007"].iloc[0]
assert f007_row["DRIVERSLICENUM"] == "XYZ99-CLEAN", "F007 should carry forward the CLEAN file's field placement"
assert f007_row["PASSPORTNUM"] == "ABC12-CLEAN"

exc = outputs["exception_output"]
cat_counts = exc["Exception Category"].value_counts().to_dict()
print("\nException category counts:", cat_counts)
assert cat_counts.get("Different hash") == 1, "F002 (real NATIDNUM change) must be 'Different hash'"
assert "Same values - different field order (confirm before cleaning)" not in cat_counts, \
    "Rearrangements should be auto-processed now, not sitting in the exception file"
assert cat_counts.get("No previous-month match") == 1  # F006
assert cat_counts.get("Same hash, not found in cleaned file") == 1  # F005
assert summary["Rearranged (auto-processed)"] == 2, "F003 and F007 should both be detected as rearranged"

print("\nALL ASSERTIONS PASSED")