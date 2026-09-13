"""
CKMS Pipeline Configuration
---------------------------
Column names below are the DEFAULTS based on the AB3 bulk subscriber file
layout. If a specific subscriber's file uses different header names, override
these in the Streamlit sidebar at runtime — nothing below is hardcoded into
the pipeline logic itself, it's just the starting default.
"""

# --- Composite key fields (used for all matching) ---
KEY_FACILITY = "Credit FacilityAccNum"
KEY_CUSTOMER = "CustomerID"
KEY_DATE = "DisbursementDate"

THREE_KEY = [KEY_FACILITY, KEY_CUSTOMER, KEY_DATE]
TWO_KEY = [KEY_FACILITY, KEY_CUSTOMER]

# --- Balance field used for dedup tie-breaking ---
BALANCE_COL = "CurBal"

# --- Identity / contact fields to hash and compare ---
# NOTE: these must match your subscriber's ACTUAL file headers exactly
# (case- and whitespace-sensitive). Common alternate spellings seen in
# practice: DRIVERSLICENUM vs DRIVERLICNUM, OTHERID vs OTHERIDTYPE.
# If a column is missing, the app will now stop with a clear error listing
# your file's real headers — adjust these (or the sidebar overrides) to
# match rather than letting a mismatch pass silently.
IDENTITY_FIELDS = [
    "NatIDNum",       # National ID
    "VotersIDNum",    # Voter ID
    "DriverLicNum", # Driver's Licence
    "PassportNum",    # Passport
    "SSNum",         # SSN
    "EzwichNum",      # Ezwich
    "OtherIDType",        # Other ID Type
    "OtherIDNum",     # Other ID Number
    "TINum",          # TIN
]

CONTACT_FIELDS = [
    "HomeTel",   # Home Phone
    "MobileTel1",   # Mobile Phone 1
    "MobileTel2",   # Mobile Phone 2
    "WorkTel",   # Work Phone
]

# Fields checked as a group for the "same values, different field" pattern.
# Covers ALL identity ID-type fields — a value can shift between ANY of
# these (e.g. an ID under Passport this month that was under Driver's
# Licence last month) and still represent the same underlying data, just
# recorded against a different field label.
REARRANGEMENT_GROUP = list(IDENTITY_FIELDS)

ALL_HASH_FIELDS = IDENTITY_FIELDS + CONTACT_FIELDS

# Columns that must be forced to string on read, to avoid losing leading
# zeros or having pandas silently coerce IDs/phone numbers to numeric.
FORCE_STRING_COLS = list(set(ALL_HASH_FIELDS + THREE_KEY))

HASH_DELIMITER = "|"