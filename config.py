"""
CKMS Pipeline Configuration
---------------------------
IND and BUS submissions have different identity/contact field sets (different
columns entirely, not just renamed ones), so field lists are now keyed by
SubmissionType rather than being one global list.

IdentityKey is 4-part: SubscriberCode|SubmissionType|FacilityAccNum|CustomerID
(SubmissionType is included in case a subscriber assigns the same CustomerID
to an individual and a business -- confirmed as a case to guard against).

DisbursementDate is intentionally NOT in any hash-field list below. It is
tracked separately (DateChangeLog) and never affects matching or
classification, for any facility type -- confirmed decision.
"""

# --- Shared key/structural fields (present in both IND and BUS files) ---
KEY_FACILITY = "Credit FacilityAccNum"
KEY_CUSTOMER = "CustomerID"
KEY_DATE = "DisbursementDate"
BRANCH_COL = "BranchCode"
FACILITY_TYPE_COL = "CreditFacilityType"
BALANCE_COL = "CurBal"

# Facility type codes that identify overdraft-type facilities. Kept here
# because CreditFacilityType is still useful context on DateChangeLog rows
# even though it no longer drives severity/routing.
OVERDRAFT_FACILITY_TYPE_CODES = ["121", "V", "Overdraft"]

# Composite keys used for DEDUP ONLY (unchanged -- 3-key, keep-lowest-balance).
# Matching/registry lookups use IdentityKey (SubscriberCode+SubmissionType+
# FacilityAccNum+CustomerID) instead -- see pipeline.py / ingestion.py.
THREE_KEY = [KEY_FACILITY, KEY_CUSTOMER, KEY_DATE]
TWO_KEY = [KEY_FACILITY, KEY_CUSTOMER]

# --- Per-submission-type identity/contact fields ---
# NOTE: these must match each subscriber's ACTUAL file headers exactly
# (case- and whitespace-sensitive). If a column is missing, the app stops
# with a clear error listing the file's real headers (see validate_columns
# in pipeline.py) rather than silently creating a blank column.
FIELDS = {
    "IND": {
        "identity": [
            "NatIDNum",
            "VotersIDNum",
            "DriverLicNum",
            "PassportNum",
            "SSNum",
            "EzwichNum",
            "OtherIDType",
            "OtherIDNum",
            "TINum",
        ],
        "contact": [
            "HomeTel",
            "MobileTel1",
            "MobileTel2",
            "WorkTel",
        ],
    },
    "BUS": {
        "identity": [
            "BusRegNum",
            "PrevRegNum",
            "TINum",
        ],
        "contact": [
            "OfficeTel1",
            "OfficeTel2",
            "OfficeFaxNum",
        ],
    },
}

# Fields checked as a group for the "same values, different field" pattern
# (e.g. an ID under Passport this month that was under Driver's Licence last
# month). Only meaningful for identity-type fields, per submission type.
REARRANGEMENT_GROUP = {
    "IND": list(FIELDS["IND"]["identity"]),
    "BUS": list(FIELDS["BUS"]["identity"]),
}

ALL_HASH_FIELDS = {
    t: FIELDS[t]["identity"] + FIELDS[t]["contact"]
    for t in FIELDS
}

# Columns that must be forced to string on read, to avoid losing leading
# zeros or having pandas silently coerce IDs/phone numbers to numeric.
FORCE_STRING_COLS = {
    t: list(set(ALL_HASH_FIELDS[t] + THREE_KEY + [BRANCH_COL, FACILITY_TYPE_COL]))
    for t in FIELDS
}

HASH_DELIMITER = "|"
IDENTITY_KEY_DELIMITER = "|"

# Match classification categories, in the order they should be reported.
# 'Enriched' is new: every differing field is an ADDED value (blank last
# month, populated now) with no Changed/Removed fields -- auto-processed,
# current-month values kept as-is, does NOT go through carry-forward
# substitution the way Exact/Rearranged do.
MATCH_CATEGORIES = ["Exact match", "Rearranged", "Enriched", "Different", "No previous-month match"]
