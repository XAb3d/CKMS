/* ============================================================================
   CKMS Registry Schema
   ----------------------------------------------------------------------------
   Target: new SQL Server database, SEPARATE from Cleanser's database.
   Merging the two is explicitly deferred to a future build (see project notes).

   Design decisions baked into this schema (from design discussion):
     - IdentityKey is 4-part: SubscriberCode|SubmissionType|FacilityAccNum|CustomerID
         (SubmissionType included because the same CustomerID could in theory be
         assigned to both an individual and a business by the same subscriber)
     - DisbursementDate is NEVER part of the identity key and NEVER part of the
         content hash. Date changes are logged informationally in DateChangeLog
         only -- they never block a match and never route a record to review.
     - IND and BUS submissions have different identity/contact field sets, so
         MasterRawRegistry/MasterCleanRegistry store the type-specific fields as
         JSON (IdentityFieldsJSON) rather than fixed typed columns, to avoid a
         schema migration every time a new submission type or field is added.
     - MasterRawRegistry is APPEND-ONLY. Dedup marks IsDuplicate=1, it never
         deletes rows -- nothing already processed is unrecoverably lost.
     - Ingestion is INCREMENTAL per file: insert this file's rows, then run
         dedup/match against everything already in the registry for that
         period+type. We do not wait for all sequence files in a month.
     - MasterUNLRegistry is the "UNL" table from the original Raw/Clean/UNL
         design -- previously called ErrorQueue in early discussion, renamed
         here to match the terminology used from the start.
   ============================================================================ */

CREATE TABLE Subscribers (
    SubscriberID    INT IDENTITY PRIMARY KEY,
    SubscriberCode  NVARCHAR(20)  NOT NULL UNIQUE,     -- e.g. 'CPOINT'
    SubscriberName  NVARCHAR(255) NULL
);

CREATE TABLE Submissions (
    SubmissionID     INT IDENTITY PRIMARY KEY,
    SubscriberID     INT NOT NULL REFERENCES Subscribers(SubscriberID),
    ReportingPeriod  CHAR(7)      NOT NULL,             -- 'YYYY-MM', parsed from MMYY in filename
    SubmissionType   NVARCHAR(10) NOT NULL,             -- 'IND' or 'BUS'
    SequenceNumber   INT NOT NULL DEFAULT 1,            -- filename suffix, e.g. CPOINT0626_BUS_1
    FileName         NVARCHAR(255),
    RowCount         INT,
    UploadedAt       DATETIME2 DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_Submissions UNIQUE (SubscriberID, ReportingPeriod, SubmissionType, SequenceNumber)
);

CREATE TABLE MasterRawRegistry (
    RawRecordID         BIGINT IDENTITY PRIMARY KEY,
    SubmissionID        INT NOT NULL REFERENCES Submissions(SubmissionID),
    SubscriberID        INT NOT NULL REFERENCES Subscribers(SubscriberID),
    SubmissionType      NVARCHAR(10) NOT NULL,          -- 'IND' or 'BUS'
    ReportingPeriod     CHAR(7)      NOT NULL,
    FacilityAccNum      NVARCHAR(100) NOT NULL,
    CustomerID          NVARCHAR(100) NOT NULL,
    BranchCode          NVARCHAR(50)  NULL,
    CreditFacilityType  NVARCHAR(20)  NULL,
    DisbursementDate    DATE NULL,                      -- tracked, never part of key/hash
    CurBal              DECIMAL(18,2) NULL,              -- dedup tie-breaker
    IdentityKey         AS (CAST(SubscriberID AS NVARCHAR(20)) + '|' + SubmissionType
                            + '|' + FacilityAccNum + '|' + CustomerID) PERSISTED,
    ContentHash         NVARCHAR(64) NOT NULL,          -- identity+contact fields only, per SubmissionType, date excluded
    IdentityFieldsJSON  NVARCHAR(MAX) NOT NULL,         -- the type-specific ID+contact field values, as submitted
    RawPayload          NVARCHAR(MAX) NOT NULL,         -- full as-submitted row (all ~140 columns), JSON -- nothing lost
    IsDuplicate         BIT DEFAULT 0,
    DedupReason         NVARCHAR(255) NULL,
    KeptOverThisRecordID BIGINT NULL,                   -- which RawRecordID was kept instead, if this one was a dup
    CreatedAt           DATETIME2 DEFAULT SYSUTCDATETIME()
);
CREATE INDEX IX_RawRegistry_IdentityKey ON MasterRawRegistry(IdentityKey, ReportingPeriod);
CREATE INDEX IX_RawRegistry_Submission ON MasterRawRegistry(SubmissionID);

CREATE TABLE MasterCleanRegistry (
    CleanRecordID       BIGINT IDENTITY PRIMARY KEY,
    IdentityKey         NVARCHAR(210) NOT NULL,
    SubmissionType      NVARCHAR(10) NOT NULL,
    ReportingPeriod     CHAR(7)      NOT NULL,
    SourceRawRecordID   BIGINT NOT NULL REFERENCES MasterRawRegistry(RawRecordID),
    IdentityFieldsJSON  NVARCHAR(MAX) NOT NULL,         -- authoritative clean values, type-specific shape
    MatchCategory       NVARCHAR(20)  NOT NULL,         -- 'Exact' | 'Rearranged' | 'Enriched'
    Status              NVARCHAR(20)  DEFAULT 'Clean',
    CreatedAt           DATETIME2 DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_CleanRegistry UNIQUE (IdentityKey, ReportingPeriod)
);

CREATE TABLE MasterUNLRegistry (
    UNLRecordID       BIGINT IDENTITY PRIMARY KEY,
    RawRecordID       BIGINT NOT NULL REFERENCES MasterRawRegistry(RawRecordID),
    IdentityKey       NVARCHAR(210) NOT NULL,
    ReportingPeriod   CHAR(7)      NOT NULL,
    ExceptionCategory NVARCHAR(150) NOT NULL,
    -- Exception categories in use:
    --   'Different hash'                                       (genuine Changed/Removed field conflict)
    --   'No previous-month match'
    --   'Same hash, not found in cleaned file'
    --   'Rearranged, not found in cleaned file'
    --   'Matched in Raw History - Previously Routed to UNL, Never Resolved in Clean'
    Details           NVARCHAR(MAX) NULL,               -- e.g. field-level diff string
    Resolved          BIT DEFAULT 0,
    ResolvedInPeriod  CHAR(7) NULL,
    CreatedAt         DATETIME2 DEFAULT SYSUTCDATETIME()
);
CREATE INDEX IX_UNLRegistry_IdentityKey ON MasterUNLRegistry(IdentityKey);
CREATE INDEX IX_UNLRegistry_Unresolved ON MasterUNLRegistry(Resolved) WHERE Resolved = 0;

CREATE TABLE DateChangeLog (
    DateChangeID        BIGINT IDENTITY PRIMARY KEY,
    IdentityKey         NVARCHAR(210) NOT NULL,
    ReportingPeriod     CHAR(7) NOT NULL,
    PreviousDate        DATE NULL,
    CurrentDate         DATE NULL,
    CreditFacilityType  NVARCHAR(20) NULL,
    -- Informational for every facility type (decided in discussion) --
    -- kept as a queryable audit trail only; never blocks a match, never
    -- routes to MasterUNLRegistry.
    CreatedAt           DATETIME2 DEFAULT SYSUTCDATETIME()
);
CREATE INDEX IX_DateChangeLog_IdentityKey ON DateChangeLog(IdentityKey, ReportingPeriod);
