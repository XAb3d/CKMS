"""
CKMS Backfill Runner
---------------------
Replays historical monthly submission files into the registry, oldest to
newest, using the EXACT SAME ingestion.process_file() path live uploads use
-- no separate backfill-only logic, so backfilled history and future live
runs are guaranteed to be produced the same way.

Usage:
    python3 backfill.py /path/to/historical_files

Expects a directory (searched recursively) containing files named per the
standard convention: <SUBSCRIBERCODE><MMYY>_<IND|BUS>[_<SEQ>].csv/.xlsx

Chronological order matters: a period's data must be fully loaded before the
next period is processed, since matching always compares against "whatever
is already in the registry for the prior period." Files are sorted by
(ReportingPeriod, SubscriberCode, SubmissionType, SequenceNumber) and
processed strictly in that order -- periods are never interleaved out of
order, even though the underlying registry writes are technically safe to
interleave across different subscribers.

Resumable by construction: before processing a file, this checks whether a
Submissions row already exists for that exact subscriber+period+type+seq
(the same UNIQUE constraint that would reject a duplicate live upload) and
skips it if so. A crashed or interrupted run can simply be re-launched from
the top -- already-loaded files are skipped, not reprocessed.

KNOWN LIMITATION, not fixed here: dedup only runs within a single file being
uploaded (pipeline.deduplicate operates on current_raw_df only). If the same
record genuinely appears across TWO sequence files in the same period (e.g.
seq_1 and seq_2 both contain the same FacilityAccNum+CustomerID+Date), it
will NOT be caught as a duplicate across files -- each file's own internal
duplicates are still caught correctly. Confirm this is an acceptable
assumption (sequence files are non-overlapping batches) before relying on
backfilled dedup counts for anything audit-sensitive.
"""

import sys
import glob
import os
import traceback

import ingestion


def discover_files(root_dir):
    """
    Recursively finds .csv/.xlsx files under root_dir, parses each filename,
    and returns a list of (path, filename, meta) tuples for files that match
    the naming convention. Files that don't match are reported separately
    rather than silently skipped.
    """
    candidates = sorted(
        glob.glob(os.path.join(root_dir, "**", "*.csv"), recursive=True)
        + glob.glob(os.path.join(root_dir, "**", "*.xlsx"), recursive=True)
    )

    matched, unmatched = [], []
    for path in candidates:
        filename = os.path.basename(path)
        try:
            meta = ingestion.parse_filename(filename)
            matched.append((path, filename, meta))
        except ValueError:
            unmatched.append(path)

    return matched, unmatched


def sort_chronologically(matched):
    """
    Sort strictly by (ReportingPeriod, SubscriberCode, SubmissionType,
    SequenceNumber) -- every file for period N is processed before ANY file
    for period N+1, which is the property the whole backfill depends on.
    """
    return sorted(
        matched,
        key=lambda item: (
            item[2]["reporting_period"],
            item[2]["subscriber_code"],
            item[2]["submission_type"],
            item[2]["sequence_number"],
        ),
    )


def already_processed(engine, meta):
    """
    Checks Submissions for an existing row matching this exact
    subscriber+period+type+sequence -- the natural resume checkpoint, since
    it's the same key the live UNIQUE constraint enforces.
    """
    import pandas as pd
    result = pd.read_sql(
        """SELECT TOP 1 s.SubmissionID
           FROM Submissions s
           JOIN Subscribers sub ON sub.SubscriberID = s.SubscriberID
           WHERE sub.SubscriberCode = ? AND s.ReportingPeriod = ?
             AND s.SubmissionType = ? AND s.SequenceNumber = ?""",
        engine,
        params=(
            meta["subscriber_code"], meta["reporting_period"],
            meta["submission_type"], meta["sequence_number"],
        ),
    )
    return not result.empty


def run_backfill(root_dir):
    engine = ingestion.get_engine()

    matched, unmatched = discover_files(root_dir)
    if unmatched:
        print(f"WARNING: {len(unmatched)} file(s) did not match the naming convention and will be skipped:")
        for p in unmatched:
            print(f"  - {p}")
        print()

    ordered = sort_chronologically(matched)
    print(f"Found {len(ordered)} matching file(s) across "
          f"{len({m['reporting_period'] for _, _, m in ordered})} period(s).\n")

    stats = {"processed": 0, "already_loaded": 0, "failed": 0, "total_input_records": 0}
    failures = []

    for i, (path, filename, meta) in enumerate(ordered, start=1):
        prefix = f"[{i}/{len(ordered)}] {filename}"

        if already_processed(engine, meta):
            print(f"{prefix} — already loaded, skipping")
            stats["already_loaded"] += 1
            continue

        try:
            with open(path, "rb") as f:
                outputs, summary, _ = ingestion.process_file(engine, f, filename)
            stats["processed"] += 1
            stats["total_input_records"] += summary["Input records"]
            print(
                f"{prefix} — OK "
                f"({summary['Input records']:,} rows; "
                f"exact={summary['Exact match']:,} "
                f"rearranged={summary['Rearranged (auto-processed)']:,} "
                f"enriched={summary['Enriched (auto-processed)']:,} "
                f"different={summary['Different (needs review)']:,} "
                f"no-match={summary['No previous-month match']:,})"
            )
        except Exception as e:
            stats["failed"] += 1
            failures.append((filename, str(e)))
            print(f"{prefix} — FAILED: {e}")
            traceback.print_exc()
            # Deliberately continue rather than abort -- one bad file
            # shouldn't stop 18 months of otherwise-good data from loading.
            # Re-running the script later will retry only the failed ones,
            # since everything else is now checkpointed as already-loaded.

    print("\n" + "=" * 60)
    print("BACKFILL SUMMARY")
    print("=" * 60)
    print(f"Files processed this run:  {stats['processed']}")
    print(f"Files already loaded:      {stats['already_loaded']}")
    print(f"Files failed:              {stats['failed']}")
    print(f"Total input records:       {stats['total_input_records']:,}")
    if failures:
        print("\nFailed files (re-run this script to retry these):")
        for fname, err in failures:
            print(f"  - {fname}: {err}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 backfill.py /path/to/historical_files")
        sys.exit(1)
    run_backfill(sys.argv[1])
