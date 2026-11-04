# Incremental loading and state

Read the [incremental loading docs](https://dlthub.com/docs/general-usage/incremental-loading),
[lag docs](https://dlthub.com/docs/general-usage/incremental/lag),
[merge docs](https://dlthub.com/docs/general-usage/merge-loading), and
[state docs](https://dlthub.com/docs/general-usage/state) for the pinned version.

## Separate extraction from writes

An extraction cursor controls which records the source sends. Write disposition
controls how those records affect destination tables.

- **Merge:** choose stable primary keys for mutable entities or overlapping
  extractions. Define which source version wins if a batch contains multiple
  records for one key; arrival order alone is not a sufficient contract.
- **Append:** appropriate for immutable records when retry and replay behavior
  prevent unwanted duplicates. Declare the deduplication strategy if windows
  overlap or the provider repeats records.
- **Replace:** appropriate for complete snapshots. A partial page or limited
  extraction must never replace a maintained complete table.

Keys may need source/account scope. Verify keys and replacement behavior for
normalized child tables as well as the parent. Merge does not create historical
versions automatically; design history retention when it is required.

## Cursor boundaries and late arrivals

Choose an update cursor for changing entities when the provider supports it.
Parse its type and time zone, set an intentional initial boundary, and verify
null handling and same-cursor-value records. Use the run's incremental start
value to form requests rather than inventing a separate maximum tracker.

Declare `row_order` or early-stop behavior only when provider ordering is
guaranteed and tested. An unordered page with an old record cannot establish
that later pages contain no new records.

Use a measured lookback when records arrive late or are revised. Verify the
pinned version's lag units: datetime cursors use seconds, date cursors use days,
and numeric cursors use their numeric units. Overlap can disable extraction
boundary deduplication; provide destination deduplication, typically keyed
merge. A lookback captures only changes within that window. Define periodic
reconciliation or another recovery path for older corrections.

Absence from an incremental response is not a deletion signal. Use documented
tombstones, deletion endpoints, or complete snapshot reconciliation where
required, and state when the source cannot expose deletions.

## State and backfills

- Keep pipeline, source, and resource identities stable within an environment.
  Renaming them can change checkpoint lookup and trigger re-extraction.
- Isolate working directories and destination state between environments and
  independent tests. Prevent concurrent runs from advancing the same checkpoint.
- Preserve pending load packages where recovery needs them. Destination state
  synchronization alone does not preserve every unfinished local artifact.
- Verify a failed extraction/load followed by retry, and restoration in a fresh
  run container. Confirm loaded rows and cursor continuity together.
- Bound backfills explicitly and keep their state effects deliberate. Check the
  pinned version's start/end range semantics before relying on stateless ranges.
- A reset or sync operation can discard local pending work. Inspect the target
  and recovery needs before executing it; a checkpoint reset is not a substitute
  for a backfill design.

Test repeat runs, same-timestamp records, updates, overlap, supported deletions,
and failure recovery as relevant. Use stable datasets with development mode off
for continuity checks.
