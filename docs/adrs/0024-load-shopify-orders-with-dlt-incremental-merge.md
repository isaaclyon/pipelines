# 0024: Load Shopify orders with dlt incremental merge

Date: 2026-10-13

## Status

Accepted on 2026-10-14. Amends
[0017](0017-ingest-data-with-dlt.md) for custom sources. Replaces
the extraction design of the
[raw ingestion specification](../specs/20261009-shopify-raw-ingestion.md) with the
[rebuild specification](../specs/20261013-shopify-orders-dlt-rebuild.md).

## Context

[ADR 0017](0017-ingest-data-with-dlt.md) chose dlt so that sources would share its
normalization, schema handling, incremental state and loading. It also allowed
custom Python sources for "GraphQL, bulk operations, or extraction behavior the
existing sources cannot express reliably." The Shopify orders pipeline used that
allowance to build a framework around dlt.

Measured at commit `a578ed9` (2026-10-13), `packages/ingestion` holds 7,130 source
lines and 4,519 test lines plus a 1,680-line schema snapshot, for one pipeline.
A separate observer (1,009 source and 616 test lines) is being deleted separately;
the remaining 6,121 source lines still use dlt only as the final loader inside
`shopify/writer.py`. Around it sit:

- bulk operation submission, polling, download and resubmission recovery
  (`extract.py`, 830 lines);
- a full ID listing after every export to prove the export was complete;
- retained batch files, a seven-state batch lifecycle and `progress.json` with
  coverage intervals, an exception ledger and a discrepancy ledger
  (`batches.py`, 631 lines);
- two file locks (`operation.lock` and the writer lock);
- an eight-outcome version guard that reads destination versions before every
  merge, with equal-version conflict and revision rules (`writer.py`, 925 lines);
- extraction revisions, `reextract` batches, `revision_mismatch` retirement and
  separate correction discrepancies (spread across `service.py`,
  `batches.py` and `writer.py`);
- a weekly deletion check, a nightly reconciliation and Pacific-day window
  policy (`service.py`, `planning.py`);
- an operator CLI that resumes, replays and backfills outside Dagster
  (`__main__.py`).

The raw ingestion specification asked for the opposite. Its section 1 said "Use
one batch record alongside dlt state; no separate manifest/receipt/ledger
services." `progress.json` now holds two ledgers. The CLI's `resume`, `replay`
and `backfill` commands write warehouse data outside Dagster, which
[ADR 0021](0021-run-data-changes-as-orchestrated-pipeline-work.md) rules out:
"Every data change runs afterward as an ordinary Dagster run."

Each mechanism answered a real observation, but later evidence weakens most of
them:

- **Bulk speed.** An earlier peak and catch-up test measured bulk at 1.5 and 4.6 minutes against 12.6 and 37.2 minutes for the
  peak and catch-up cohorts. The comparison was against *sequential per-order*
  reads (2,614 and 7,297 requests, about one per order). It did not measure an
  `orders` connection paginated many orders per page.
- **Identity listing.** It was added after a spike found a 464 count versus
  463 listing discrepancy. A change-capture study then showed that updated orders leave earlier windows and appear in later ones.
  An `updated_at` incremental with overlap already handles that cursor movement.
- **Stale-version guard.** A local spike reproduced an older *retained-file replay* overwriting newer data. The hazard
  exists because files are replayed and because refresh, backfill and replay can
  load in an order different from extraction. Serialized runs that extract and
  load in one step, with no replay of old files, do not create it.
- **Equal-version conflicts.** A child-change discovery study compared 34
  successive
  observations at an equal `Order.updatedAt` and found zero selected-field
  differences. Its 184-order live probe found none either.
- **Change discovery.** The same report found no child change at an unchanged
  parent timestamp, but its gate remains open: it cannot prove that every refund,
  transaction status change, order edit or fulfillment-only change advances
  `updatedAt`. Every mechanism in the current pipeline discovers orders through
  the same `updated_at` search, so none of them closes this gap either.

History is already loaded, at a scale of millions of orders. A rebuilt pipeline
can continue into the same tables rather than reload history, so backfill speed
matters only for a rare full re-extraction.

## Decision

Rebuild Shopify orders ingestion as one dlt source whose `orders` resource uses
dlt's own incremental cursor, merge disposition and state. Remove the custom
batch, ledger, lock, guard and revision framework.

- **Resource.** A dlt resource reads the Admin GraphQL `orders` connection sorted
  by `UPDATED_AT`, filtered to `updated_at` at or after the cursor and before a
  fixed per-run upper bound. Cursor state is `dlt.sources.incremental("updated_at")`
  with a two-minute `lag` for late indexing. The resource follows nested
  connection pages for each order before yielding it, and raises on any
  incomplete page or collection so that nothing from the run loads. An order
  that fails row parsing is different: it is skipped and recorded in a small
  rejects table, and the next run re-reads open rejects by ID. One malformed
  order does not hold back the rest of the day's data.
- **Write behavior.** `merge` on (`shop_id`, `order_id`) with dlt's nested tables
  under the root. Each merged order replaces its child rows. The latest
  observation always wins; there is no destination read before the merge.
- **State.** dlt pipeline state, stored in the destination and restored at the
  start of each run, replaces `progress.json`, batch records and retained files.
  The cursor advances only when its load package commits. A failed run re-reads
  the source on the next run.
- **Single writer.** Dagster is the only writer. Every job that writes the
  dataset carries the existing `analytics/exclusive-resource=shopify-orders` tag,
  so runs serialize without file locks. The CLI is removed.
- **Pagination.** Use ordinary paginated GraphQL for refresh, lookback,
  backfill and re-extraction. Do not use bulk operations.
- **Monitoring.** Each successful run appends one row to `ingestion_batches`
  recording the window it covered. Raw freshness becomes a dbt source freshness
  check on that table, owned by the transformation package.
- **Re-extraction.** A manual Dagster job runs the same resource over explicit
  `created_at` bounds without touching cursor state. Merge rewrites rows whose
  source version is unchanged, which satisfies ADR 0021's re-extraction rule
  without revision guards. `_extraction_revision` remains a stamped integer that
  records which logic produced a row.
- **Raw contract.** Keep the physical table names, columns, keys and decimal
  `(38, 9)` money of the raw orders dataset, so staging and marts do not change
  at cutover ([ADR 0020](0020-keep-consecutive-releases-compatible.md)).

### Why pagination instead of bulk

Bulk is faster for large windows but brings an operation lifecycle (submission,
ambiguous-submission recovery, polling, partial results, JSONL download and
parent reassembly). Bulk also cannot return connections inside list fields, so
refund children and revision-2 commercial connections already need ordinary
GraphQL follow-up reads. The current pipeline therefore maintains two extraction
paths for every order.

Routine windows are small: three retained refresh batches held 2,919 root
observations in total. The heaviest measured window, a ten-minute sale peak,
held 2,332 orders. Assuming 25 orders per page, that is about 94 connection
requests plus follow-ups, against 2,614 sequential per-order requests. A full
re-extraction of a store with about two million orders at the same page size is
about 80,000 requests: a day-scale, partitioned background job that
runs rarely. One extraction path is worth that cost.

This choice has a measured gate. The rebuild specification requires the
paginated resource to extract a ten-minute peak cohort, with the full
revision-2 field set, within four minutes on a development store. If it fails,
add bulk as a second resource for backfill and catch-up only, in a follow-up
record; do not restore the batch framework.

### General rule for custom sources

This record amends ADR 0017's custom-source allowance. A custom source may
replace how records are fetched and parsed. It must use dlt's incremental, merge
and state primitives for cursors, keyed updates and recovery, and Dagster for
scheduling, retries and serialization. Batch ledgers, file locks, replay stores
and version guards need their own accepted decision with evidence that dlt and
Dagster cannot provide the guarantee.

## Consequences

- The package shrinks to a budget of 1,600 source lines and 1,500 test lines
  (excluding the schema snapshot), from 6,121 and 3,903 without the observer.
  The specification allocates the budget by module.
- Recovery becomes rerunning the job. There is no resume, replay or batch
  inspection to operate, and run containers no longer need durable ingestion
  state on a mounted volume.
- Retained source files go away. A past observation cannot be replayed offline;
  recovery and correction re-read the source. Shopify remains the system of
  record and the raw tables the only retained copy.
- Without the guard, correctness depends on serialization. Any future writer
  outside the exclusive-resource tag could load an older observation over a newer
  one. Tests must show that a pending package from a failed run loads before the
  next extraction.
- An equal-version content difference is applied silently. The study observed none;
  if one appears it overwrites with the later observation, which the current
  writer also does for refresh.
- The child-change discovery gap stays open and unchanged. This record neither
  closes nor widens it; an event-labelled validation remains the gate for
  a full-scope freshness claim.
- The per-export identity check and nightly reconciliation are replaced by a
  weekly count reconciliation per creation month, which also finds deletion
  candidates. A missing order is detected within a week of the run that missed it.
- `ingestion_batches`, `_batch_id`, `_observation_sequence` and `_content_digest`
  keep their columns for compatibility, with simpler or null values. The
  contract release that deletes the old modules, after seven clean production
  days, also drops `_observation_sequence` and `_content_digest` under ADR 0020.
- Peak throughput becomes a measured precondition. If the gate fails, the remedy
  is a bulk resource inside the same dlt source; the batch framework stays gone.
- On acceptance: add a dated addendum to ADR 0017 linking here, reduce the raw
  ingestion specification to its field contract, and update the ingestion
  README.
