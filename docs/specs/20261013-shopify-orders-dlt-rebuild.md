# Shopify orders dlt-native rebuild

Date: 2026-10-13\
Status: Accepted 2026-10-14 with [ADR 0024](../adrs/0024-load-shopify-orders-with-dlt-incremental-merge.md).
Implemented; the migration in section 4 is complete.\

This specification scopes the replacement of the custom Shopify orders pipeline
in `packages/ingestion` with a minimal dlt-native pipeline. Section 4 records
the migration. The [raw field contract](20261009-shopify-raw-ingestion.md),
including its revision-2 tables, stays in force.

## 1. Target shape

| Concern | Target |
| --- | --- |
| Source | One dlt source, `shopify_orders`, with one `orders` resource |
| Extraction | Admin GraphQL `orders` connection, API `2026-10`, `sortKey: UPDATED_AT`, query `updated_at:>='<start>' updated_at:<'<end>'`. `<end>` is the run's whole-second UTC start time; rows at or after it are dropped |
| Nested data | The page query selects the revision-2 shape with small `first:` values. For each order whose nested connection reports `hasNextPage`, follow-up queries fetch the rest before the order is yielded. Each page of complete orders goes to dlt as it is read. Sales agreement pages keep the existing agreement-ID lookup |
| Completeness | Port the existing parse-boundary checks: final page reached with advancing cursors, unique child IDs, `transactionsCount` exact and equal to returned length, GraphQL errors and partial responses rejected, API version header checked. Any failure raises; nothing from the run loads |
| Rejects | An order whose node fails row parsing is skipped and appended to `shopify_order_rejects` (`run_id`, `order_id`, `updated_at`, `error`, `rejected_at`). Each refresh re-reads open rejects by ID first; a reject resolves when its order loads. A Dagster asset check warns while any reject is open and fails after 24 hours |
| Cursor | `dlt.sources.incremental("updated_at", initial_value=<seed>, lag=120)`, closed start. Replaces `planning.py` overlap logic |
| Write | `merge`, primary key (`shop_id`, `order_id`), dlt nested tables `orders__*` with `_dlt_root_id`. Delete-insert replaces child rows of each merged order |
| Schema | dlt inference with explicit hints only for keys, decimal `(38, 9)` money, timestamps, and nullable columns added after revision 1. Schema contract `evolve` for columns and `freeze` for data types. The committed schema snapshot stays the compatibility test |
| State | dlt pipeline state in the destination, `restore_from_destination` enabled. No `progress.json`, batch records or retained files. The ingestion volume stays: it holds dlt's working directory, so a killed run's load package survives until the next run loads it |
| Run evidence | After a successful load, append one `ingestion_batches` row per run: run ID as `batch_id`, `kind`, `window_field`, `window_start`, `window_end`, `status = complete`, `completeness = complete`, observation times, `orders_observed`, `api_version`, `query_version`, `extraction_revision` |
| Row metadata | `_observed_at` = page fetch time; `_batch_id` = Dagster run ID; `_extraction_kind` = job kind; `_api_version`, `_query_version` as today; `_extraction_revision = 3`; `_observation_sequence` and `_content_digest` null |
| Orchestration | `dagster-dlt` assets under the existing key `raw/shopify/shopify_orders_raw`. All writing jobs carry `analytics/exclusive-resource=shopify-orders`. Schedules defined in Dagster with `execution_timezone` |
| Monitoring | dbt `sources.yml` freshness on `ingestion_batches` with `loaded_at_field: window_end`: warn after 25 minutes, error after 30, matching today's policy. Lives in `packages/transformations` |

### Jobs

| Job | Trigger | Work |
| --- | --- | --- |
| `shopify_orders_refresh` | Every 5 minutes (per environment, as today) | Incremental run from the dlt cursor |
| `shopify_orders_lookback` | Nightly 00:15 America/Los_Angeles | Same resource, explicit `updated_at` range of the last 26 hours, cursor untouched. Catches indexing later than the two-minute lag |
| `shopify_orders_reextract` | Manual, partitioned by creation month | Same resource filtered by `created_at` bounds, cursor untouched. Used for logic changes under ADR 0021 and for gaps found by reconciliation |
| `shopify_orders_reconcile_counts` | Weekly, Sunday 03:00 America/Los_Angeles | Exact `ordersCount` per UTC creation month, up to the latest complete refresh, against raw orders not marked unavailable; reads raw orders by ID only for months that differ, so cost follows raw, not Shopify; Shopify's count minus the raw orders still returned is the gap; an absence needs two direct reads before it is written to `order_source_status`. No ID listing: the 2026-10-15 shadow run listed 63 months and exhausted its 20-minute budget. A deletion and a gap in one month cancel in the counts |

Dagster retries a failed run once. A second failure leaves the cursor where it
was; the next tick retries. Priorities keep refresh ahead of the other jobs.

### Bulk versus pagination

Paginated GraphQL is chosen in [ADR 0024](../adrs/0024-load-shopify-orders-with-dlt-incremental-merge.md#why-pagination-instead-of-bulk).
Stage 1 sets the page size from measured query cost and must pass the throughput
gate in section 4 before any other stage starts.

## 2. Current behavior disposition

Line counts are from commit `a578ed9`. Keep means the behavior survives in the
new package. Re-home means the guarantee moves to dlt, Dagster or dbt. Drop means
it goes away.

| Current behavior | Where | Disposition | Justification |
| --- | --- | --- | --- |
| Bulk operation submit, poll, download, ambiguous-submission recovery | `extract.py` | Drop | Pagination covers refresh and rare re-extraction with one path; bulk already needs GraphQL follow-up for children. Gate in section 4 |
| Nested connection follow-up and agreement sales lookup | `extract.py` | Keep | Required for the revision-2 contract regardless of extraction method |
| Completeness checks (pages, cursors, duplicate IDs, transaction counts, partial responses, API version) | `extract.py`, `client.py`, `assemble.py` | Keep | Parse-boundary guarantees under ADR 0016; failure now fails the run instead of opening an exception |
| Moving-parent version check and bounded refetch during hydration | `extract.py` | Drop | A parent that moves during a run has a newer `updatedAt` and is read again by the next run; merge keeps the latest |
| Identity listing after every export | `extract.py`, `service.py` | Re-home | Raised by a one-order count/listing gap, later explained as cursor movement. Weekly count reconciliation per creation month detects real gaps; reextract repairs them |
| Preflight count and window splitting by count and bytes | `service.py`, `batches.py` | Drop | Pagination needs no window sizing; dlt streams pages. Run deadline stays a Dagster timeout |
| Retained batch files and offline replay | `batches.py`, `service.py` | Drop | Replay caused a stale-overwrite hazard. Recovery is a source re-read; failed loads keep dlt's pending package until the next run |
| Batch records, seven-state lifecycle, `progress.json` coverage intervals | `batches.py` | Re-home | Cursor to dlt state; run outcome to Dagster run history; covered window to the `ingestion_batches` row |
| Exception and discrepancy ledgers | `batches.py`, `service.py` | Drop | The 2026-10-09 specification already excluded ledger services. Incomplete data fails the run and stays visible in Dagster; reconciliation finds residual gaps |
| `operation.lock` and writer lock | `service.py`, `writer.py` | Re-home | Dagster tag concurrency (`analytics/exclusive-resource`) already serializes runs; with the CLI gone it is the only entry point |
| Stale-version guard (destination read before merge, eight decisions) | `writer.py` | Drop | The hazard came from replay and out-of-order loads. Serialized extract-and-load runs without replay load in observation order. Test in section 3 proves pending-package ordering |
| Equal-version conflict and preserve rules | `writer.py` | Drop | A study found zero equal-timestamp content differences in 34 pairs and a 184-order probe. Latest observation wins |
| Extraction revisions, `reextract` batch kind, `revision_mismatch`, correction discrepancies | `revision.py`, `service.py`, `batches.py`, `writer.py` | Re-home | Re-extraction is the same resource over `created_at` bounds; merge overwrites unconditionally. `_extraction_revision` stays a stamped constant because marts read `_extraction_revision >= 2` |
| Empty-table setup before first load | `writer.py` | Drop | Tables already exist in every environment that matters; local and CI fixtures create them by loading |
| Required-table and child-count verification after load | `writer.py` | Re-home | Becomes dbt source tests (uniqueness, not-null keys, child `order_id` relationships) in the transformation package |
| Nightly reconciliation with reporting-day planning | `service.py`, `planning.py` | Re-home | Nightly lookback job: same resource, fixed 26-hour range. Timezone handling moves to the Dagster schedule |
| Refresh window and two-minute overlap | `planning.py`, `operations.py` | Re-home | dlt incremental `lag=120` |
| Historical backfill with adaptive chunks | `service.py` | Re-home | History is already loaded. Any future backfill is `shopify_orders_reextract` over creation-month partitions |
| Weekly deletion check (ID inventory, second enumeration, direct read) | `service.py`, `extract.py` | Re-home | Part of weekly count reconciliation; Direct reads run only for months whose counts differ. Still writes `order_source_status`, which staging reads |
| `ingestion_batches` evidence table | `writer.py`, `model.py` | Keep (reduced) | Freshness needs an explicit covered window; a latest-row time cannot prove coverage. One row per successful run |
| Coverage metadata on the Dagster asset | `operations.py`, `service.py` | Drop | Freshness moves to dbt source freshness on `ingestion_batches`; the Python check in `shopify_freshness.py` is retired after cutover |
| CLI `status`, `batches`, `discrepancies` | `__main__.py` | Drop | Read-only views of state that no longer exists; Dagster UI and the raw tables replace them |
| CLI `resume`, `replay`, `backfill` | `__main__.py` | Drop | Writes outside Dagster conflict with ADR 0021 |
| Per-chunk budgets (requests, bytes, active time) | `client.py`, `service.py` | Drop | Replaced by a Dagster run timeout and the client's throttle backoff. Bytes cap only protected bulk downloads |
| Child-change discovery gap | n/a | Keep open | Same `updated_at` discovery as before; the rebuild neither closes nor widens it. An event-labelled validation remains the gate for a full-scope freshness claim |

## 3. Tests for the rebuilt package

Offline, deterministic, against a fake GraphQL transport and DuckDB:

- pagination across pages and nested follow-up pages; duplicate child IDs and
  non-advancing cursors fail;
- incomplete transactions or partial responses fail the run and load nothing;
- an order that fails row parsing is recorded as a reject while the rest of the
  run loads; the next run re-reads it by ID and resolves the reject;
- cursor advances only after a committed load; a failed load followed by a new
  run loads the pending package before the new extraction, so an older
  observation never lands after a newer one;
- repeat run with no source changes writes no order rows and one evidence row;
- merge replaces removed children for an updated order;
- `reextract` over `created_at` bounds rewrites equal-version rows and leaves
  the cursor unchanged;
- money stays exact at nine places; timestamps are UTC;
- schema snapshot matches the committed contract;
- reconciliation flags a month with a count difference and marks only confirmed
  absences.

Remote checks in development Snowflake repeat the merge, child replacement and
evidence-row cases.

## 4. Migration and parity

The rebuild replaced an earlier custom pipeline in three stages. Only the
lessons that still shape the current design are kept here.

1. **Throughput gate.** The paginated source extracted a ten-minute sale-peak
   cohort, with the full revision-2 shape, in about one minute with four readers
   and no throttling, against a four-minute gate. Each Snowflake load adds about
   90 seconds regardless of size, so refresh defaults to four readers
   (`SHOPIFY_ORDERS_REFRESH_WORKERS`).
2. **Shadow run.** The new pipeline ran beside the old one into a separate
   dataset. Parity passed on every compared window, kill tests during the read
   and the load recovered, and old orders edited during a run matched the
   source's latest version. Two findings carried forward:
   - **Seed gap.** Leave no gap between the last loaded instant and the refresh
     seed (`SHOPIFY_ORDERS_SEED`); orders created in a gap and never edited are
     otherwise missed until a re-extract.
   - **Queued-window race.** A run's `end` is fixed at launch, so a refresh
     queued behind another can report versions a later run already superseded.
3. **Cutover.** Clone the raw dataset, stop the old schedules, seed the first
   refresh one hour before the old pipeline's processed end, deploy, then start
   refresh, lookback and reconciliation and run one reconciliation by hand. A
   pending load package that references dropped staging tables can be cleared
   with `drop_pending_packages()` so the next run re-extracts from the seed.

### Rollback

Rollback is restoring a pre-change clone of the raw dataset and re-extracting
the affected windows with a `reextract` run of the dlt job.

## 5. Line budget

Measured at `a578ed9`: 7,130 source lines (6,121 without the observer) and 4,519
test lines (3,903 without the observer), plus the 1,680-line schema snapshot.

| Module | Responsibility | Budget |
| --- | --- | ---: |
| `shopify/queries.py` | GraphQL documents, ported | 300 |
| `shopify/client.py` | POST, API version check, throttle backoff, error parsing | 150 |
| `shopify/source.py` | dlt source and resource, incremental, nested follow-ups, completeness | 350 |
| `shopify/rows.py` | Typed parsing of nodes into rows, decimals, ancestor IDs | 350 |
| `shopify/schema.py` | Table names and column hints that dlt cannot infer | 200 |
| `shopify/reconcile.py` | Weekly counts, direct reads for differing months, absence confirmation | 120 |
| `shopify/rejects.py` | Rejects table, re-read by ID, asset check | 60 |
| `defs/shopify_orders.py` | Assets, jobs, schedules, run evidence row | 130 |
| Total source | | **1,660** |
| Tests | Excluding the schema snapshot | **1,500** |

That is a 73% source reduction from the package without the observer. Exceeding
a module budget by more than 20% needs a note in the PR explaining what dlt or
Dagster could not provide.

## 6. Decisions and remaining measurements

Decided on 2026-10-14:

- A malformed order is skipped and recorded (section 1, Rejects), not a run
  failure.
- The post-cutover contract release drops `_observation_sequence` and
  `_content_digest`. As shipped on 2026-10-15 it made them nullable instead;
  dropping them is a breaking schema change under ADR 0020 and waits for a
  release that needs it.

Measured in stage 1 on 2026-10-14 (read-only probes against a production store):

- The store is on Shopify Plus: a 20,000-point bucket restoring 1,000 points
  per second.
- Query cost per page of the revision-2 shape with five inline line items:

  | Page size | Requested cost | Actual cost | Seconds |
  | --- | --- | --- | --- |
  | 50 | 772 | 311 | 3.68 |
  | 100 | 992 | 383 | 7.52 |

  100 orders with ten inline line items, and 250 with two, exceed the
  1,000-point single-query limit. The page size is 50; orders with more line
  items or child rows are completed with follow-up reads.
- One sequential pass over the peak cohort took 47 pages and 166
  seconds of page reads. Available points never fell below 19,648, so
  throttling is not the limit; request latency is. Follow-ups: 21 orders with
  more line pages, 62 agreements with more sales pages, 81 orders with 84
  refunds. The source splits a window into concurrent whole-second slices so
  the full run, follow-ups included, fits the gate.
- `ordersCount` returned `EXACT` for a full busy month and for the peak cohort,
  so reconciliation can count whole creation months.
