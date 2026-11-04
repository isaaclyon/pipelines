# Ingestion

Owns dlt source extraction and loading, ingestion state and recovery, and the
ingestion Dagster code location. This independent Python project owns its
`src/`, `tests/`, dependency declarations, and uv lockfile.

```sh
./scripts/platform local check ingestion
./scripts/platform local dg ingestion list defs
```

Run commands from the repository root. Checks exercise an installed wheel,
strict typing, formatting, dependency compatibility, and blocked sibling imports.
The `scaffold_check` job verifies non-root execution in an isolated run directory
without a Docker socket.

## Shopify orders

`analytics_ingestion.shopify` loads raw Shopify orders, lines, refunds, payment
transactions, discounts, shipping charges, taxes and sales agreements into the
environment's raw dataset with dlt, as defined by the
[rebuild specification](../../docs/specs/20261013-shopify-orders-dlt-rebuild.md)
and [ADR 0024](../../docs/adrs/0024-load-shopify-orders-with-dlt-incremental-merge.md). The [raw field contract](../../docs/specs/20261009-shopify-raw-ingestion.md)
defines the tables and columns.

| Module | Responsibility |
| --- | --- |
| `queries`, `client` | Pinned 2026-10 queries; GraphQL client with a request/time/byte budget |
| `model` | Source rows to raw tables with exact decimals and ancestor IDs |
| `source` | Paged reads, follow-up completion and rejects |
| `pipeline` | Column hints, write settings and the dlt pipeline shared with the customer source |
| `reconcile` | Monthly count comparison and confirmed-deletion marks |
| `extract` | Shop identity, order counts and cursor paging shared by the two above |
| `defs/shopify_orders.py` | Dagster assets, two jobs and three schedules (all stopped by default) |

Each raw dataset, table and column declaration in `model`, `customer_model`
and `source` (including rejects and evidence tables) carries a description.
`Column` requires one, and `tests/test_raw_documentation.py` fails for a
missing or blank description. dlt-managed tables and `_dlt_*` columns are
excluded.

### Configuration

Configuration is explicit; nothing falls back to production.

| Variable | Meaning |
| --- | --- |
| `ANALYTICS_ENVIRONMENT` | `local` loads DuckDB under the state root; `development`/`production` load Snowflake; `ci` is refused |
| `SHOPIFY_CREDENTIALS_PATH` | JSON file with `shop_domain` (`<shop>.myshopify.com`) and `access_token` |
| `ANALYTICS_INGESTION_STATE_ROOT` | Durable state root shared by every source (default `/var/lib/analytics/ingestion`) |
| `SHOPIFY_ORDERS_DATASET` | Raw schema (default `SHOPIFY_ORDERS_RAW`) |
| `SHOPIFY_ORDERS_SEED` | The first refresh's start (ISO instant with offset). Later refreshes resume from the dlt cursor. Unset means no schedules |
| `SHOPIFY_REFRESH_CRON`, `SHOPIFY_REFRESH_TIMEZONE` | Refresh cron and IANA timezone (default `*/5 * * * *` in UTC). An invalid value stops the code location from loading |
| `SHOPIFY_ORDERS_REFRESH_WORKERS` | Concurrent readers per refresh (default 4, at least 1). Four keep a refresh inside its tick at about four times the 2026-08-18 peak hour |
| `SNOWFLAKE_*` | Account, user, role, database, warehouse and key path for Snowflake targets |

The destination, state folder and crash recovery come from the shared kit
target (see [Adding a source](#adding-a-source)). dlt keeps the
`shopify_orders_dlt_<environment>_<shop>` working directory under
`<state root>/shopify_orders/<environment>/` and its cursor state in the
destination. Keep the state volume: a load package
left by a killed run waits there until the next run loads it. Remote Compose
mounts the credential file from `SHOPIFY_CREDENTIALS_HOST_PATH`. Local Compose
does the same for the developer's own credential, and writes
`<state root>/shopify_orders/local/shopify_orders.duckdb`, the file local dbt
attaches; see [live local loads](../../deploy/compose/README.md#live-local-loads-adr-0029).
An empty credential file fails the run before any read.

### Line fulfillment evidence
Extraction revision 4 adds nullable source `unfulfilled_quantity`,
`non_fulfillable_quantity`, and `requires_shipping` on `orders__lines`.
New reads require these fields. Old rows stay null until re-observed. The source
preserves these quantities without applying cancellation or refund formulas.
See the [raw contract](../../docs/specs/20261009-shopify-raw-ingestion.md#line-fulfillment-evidence-revision-4)
for query provenance and the required separate re-extraction operation.

### Loading

`shopify/source.py` pages orders 50 at a time, completes each order with
follow-up reads, parses it with the field contract and loads it with dlt `merge`
on `(shop_id, order_id)`. Refresh runs keep a dlt `updated_at` cursor with a
two-minute lag; lookback reads an explicit `[start, end)` window by
`updated_at`, and re-extraction reads one by `created_at`, so choose
re-extraction windows from order creation times, not from `updated_at`. An order that fails the field contract is written to
`shopify_order_rejects` and the rest of the run loads; any other incomplete
order fails the run before anything is written. A reject stays open until a
version at least as new loads. Each refresh first re-reads open rejects by ID
(an `id:` search, 50 per request) in a separate load without the cursor, and
an order that fails again at the same version is not recorded twice. The
`open_rejects` asset check warns while any reject is open and fails once one
has been open for 24 hours.

Launch a window by hand with the `shopify_orders_dlt_load` job:

```yaml
ops:
  raw__shopify__shopify_orders_raw:
    config:
      kind: reextract        # refresh | lookback | reextract
      start: "2026-08-18T16:19:00+00:00"
      end: "2026-08-18T16:29:00+00:00"
      workers: 4             # concurrent time slices
```

The `raw/shopify/shopify_orders_raw` materialization reports orders, rejects,
re-reads, open rejects, pages, requests, throttled requests, requested and
actual query cost, the lowest available throttle points and extraction seconds.
After the orders load succeeds, a second load appends one `ingestion_batches`
row with the covered window, so a failed run leaves no evidence and dbt source
freshness can trust `window_end`.

### Scheduling

With `SHOPIFY_ORDERS_SEED` set, the code location adds three schedules, all
stopped at first:

The reporting time zone is `ANALYTICS_REPORTING_TIME_ZONE` (default
`America/Los_Angeles`), the same calendar dbt and Cube use for order dates.

| Schedule | Cadence | Run |
| --- | --- | --- |
| `shopify_orders_dlt_refresh_schedule` | `SHOPIFY_REFRESH_CRON` | `refresh` from the dlt cursor; the first reads from the seed. Priority 10 |
| `shopify_orders_dlt_lookback_schedule` | 00:15 reporting time zone | `lookback` over the last 26 hours, cursor untouched, for orders Shopify indexed after the two-minute lag. Priority -5 |
| `shopify_orders_reconcile_counts_schedule` | Sunday 07:00 reporting time zone | Count reconciliation, below. Priority -5 |

Each run splits its window into `SHOPIFY_ORDERS_REFRESH_WORKERS` time slices
read concurrently. Readers hand each page to dlt as they read it, so a run's
memory does not grow with its window; when an order is read twice, the merge
keeps the newest `updated_at`. A failure anywhere discards the run's extract
and its cursor. Each reader may read up to 1 GiB of source data in one hour of
active time and 5,000 requests, enough for a month at eight readers. A refresh
tick skips while any `shopify_orders_dlt_load` run is queued or running;
lookback and reconcile queue instead, since they run once. Both jobs carry the
`analytics/exclusive-resource=shopify-orders` tag, so Dagster runs one at a
time.

The [transformation-owned freshness check](../transformations/README.md#raw-shopify-validation)
runs `dbt source freshness` on the latest complete refresh window; refresh
success alone is not a freshness verdict.

### Count reconciliation

`shopify_orders_reconcile_counts` compares Shopify's exact `ordersCount` per UTC
creation month with raw orders not marked unavailable, up to the latest complete
refresh's `window_end`. Only months that differ read their raw orders directly
by ID, 250 per request, so a month raw barely covers costs almost nothing. A raw
order that two direct reads miss is marked `unavailable` in
`order_source_status`, and a marked order that a read returns is marked
`available`; raw rows are never removed. More than 500 candidates marks nothing
and fails the run, since that suggests lost access rather than deletions.
Shopify's count minus the raw orders it still returns is the gap a `reextract`
must fill. The `counts_match` check fails while any gap or unconfirmed absence
remains. A deletion and a gap in the same month cancel in the counts, so
neither is found until one of them changes. It takes no run config, so it can
also be launched by hand.

### Recovery

A run killed mid-load leaves a dlt load package on the ingestion volume. The
next run loads it before extracting its own window, so the retry still records
evidence for that window. Dagster run monitoring fails a run whose container
dies and retries it once. A package that can never load (for example after a
destination table was dropped) blocks every run until it is discarded with
`pipeline.drop_pending_packages()` from the ingestion container.

The runtime reserves `/var/lib/analytics/ingestion` for pipeline working state.
Each future source must declare pipeline identity, destination credentials,
checkpoint ownership, and recovery behavior explicitly. Remote containers
receive Snowflake configuration and a read-only key mount; source code must
parse those values into the destination's credential contract.

See [ingestion](../../docs/adrs/0017-ingest-data-with-dlt.md) and
[code-location isolation](../../docs/adrs/0003-isolate-dagster-code-locations.md).

## Shopify customers
`analytics_ingestion.shopify` also loads raw Shopify customer profiles into an
independent dlt pipeline, dataset and state. It never requires order scopes:
the credential needs `read_customers` and, for names, email, phone, addresses
and notes, Shopify's protected customer data approval. The source does not
calculate customer behavior. Shopify-provided summaries are kept as read.
Freshness thresholds and verdicts belong to transformations; this package only
reports coverage and access evidence.

| Module | Responsibility |
| --- | --- |
| `customer_model` | Table and column declarations |
| `customer_queries` | Pinned 2026-10 selections per field group and the query version |
| `customer_parse` | Source customer to root row and child lists; nulls for unselected groups |
| `customer_source` | Probes, paged reads, child completion, rejects, evidence |
| `customer_reconcile` | Deletion confirmation after a full sweep, personal-data purge |
| `customer_settings` | Customer dataset and credential selection |
| `defs/shopify_customers.py` | Asset, reconcile asset, two jobs, three schedules (stopped by default) |

### Customer configuration

| Variable | Meaning |
| --- | --- |
| `SHOPIFY_CUSTOMERS_DATASET` | Raw schema (default `SHOPIFY_CUSTOMERS_RAW`) |
| `SHOPIFY_CUSTOMERS_CREDENTIALS_PATH` | Optional credential limited to customer scopes; default is `SHOPIFY_CREDENTIALS_PATH` |
| `SHOPIFY_CUSTOMERS_SEED` | First refresh start (ISO instant with offset). Unset means no schedules |
| `SHOPIFY_CUSTOMERS_REFRESH_CRON` | Refresh cron (default `*/15 * * * *`, timezone `SHOPIFY_REFRESH_TIMEZONE`) |

Customers are their own kit source, `shopify_customers`. Local runs load
`<state root>/shopify_customers/local/shopify_customers.duckdb`, which local dbt
does not attach yet; development and production use the same Snowflake database
as orders. The pipeline is `shopify_customers_dlt_<environment>_<shop>` with its
working state in `<state root>/shopify_customers/<environment>/`. The customer
jobs carry `analytics/exclusive-resource=shopify-customers`.

### Raw tables

All tables are in the customer dataset. Child rows carry `shop_id` and
`customer_id`. `tests/fixtures/shopify_customer_raw_schema.json` lists every
column with its type and nullability, and a test keeps it equal to
`customer_model`.

| Table | Grain and content |
| --- | --- |
| `customers` | `(shop_id, customer_id)`: identity, names, locale, account state, consent for email, SMS and WhatsApp, tax, note, default address ID, Shopify summaries, `tax_id`, `_batch_id`, `_observed_at`, `_extraction_kind`, `_query_version`, `_extraction_revision`, `_api_version` |
| `customers__tax_exemptions` | One row per exemption code, with `position` |
| `customers__addresses` | Every `addressesV2` address; `is_default_address` compares with `defaultAddress.id` |
| `customers__tags` | Original tag values, with `position` |
| `customers__metafields` | Approved metafields with ID, namespace, key, type, value and timestamps |
| `customers__company_contacts` | One row per `(company_contact_id, company_location_id)`; `company_location_id` is `''` when the contact has no role assignment |
| `customer_batch_evidence` | One row per successful run: window, `is_full_sweep`, counts, versions |
| `customer_field_access` | Access status per field group per run |
| `customer_metafield_inventory` | Namespace, key and type counts per run, with `collected`; no values |
| `customer_source_status` | `available`, `unknown` or `confirmed_unavailable` per customer, no personal data |
| `shopify_customer_rejects` | Customers that broke the contract |

Restricted personal data: names, `customer_note`, email, phone, address
columns, coordinates, tags, metafield values and `tax_id`. Tags, notes and
metafield values start restricted because they can hold unexpected personal
data. Shopify's `state` is meaningful for Classic Customer Accounts, and
`verifiedEmail` can default to true for customers created through the admin or
API. A missing email, phone, SMS or WhatsApp object leaves every column of that
channel NULL; it never becomes an unsubscribed state.

Each child table is created empty after the first run, so a store with no
addresses, metafields or company contacts still has every table. Every merge
replaces a customer's children as a whole: a removed child disappears and an
empty collection leaves no rows.

### Field groups and access

Every run first checks the shop identity and the `read_customers` scope, then
probes each group with a one-customer query. Only `available` groups are
selected, so a denied group never fails the run. Columns of an unselected
group are NULL, and `customer_field_access` is the only way to tell that from a
source NULL. Statuses: `available`, `access_denied`, `unsupported` (the field
is not valid for the pinned API version) and `excluded`.

| Group | Fields |
| --- | --- |
| `profile` (required) | IDs, timestamps, locale, state, email verification, subscriber status, data sale opt-out, tax exemption, default address ID |
| `names` | `first_name`, `last_name`, `display_name`, `customer_note` |
| `email` | `defaultEmailAddress` address, format validity, consent, tracking level |
| `phone` | `defaultPhoneNumber.phoneNumber` and SMS consent |
| `whatsapp_consent` | WhatsApp consent |
| `addresses`, `tags`, `metafields` | Complete connections, paginated to the last page |
| `summaries` | `numberOfOrders`, `amountSpent`, `lastOrder.id` |
| `statistics` | `predictedSpendTier`, `rfmGroup`, where available |
| `tax_id` | Conditional: `taxSettings.taxId`, only after a reporting purpose is approved and `SHOPIFY_CUSTOMERS_TAX_ID_ENABLED=true`; otherwise `excluded` |
| `company_contacts` | Conditional B2B: contact, company and role-assignment locations |
| `external_identities` | `excluded`: Multipass and identity-provider identifiers have no demonstrated linking need |

Metafield values are excluded by default. Set
`SHOPIFY_CUSTOMERS_APPROVED_METAFIELDS='[["custom","loyalty_tier"]]'` only after
approval of each exact namespace/key pair and its reporting purpose. The inventory
records accessible namespace/key/type counts even when values are excluded.
Namespaces starting with `secret`, `$app:`, `multipass`, `password` or `token`
remain excluded even when listed. Wildcards are rejected. After an approval
change, run a complete sweep before publication to replace existing child rows.
The `field_group_access` asset check warns while any group is denied or
unsupported. Before enabling the source, run a read-only probe that verifies
`read_customers` and every enabled group, and check that a 50-customer query's
requested cost stays below Shopify's 1,000-point request limit.

### Customer loading

| Kind | Reads | Cursor |
| --- | --- | --- |
| `refresh` | `updated_at` from the dlt cursor, with a two-minute lag; the first run starts at the seed | dlt state |
| `lookback` | An explicit `[start, end)` by `updated_at` (manual repair) | none |
| `reextract` | An explicit `[start, end)` by `updated_at` | none |
| `sweep` | Every customer updated before `end`, from the epoch | none |

A refresh reads 50 customers a page and follows addresses, metafields and role
assignments to their last page before it keeps a customer. Each search stops at
25,000 roots and continues from the last timestamp second. It rereads that
second to avoid truncation or duplicate output. A second with 25,000 customers
cannot be split safely and fails the run. A cursor that does
not advance, a missing `hasNextPage` or a request, byte or time limit fails the
run, and dlt discards the extract, so a failed run loads nothing and keeps the
cursor. Completed dlt job files are deleted after loading. The next load or
reconcile also removes legacy completed job files in this pipeline. Failed and
pending packages remain recoverable; reconciliation fails until recovery finishes.
A package left by a killed run loads first on the next run. A customer
that breaks the field contract goes to `shopify_customer_rejects` with an error
that names the field and never the value. The next refresh re-reads open rejects
by ID. The `open_rejects` check warns for a day and then fails.

Shopify may not advance `updatedAt` when the order count, spending or
subscription status change. The nightly sweep refreshes those summaries, so a
summary is as fresh as the last complete sweep, not the last refresh. After
each successful load one row in `customer_batch_evidence` records the covered
`[window_start, window_end)`, so an empty window still proves coverage, and
`is_full_sweep` tells a sweep from a window.

Launch a run by hand with the `shopify_customers_dlt_load` job:

```yaml
ops:
  raw__shopify__shopify_customers_raw:
    config:
      kind: sweep            # refresh | lookback | reextract | sweep
      start: "2026-10-01T00:00:00+00:00"
      end: "2026-10-02T00:00:00+00:00"
```

| Schedule | Cadence | Run |
| --- | --- | --- |
| `shopify_customers_dlt_refresh_schedule` | `SHOPIFY_CUSTOMERS_REFRESH_CRON` | `refresh`; skips while a customer run is active |
| `shopify_customers_dlt_sweep_schedule` | 01:30 reporting time zone | `sweep` |
| `shopify_customers_reconcile_schedule` | 04:30 reporting time zone | Deletion confirmation |

The nightly full sweep also covers the former 26-hour lookback population, so
customers have no scheduled lookback. Use manual `reextract` or `lookback` windows
for repair; neither advances the refresh cursor. Order schedules are unchanged.

The load job uses one exclusive slot for sweeps and refreshes. Before enabling
schedules, measure a full-store sweep and refresh, including scheduler wait.
Check the sweep's blocking time against the transformation-owned profile freshness
budget in the [activation procedure](../transformations/README.md#customer-activation).
A bounded single-page cost probe does not prove full-sweep throughput. Schedules
remain stopped until the activation checks pass. On an existing deployment,
stop `shopify_customers_dlt_lookback_schedule` and let its queued or active runs
finish before deploying this removal.

### Deletion and unavailability

Shopify has no deletion feed for customers. `shopify_customers_reconcile` uses
the newest complete full traversal. Candidates are raw customers that sweep
did not load, plus unresolved rejects, including customers never loaded.
Destination SQL finds these IDs without loading the customer population into
memory. A profile parse rejection does not imply a traversal gap. It still
fails freshness, but cannot block deletion of unrelated customers.
Access denial, a partial traversal, a limit or one missing read never
confirm a deletion:

1. No complete sweep means no marks.
2. More than 500 candidates mark nothing and fail the run by default.
   For a reviewed bulk deletion, set `max_candidates` on the
   `raw__shopify__shopify_customer_source_status` op in
   `shopify_customers_reconcile` run config. The override accepts 1 through 5000.
3. Shop identity and scope are checked again, then each candidate is read by ID
   twice. A candidate a read returns is marked `unknown` and kept.
4. A candidate that is null in both reads is `confirmed_unavailable`: its root
   and child rows are deleted and only its ID and status remain.
5. A confirmed customer that a later sweep reads is marked `available`.

Deleting a customer purges its profile and child personal data from raw.
This operation does not redact the contact values recorded on historical orders.
Those values require separate retention and redaction rules before rollout;
retaining order facts does not authorize indefinite contact-data retention.
A customer-redaction request that removes the source customer follows the same
path: the next sweep and reconcile purge it. With successful daily sweeps and
reconciliation, raw can hold a stale profile for up to a day. A failed sweep or
reconciliation extends that interval.

Coverage evidence counts unresolved rejects even when duplicate reject events
are suppressed. A customer that disappears during child pagination makes the
coverage `partial`; transformation freshness checks require `complete` coverage.
Tax IDs remain excluded until a reporting purpose is approved and
`SHOPIFY_CUSTOMERS_TAX_ID_ENABLED=true` is set explicitly.

Confirmed-unavailable purges delete children before the root in one transaction
per batch. An interrupted purge retains the root as a retry candidate, even
when its reject is closed and the latest sweep loaded that root. Reconciliation
reads its persisted unavailable status and repeats the direct absence checks.
Only literal null direct reads count as absence; malformed reads fail the run.
Reconciliation uses complete full-traversal evidence, including traversals with
profile rejects. Each candidate still requires two direct null reads, with shop
and scope revalidation between them. A confirmed-unavailable status closes older
rejects; a later rejection reopens them. The combined loaded, rejected and
interrupted-purge candidate set remains subject to the deletion limit.

## Loop Returns headers
`analytics_ingestion.loop_returns` provides the manual
`loop_returns_dlt_snapshot` job. Definition discovery registers it automatically.
It has no schedule. The job selects a full-history header snapshot in six-hour
creation-time windows. Each completed window commits to an unpublished candidate
dataset using dlt merge and native source state. Only a complete, validated
candidate replaces the maintained snapshot.
It does not ingest customer, address, tracking, or return-line fields.

The default dataset is **`LOOP_RETURN_HEADERS_RAW`**. Keep it separate from any
wider returns table: a header-only replacement must not replace that table. The `returns` table
holds shop identity, return and Shopify order IDs, state, timestamps, currency,
and six exact-decimal amounts. `return_snapshots` holds load evidence. The source
uses dlt replacement with `insert-from-staging`. It rejects conflicting duplicate
IDs, a new empty snapshot after a nonempty snapshot, and removals above the
configured count limit. The guard reads destination rows, not a second ID
inventory in dlt state. A destination with another shop is rejected. A local
advisory lock also prevents concurrent runs that share the state root.
The job retains its `loop-returns` exclusive-resource tag and also shares
`analytics/publication-input=development-marts` with order-group publication. The second
queue limit prevents a snapshot replacement between fact builds and availability
capture. Deploy both code locations and the coordinator, then drain older
untagged runs before relying on this isolation.

### Recovery, progress and limits

The candidate uses `<LOOP_RETURNS_DATASET>_candidate` in the same destination and
its own `loop_returns_dlt_<environment>_<shop>_candidate` pipeline in the kit
state folder, `<state root>/loop_returns/<environment>/`. It contains only the
same parsed header columns, never raw customer payloads. No dbt source or Cube
model reads the candidate. Completed window rows and their native dlt checkpoint
survive a killed step; pending packages finish before destination synchronization
or another extraction. A rerun resumes the original frozen cutoff, not a new
cutoff silently combined with earlier windows. Changed shop, target, history
bounds or window size are rejected while work or publication acknowledgement is
unfinished, except that an explicit `restart_candidate` may correct history bounds
while keeping the same shop, destination, dataset and window size. Pending packages
finish against that unchanged target before rebuilding. A restart never redirects
pending work to a different destination. After publication and acknowledgement
complete, a new scope can start.

Progress logs report the snapshot ID, phase, current date bounds, completed/total
windows, committed row count and elapsed seconds. Request counts cover completed
windows, not discarded attempts. The Dagster step has a five-minute process
watchdog, including loading; the job also carries Dagster's runtime-limit tag.
A blocked worker is killed rather than allowed to run indefinitely. Dagster may
take additional time to record the failure and clean up its run container.
There is no automatic retry: inspect progress and explicitly rerun the same job.
The unfinished window may be fetched again; completed windows are not fetched
again. Retain durable dlt state and pending packages when investigating a failure.

After every window commits, SQL checks candidate counts/identity and compares
removals with the actual maintained table. Promotion streams the validated rows
through the existing `insert-from-staging` replacement rather than collecting the
entire history in Python memory. Successful publication is acknowledged in native
dlt state. A retry after publication but before that acknowledgement finishes it
without extracting the day again. The candidate is retained until the next
snapshot's first committed window replaces it. This is not point-in-time vendor
snapshot isolation or proof of a business freshness target.

A candidate rejected for an empty result, excessive removals, or conflicting
identities needs a fresh source read after the upstream problem is resolved. Set
`ops.raw__loop_returns__loop_returns_raw.config.restart_candidate: true` for that
manual run (or `ops.loop_returns_recovery_trial.config.restart_candidate: true`
for a trial). This finishes pending dlt packages first, then starts a new candidate
from the configured history start. The replacement still passes every publication
guard; rejected candidate data never replaces the published table. Do not delete
state or increase the removal allowance merely to bypass a rejected candidate.
Leave this option false on ordinary interruption retries to preserve progress.

### Isolated one-day development trial

`loop_returns_dlt_recovery_trial` runs the same maintained code for one completed
UTC day, supplied as `ops.loop_returns_recovery_trial.config.day` (`YYYY-MM-DD`).
It refuses production and future days. It uses
`<LOOP_RETURNS_DATASET>_trial_YYYYMMDD` plus that dataset's `_candidate` schema and
separate `loop_recovery_trial/YYYYMMDD` working state inside the Loop state folder. It never materializes the
maintained Loop asset or changes its dataset, and has no schedule. It retains the
same exclusive/input-lock tags and five-minute worker limit as the normal job.

To verify recovery after release, launch the trial through development Dagster,
wait for a `window_committed` log, cancel that run, and verify it has stopped.
Launch the same day again. Verify the `resumed` count, no repeat of committed
windows, unique identities, all selected header fields against a fresh bounded
API read, and the absence of any maintained-data change. Do not launch the
full-history job or expand the range as part of this trial. DuckDB recovery checks
pass locally. A trial validates only its one day, not historical throughput or
reporting freshness.

Required configuration:

- `ANALYTICS_ENVIRONMENT`: explicit `local`, `development`, or `production`; `ci`
  is refused. Local runs load `<state root>/loop_returns/local/loop_returns.duckdb`.
- `LOOP_RETURNS_CREDENTIALS_PATH`: JSON file with `api_key`; never commit it.
- `LOOP_RETURNS_SHOPIFY_SHOP_ID`: Shopify Shop GID for this source.
- `ANALYTICS_INGESTION_STATE_ROOT`: absolute durable state root shared by every
  source; defaults to `/var/lib/analytics/ingestion`.
- `LOOP_RETURNS_DATASET`: separate header dataset; default as above.
- `LOOP_RETURNS_HISTORY_START`: offset-aware whole-second timestamp; default
  `2024-06-01T00:00:00+00:00`.
- `LOOP_RETURNS_MAX_REMOVED_ROWS`: maximum removed IDs per snapshot; default 0.
  Review confirmed upstream deletions before setting a positive allowance.
- Development and production also require explicit `SNOWFLAKE_ACCOUNT`,
  `SNOWFLAKE_USER`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_DATABASE`,
  `SNOWFLAKE_WAREHOUSE`, and `SNOWFLAKE_PRIVATE_KEY_PATH`.

A bounded read-only sample should match the default unpaged response by return
ID before the first full run. A sample does not prove shop identity, historical
coverage, or currency basis for all returns. The client requests paginated responses with
750 rows per page. It follows only validated same-origin page URLs and rejects
missing pagination markers, cycles, and out-of-window rows. One narrow exception
handles the live API's bare `[]` response for an empty initial window. Nonempty bare arrays and bare empty continuation
pages still fail rather than certify a partial result. Snapshot removal guards
still apply, so empty windows cannot silently erase an earlier loaded snapshot.
Windows that exceed
the 100-page bound split; the minimum window is one hour. These checks follow
vendor documentation, offline fixtures, and the bounded live sample above.
Confirm the response shape, next-page date behavior, history boundary, shop
identity, and full-history request cost before the first real run. No deployment,
schedule, historical load, or production use accompanies this implementation.

Loop header runs stay opt-in and manual; deploying the wiring does not load or
schedule a snapshot.

## Adding a source
Follow [ADR 0017](../../docs/adrs/0017-ingest-data-with-dlt.md)'s order: a
verified dlt connector, then dlt's declarative REST API source, then custom code.
The shared kit in `analytics_ingestion.kit` supplies everything else. GoodDay,
Google Sheets and Impact Radius use all of it. Shopify orders, Shopify customers
and Loop Returns use its target and recovery, and keep their own credential
files and on/off settings. A new source named `<source>` needs:

- A source module that reads its settings with `kit.runtime.read_settings`,
  picks its destination with `kit.runtime.target`, and runs a dlt pipeline from
  `Target.pipeline`. Snapshot sources can use `kit.capture.capture`, which
  appends rows plus one evidence row per capture, and `kit.capture.recover`.
- A `defs/<source>.py` that calls `kit.dagster.manual_capture`. It builds one
  asset under `raw/<source>/`, one manual job with no schedule, the
  `analytics/exclusive-resource` tag, no automatic retry, and a run time limit.
- Offline fixture tests. CI never reads a live source.

No deployment file changes. These conventions replace per-source settings:

| Convention | Value |
| --- | --- |
| On/off | List `<source>` in `ANALYTICS_SOURCES`, comma-separated. Unlisted sources register no job |
| Settings | One JSON object, `<source>.json`, in `ANALYTICS_SOURCE_SECRETS_ROOT` (default `/run/secrets/sources`). It holds credentials and plain settings. Errors name the file, never its contents |
| State | `<ANALYTICS_INGESTION_STATE_ROOT>/<source>/<environment>/` (default root `/var/lib/analytics/ingestion`) |
| Local raw file | `<source>.duckdb` in that state folder, the file local dbt attaches |
| Remote destination | Snowflake from the `SNOWFLAKE_*` settings, unless the source is local-only |
| Environment | `ANALYTICS_ENVIRONMENT` must be `local`, `development` or `production`; CI is refused |

Activation checks that the settings file, and remotely the Snowflake key, is
readable. It makes no network calls. A listed source with a missing file stops
the code location from loading. Only the local stack mounts the settings folder
today; remote kit sources need their own secret delivery first.

## Impact Radius actions
`impact_radius.source.load_actions(env)` reads the Impact v14 `Actions` list
with dlt's declarative REST API source. It is local-only and has no dbt models
yet. With `impact_radius` in `ANALYTICS_SOURCES`, the code location registers
the manual `impact_radius_actions_load` job, which run monitoring stops after
900 seconds.

- **Window.** Each run reads actions whose action date falls in the last three
  days. Impact limits one date filter to 30 days.
- **Pages.** Pages are numbered from 1 up to the response's `@numpages`, with
  20,000 actions per page.
- **Updates.** Rows merge on `Id`, so a rerun updates the actions it returns.
  An older action that changes upstream is not revisited, and nothing is
  deleted. A gap of more than three days between runs is not backfilled, so
  the table is not a complete or current ledger of every action.
- **Requests.** dlt's default HTTP session retries rate limits and server
  errors and gives each request a timeout.
- **Fields.** Only the documented fields in `impact_radius.source.FIELDS` are
  kept, all as source text. Customer contact and network fields are dropped
  before loading.
- **Recovery.** A failed load keeps its package, and the next run loads it
  before reading again.

`impact_radius.json` holds `account_sid`, `auth_token` and `campaign_id`, the
numeric program ID as a string. Impact requires the program; find its ID in
the Impact dashboard. The run loads
`raw_impact_radius.actions` in
`<state root>/impact_radius/local/impact_radius.duckdb`.

## GoodDay inventory
`analytics_ingestion.goodday` reads the vendor's BigQuery `inventory_by_item`
view with dlt's built-in `sql_database` source, through the `sqlalchemy-bigquery`
driver, and loads it with dlt. dlt reflects the view's column types and streams
the query result in chunks of 1,000 rows.
Snowflake is the remote destination. The `local` target loads
`<state root>/goodday/local/goodday.duckdb`, the file local dbt attaches
([ADR 0029](../../docs/adrs/0029-load-every-source-into-local-duckdb.md)); tests
use a SQLite stand-in for the view and DuckDB for the destination. No separate GCS export service, reference-table loads, transformations,
Shopify joins, or schedules are introduced.

### Raw capture contract

- `inventory_by_item` retains source-shaped observations. `_capture_id` identifies
  the extraction attempt and `_row_number` identifies its row, starting at one.
  Source nulls, quantities, labels and additional fields are preserved. Nested
  fields stay JSON instead of becoming independently loaded child tables.
- `inventory_captures` records `capture_id`, UTC `captured_at` (capture start),
  UTC `observed_at` (source read start), `source_relation` and `row_count`.
  Neither timestamp proves vendor data freshness. The dlt load ID
  (`_dlt_load_id`) identifies the load; there is no BigQuery job ID.
- There is no incremental cursor. Each successful query captures complete current
  membership. Removed inventory rows disappear from later captures, not history.
  The legacy business grain is merchant/item/location/channel; uniqueness and
  nullable keys require live verification. Raw ingestion does not deduplicate.
- Column types come from dlt's reflection of the view. Source `NUMERIC` stays
  decimal with its reflected precision and scale. `TIMESTAMP` loads as a
  time-zone-aware instant; `DATETIME` stays naive. `FLOAT` stays approximate raw
  data, **not approved exact money**. Currency and the inventory cost basis are
  unverified; no financial metric is published. Type changes fail; additive
  columns are retained. Known fields get the descriptions in
  `goodday/bigquery.py`; unfamiliar fields are marked uninterpreted until
  assessed.
- dlt normalizes column names to snake_case. Leading `_` is reserved for loader
  metadata, and a source column that starts with `_` fails the capture.
- `row_count` is the number of rows read from the single query. It proves the
  result was read to the end, not that the vendor's view is correct or fresh.
  A zero-row result is a valid empty capture.

**A visible row is not proof of a complete capture.** Rows and evidence share a
load package, but individual table loads can finish at different times. Consumers
must join the evidence to `_dlt_loads` with `status = 0`, then select the latest
eligible capture, even if its count is zero. Join inventory rows by both capture
and load ID. Never select the latest capture from the inventory rows themselves.
For example, replacing `raw_goodday` with the configured schema:

```sql
with latest_capture as (
    select c.*
    from raw_goodday.inventory_captures c
    join raw_goodday._dlt_loads l on l.load_id = c._dlt_load_id
    where l.status = 0
    order by c.captured_at desc, c.capture_id desc
    limit 1
)
select i.*
from raw_goodday.inventory_by_item i
join latest_capture c
  on i._capture_id = c.capture_id and i._dlt_load_id = c._dlt_load_id
```

An empty result from this query can mean either a completed empty capture or no
successful capture yet. Consumers must inspect `latest_capture.row_count` and
presence separately. Freshness and reporting availability are not yet defined.

### Manual execution and configuration

The `goodday_inventory_capture` Dagster job materializes
`raw/goodday/goodday_inventory_raw`. It has no schedule and carries
`analytics/exclusive-resource=goodday-inventory`, so the existing queued run
coordinator serializes GoodDay writers. Direct Python callers must also serialize
access to a shared pipeline and destination. Imports and definition checks make
no network calls. Tests call `capture_inventory(pipeline, snapshot)` with
an isolated DuckDB pipeline, not the live entrypoint.

List `goodday` in `ANALYTICS_SOURCES` to register the job. It uses the
[kit conventions](#adding-a-source); `goodday.json` holds:

| Key | Meaning |
| --- | --- |
| `project`, `dataset` | Explicit vendor source project and merchant dataset; no production fallback |
| `billing_project` | Project charged for query jobs; independent of the source project |
| `location` | BigQuery query location |
| `service_account` | The read-only service-account JSON object; no generic Google credential fallback |

Remote targets also need the `SNOWFLAKE_*` destination settings; local ignores
them. The raw schema is `raw_goodday`. State lives under
`<state root>/goodday/<environment>/<pipeline-name>/`. The pipeline name
includes a digest of environment, source relation, destination (Snowflake
account/database, or the local DuckDB file),
and raw schema, preventing accidental checkpoint reuse across those identities.
Only the local stack mounts kit settings today, so remote GoodDay runs need a
remote settings delivery first.

Each run is one billed BigQuery query with the query cache off. There is no
per-query cost cap or BigQuery job timeout; the Dagster run time limit stops a
run that takes too long. Launching a live run remains an operator action.

### Recovery and acceptance

Before querying BigQuery, the live entrypoint finishes pending dlt normalization
and loading. Recovery reuses the original capture identity and does not query
BigQuery again. A fresh extraction has a fresh capture ID; repeated observations
are history, not duplicate updates. A failed read emits no complete capture. A failed destination job keeps a recoverable package and does
not become eligible through the successful-load filter. Preserve the state
volume and use `kit.capture.recover(pipeline)` for recovery alone. Do not drop
pending packages merely to make a new run proceed. The source rejects dlt loader
configuration with `raise_on_failed_jobs=false` or
`auto_abort_on_terminal_error=true` before recovery or extraction. Those settings
would respectively weaken successful-load evidence or discard retry packages.

Retained captures have no automatic expiry in this slice. They begin at the first
successful capture, not at a historical backfill date. Define retention, approve
source access/cost limits, verify the live schema and reconcile to a named ERP
report before activation. Run fixture parity and recovery checks in development
Snowflake before claiming remote acceptance; ordinary offline tests prove only
the local loading contract. No maximum business data age is promised yet.

### Publication coordination
Every maintained writer, including reconciliation, carries its consistency
group's `analytics/publication-input` tag in addition to its exclusive-resource
tag. Orders and Loop use `development-marts`; customers use
`development-customers`; inventory uses `development-inventory`. Each publisher
shares only its group's limit for the whole run. This serializes contributing
writers with their publisher without making unrelated groups wait. Successful
manual, scheduled and reconciliation runs trigger only their enabled group's
success sensor. Sensors ship stopped; extraction schedules do not change.

New sources apply the shared tag to their own writers and add a source-neutral
publication entry. Map the dbt source to its ingestion asset in the transformation
`CandidateTranslator` for model lineage. They do not modify another source's
module or the coordinator.
See the [publication contract and migration](../transformations/README.md#development-publication)
before deployment. Direct calls outside Dagster must provide equivalent isolation.

## Google Sheets captures

The `google_sheets` source captures one or more spreadsheet tabs as retained
history: every run appends a new observation of each tab instead of replacing
the last one. It suits reference data kept in a sheet, such as a product
catalog. It runs only on the `local` target
([ADR 0029](../../docs/adrs/0029-load-every-source-into-local-duckdb.md)).
Development and production reject it, and CI uses fixtures only.

With `google_sheets` in `ANALYTICS_SOURCES`, the code location registers the
manual `google_sheets_capture` job, which materializes
`raw/google_sheets/google_sheets_raw`. It uses the
[kit conventions](#adding-a-source). It has no schedule, does not retry, and
run monitoring stops it after 600 seconds.

`google_sheets.json` holds a service-account credential, used only with the
`spreadsheets.readonly` scope, and the tabs to capture:

```json
{
  "service_account": {"type": "service_account", "...": "..."},
  "sheets": [
    {
      "table": "product_catalog",
      "spreadsheet_id": "<spreadsheet ID from the sheet URL>",
      "sheet_id": 0,
      "columns": {"SKU": "sku", "Product Name": "product_name"},
      "text_columns": ["sku"]
    }
  ]
}
```

- `table` names the raw tables, `<table>_rows` and `<table>_captures`.
- `sheet_id` is the tab's numeric ID (the `gid` in its URL), so renaming the
  tab does not change what is read.
- `columns` maps each exact source header to its loaded column name. Headers
  match exactly, including spacing and case. Unlisted columns are never read.
- `text_columns` lists columns whose cells must hold text, never a number
  coerced to text, such as SKUs.

Each tab loads through its own dlt pipeline into the `raw_google_sheets`
dataset of `<state root>/google_sheets/local/google_sheets.duckdb`, the file
local dbt attaches, so one tab's pending package never blocks another. Reads
for one tab stop at a 120-second wall limit, on top of each response's
40-second budget.

### Extraction and upstream compatibility

The verified Google Sheets source was inspected at revision
[`5c851c48a5601d82c63419ba10aa19b74b22fbf0`](https://github.com/dlt-hub/verified-sources/tree/5c851c48a5601d82c63419ba10aa19b74b22fbf0/sources/google_sheets).
Its `process_range` skips blank rows without retaining their physical row number;
separately processing selected noncontiguous columns cannot safely reconstruct
membership across gaps. It also samples types, supplies fallback headers, skips
empty ranges and defaults to replacement, rather than retained captures. These
behaviors do not satisfy this source's aligned-row and complete-empty contracts.
No upstream connector code is vendored or installed.

The small adapter uses Google's supported
[`spreadsheets.get`](https://developers.google.com/workspace/sheets/api/reference/rest/v4/spreadsheets/get)
and [GridData offsets](https://developers.google.com/workspace/sheets/api/reference/rest/v4/spreadsheets/sheets#GridData),
then native dlt 1.31.0 loading. `google_reader(credentials)` requires explicitly
supplied Google credentials scoped **only** to `spreadsheets.readonly`. It creates
an `AuthorizedSession` with source-request refresh retries disabled; it never
chooses an identity, reads credentials or changes scopes. The GET endpoint supports
this read-only scope; the former POST `getByDataFilter` endpoint does not document
support for it. No broader grant is needed or authorized. Requests 2.34.2 remains
the existing direct dependency. The local entrypoint above is the only runtime wiring.
Before relying on live captures, verify that adjacent ranges retain separate GridData
blocks and check empty-formula response
shapes. A cell with `effectiveValue.stringValue` but no `formattedValue` currently
fails safely; do not assume fixture success proves that source representation.

- The spreadsheet and numeric tab identity come from the settings file.
  At most three spreadsheet GETs occur per tab capture: metadata only resolves the
  numeric ID to its current title; a bounded first-row range resolves exact headers;
  one batch reads only the configured single-column ranges, including headers.
  Titles are always quoted, apostrophes are escaped, and Requests-compatible query
  encoding preserves repeated ranges. This avoids named-range ambiguity. Each
  cell response must retain the same title, ID and bounded grid. Rename or header
  drift aborts the capture. Other-tab metadata and unselected header names are
  inspected but never retained or logged; other-tab cells and excluded data
  columns are never requested.
- Each range keeps its physical start position. Blank interior cells and independently
  omitted trailing blanks do not shift other columns. Duplicate/missing headers,
  unexpected offsets, absent/duplicate ranges and changed grid bounds fail before
  extraction reaches dlt. Headers are rechecked in the data response. Formula
  errors such as `#N/A` in data cells persist in the source sheet, so they are
  captured as blank cells rather than failing the capture; an error in a
  configured header still fails it.
- The reviewed bounds are 20,000 grid rows and 200 columns; larger grids fail rather
  than truncate. HTTP calls have 10/30-second connect/read timeouts, reject
  redirects and non-200 responses, and stream at most 16 MB before JSON parsing.
  Streaming checks a 40-second per-response elapsed budget. A blocked socket may
  return only at its read timeout. The local entrypoint adds the 120-second wall
  limit across a capture's reads, and Dagster stops the run after 600 seconds.
  Remote callers must enforce equivalent limits. There is no custom retry loop,
  including no automatic spreadsheet retry after a 401 response.
- Selected source strings are retained without display formatting, trimming or
  case changes. Other cells retain display text without date interpretation. Empty cells are nullable. Columns in `text_columns` must
  have a string-valued source cell, never numeric coercion. Duplicate or blank
  keys in populated rows remain raw evidence; downstream business-key checks
  decide publication validity.
  Entirely empty selected rows are not captured. Excluded-only rows do not create
  membership. Formula results are observations, not source formula history.

### Raw history, completeness and recovery

`<table>_rows` has one row per populated physical row per observation, with
every configured column as text, `_sheet_row`, `_capture_id` and native dlt metadata.
The raw key is observation plus physical row, not a business key. `<table>_captures`
records immutable capture identity, spreadsheet/tab identity, `started_at`,
`observed_at` and `row_count`. Both relations and every source/metadata column have
dlt schema descriptions. Schema drift fails rather than coercing selected types.
These raw fields are not approved Cube exposures; privacy classification and
business models remain transformation and consumer work.

`observed_at` is the UTC instant after the data response, never planned midnight
or an introduction/exit date. Reads are not transactionally isolated from edits;
read bounds and header/grid checks do not promise a transactional sheet snapshot.
Each fresh extraction appends a new identity, even if unchanged. Captures retain
past values and absence membership without expiring earlier observations. A
validated header-only result creates both relation schemas and explicit zero-row
evidence. Partial/failed reads never establish a new observation.

**Visible evidence is not a completed load.** Require `<table>_captures` to
join `_dlt_loads` by `_dlt_load_id = load_id` with `status = 0`. Join rows to that
capture by **both** `_capture_id = capture_id` and matching `_dlt_load_id`.
A pending table can be visible while another table fails. Select capture evidence,
including zero-row captures, rather than inferring membership from the latest row.
Business validation still follows; successful raw loading does not validate keys.

`capture_sheet` first recovers pending packages, then reads a **new** capture.
For recovery alone, use `kit.capture.recover(pipeline)`: this is the shared
kit recovery function GoodDay also uses, which operates on native dlt packages
and rejects unsafe loader settings. Recovery preserves the captured identity and
observation time without rereading Sheets, including failures in either rows or
evidence loading. Do not discard pending packages or state to clear failures.
Callers must serialize access and keep pipeline/state/destination identities
explicit and isolated by environment. Only the local entrypoint above has runtime wiring
and source access, so fixture success is not remote or live acceptance.
