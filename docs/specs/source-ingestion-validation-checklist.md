# Source ingestion validation checklist

Status: Reusable checklist for source spikes and maintained ingestion

## How to use

Apply this checklist together with a source-specific checklist. Keep these template
boxes unchecked; record source results in the source checklist's evidence log using
the IDs below. A result is **passed**, **failed**, **blocked**, or **not applicable**
with a reason. An unrecorded ID is pending. A pass needs an observable result and
a test or run reference, target, and date. Do not equate a passing unit test with a
live source or Snowflake verification.

Choose the smallest test that establishes the behavior. Reuse a test across related
IDs when it provides the evidence; this is not a requirement for one test per box.
Use synthetic fixtures for faults and mutations, and an explicitly selected
development target for live checks. Validate production permissions through grants
and policy inspection, without attempting writes to production.

The owning spec defines what a spike covers. Mark production-scale checks deferred
with a reason and next evidence needed; a small spike can answer its question while
leaving rollout checks pending. Follow [engineering standards](../../engineering-standards.md)
and [ADR 0017](../adrs/0017-ingest-data-with-dlt.md).

## Freshness contract and scenario evidence

This section validates the platform around ingestion. Ingestion owns extraction
windows, cursor/version ordering, completeness, progress and recovery, and emits
the timing and coverage evidence. Dagster owns scheduling and execution deadlines.
Transformations/dbt owns freshness expectations and warning/failure checks,
including contracts on raw sources; Dagster runs checks and acts on their results.
Consumer-specific serving checks extend that contract. C05 and R05 are platform
acceptance checks, not source/loader business rules. Keep technical batch completion
independent of the freshness verdict. Follow the repository-wide
[freshness ownership standard](../../engineering-standards.md#freshness-ownership).

Use [maximum data age](../../GLOSSARY.md) consistently: a 30-minute requirement
means that at any query time, in-scope changes older than 30 minutes are reflected
in the consumer's dataset. Old unchanged records remain valid. This is an end-to-end
requirement, including source visibility delay, polling, queueing, extraction,
loading, transformations, publication, and consumer cache refresh where applicable.

Measure age from the source coverage cutoff established for that publication.
The newest record timestamp and successful job completion alone do not establish
coverage. For a dataset combining required inputs, use the oldest applicable
coverage cutoff; a fresh input cannot hide a stale dependency. Empty successful
polls may establish newer coverage when the source contract supports that conclusion.
Unknown coverage must be reported as unknown, rather than assigned a fresh timestamp.
Document completeness limitations and separately measure accuracy discrepancies.

For each new pipeline or independently refreshed resource group, document:

- The consumer and relation/API where data must be queryable.
- How source change times and coverage are established. If an API only exposes
  availability time, record that measurement limitation without silently weakening
  the end-to-end promise. State completed-reporting-period deadlines separately.
- Maximum acceptable age or completion deadline, reporting timezone, polling cadence,
  normal runtime target, maximum acceptable routine runtime, and reserved headroom.
- Expected normal/peak volumes, overlap size, dependency/queue delays, and the time
  allowed to recover a missed run. Explain behavior when the promise cannot be met.
- How repeated runs will establish consistency: measurement period, run count,
  median, p95 where the sample supports it, maximum, deadline misses, and backlog.
  Include failures and retries rather than reporting only successful-run averages.

Target roughly **30–40% time headroom** as a starting design preference, with the
actual budget stated explicitly. For a 30-minute refresh cycle, routine processing
should consistently finish within **15–20 minutes**, leaving 10–15 minutes for
variation. The example allows more headroom than the baseline preference. Measure
through the consumer's queryable result, including required transformations and
publication; raw loading alone does not establish freshness.

A 30-minute polling schedule plus 20-minute processing can leave a change waiting
nearly 50 minutes. If the promise is a maximum 30-minute source-to-queryable lag,
polling wait, source visibility, queueing, processing, and publication must fit
inside that total. Schedule frequency and runtime headroom must satisfy both the
freshness promise and sustainable throughput without an accumulating backlog.

Run concrete scenarios and record starting state, event/fault, expected result,
time/request budget, and observed result. At minimum exercise:

| Scenario | What to demonstrate |
| --- | --- |
| First load | Bootstrap completes within its separate budget and hands off to routine state |
| Normal cycle, including no changes | Meets the routine budget without unnecessary full rereads |
| Peak expected volume | Meets the stated headroom target or identifies a measured capacity limit |
| Source changes during reading | Overlap recovers the change under the declared consistency contract |
| Throttling or slow source | Shared retries remain bounded; freshness impact is visible |
| Storage/load/transformation failure | Completed upstream work is reused and recovery cost is measured |
| Missed cycle or restart | Catch-up has an explicit bound and does not create a growing backlog |
| Backfill/reconciliation alongside routine work | Historical work cannot silently starve frequent data |
| Competing developer/CI or warehouse work | Isolation holds and queue/dependency delays enter the timing result |
| Reporting-day close | Required dependencies publish before the business deadline with recorded margin |

Use applicable scenarios; record why a scenario is out of scope. Synthetic faults
establish mechanisms, while representative repeated runs establish capacity.

## Contract and access

- [ ] **C01 — Scope and grain:** List resources, selected fields, stable keys,
  parent-child relationships, and intentional exclusions; exercise duplicate keys.
- [ ] **C02 — Access:** A bounded read succeeds; required permissions and accessible
  history are verified. Missing permissions produce a clear failure, not empty success.
- [ ] **C03 — Configuration:** Invalid targets, credentials, versions, and required
  settings fail clearly. Fixture tests require no live credentials.
- [ ] **C04 — Secret handling:** Credentials reach only the required runtime;
  rotation/reload works; values and sensitive payloads stay out of logs, images,
  committed fixtures, and shared artifacts.
- [ ] **C05 — Freshness budget:** Document the consumer-visible freshness contract,
  routine runtime budget and headroom, workload assumptions, and recovery deadline.

## Extraction and source change

- [ ] **E01 — Pagination:** Multi-page parents and children are complete; empty and
  final pages terminate correctly; repeated/nonadvancing cursors stop visibly.
- [ ] **E02 — Representation:** Null/missing fields, nested data, identifiers, exact
  money, currencies, and timezone offsets survive extraction without silent changes.
- [ ] **E03 — Initial load:** Empty destinations and empty resources initialize
  correctly; a bounded sample cannot masquerade as complete history.
- [ ] **E04 — Incremental boundaries:** Equal timestamps, interval edges, overlap,
  out-of-order responses, and late updates produce the expected records.
- [ ] **E05 — Source movement:** Changes during extraction have a stated consistency
  contract; a controlled example demonstrates how a later refresh catches them.
- [ ] **E06 — Deletions:** Root deletion, child removal, and an empty replacement
  child set produce the intended destination state. Distinguish no change from deletion.
- [ ] **E07 — Historical reach:** A correction outside the routine window is detected
  or explicitly outside the guarantee; demonstrate the chosen reconciliation path.

## Loading, state, and recovery

- [ ] **L01 — Repeatability:** Repeated identical input creates no duplicate business
  keys; changed values replace earlier ones under the intended write disposition.
- [ ] **L02 — Child consistency:** Updated parents do not leave stale children or
  duplicates; incomplete children cannot be published as a complete replacement set.
- [ ] **L03 — Checkpoint commit:** Failures before/during loading cannot advance state
  past uncommitted data. Verify the actual destination behavior, not only a mock.
- [ ] **L04 — Retained work:** Storage retries reuse acquired bytes; destination and
  transformation retries reuse completed extraction. Assert source request counts.
- [ ] **L05 — Restart:** Replace the run container/process and demonstrate safe
  resume from persisted state, including an interrupted pending load.
- [ ] **L06 — Backfill:** Historical replay leaves routine cursors unchanged and
  does not overwrite newer values with stale observations unnoticed.
- [ ] **L07 — Concurrency:** Independent namespaces run without interference; writes
  sharing a dataset/checkpoint serialize or reject an unsafe overlapping run.

## Failure and operating cost

- [ ] **O01 — Failure classes:** Retry transient transport failures; stop clearly on
  authentication, permission, malformed control data, and incompatible schema errors.
- [ ] **O02 — Composed retries:** Mixed failures respect a shared logical-read limit;
  nested retries cannot multiply attempts beyond it. Honor provider throttling.
- [ ] **O03 — Whole-run limits:** Request and time budgets terminate work visibly;
  exhaustion never becomes a successful empty or partial load.
- [ ] **O04 — Cancellation:** Stop without publishing incomplete data; document any
  remote work that survives cancellation and how to identify or resume it.
- [ ] **O05 — Resource use:** Measure runtime, requests, bytes, memory/disk use, and
  repeated reads by phase. Large inputs remain bounded or have a documented ceiling.
- [ ] **O06 — Diagnosis:** A failure identifies source, target, phase, and resumable
  work without sensitive values. An operator can follow a short recovery procedure.

## Schema and transformations

- [ ] **S01 — Additive change:** An unexpected field survives raw loading without
  breaking otherwise usable records; an empty initial batch does not hide it forever.
- [ ] **S02 — Incompatible change:** Missing fields, changed types, and changed nested
  structure have explicit outcomes; invalid keys/cursors cannot silently corrupt state.
- [ ] **S03 — Model change:** Rehearse a staging column addition/rename/type change
  and document the required rebuild or migration and its recovery path.
- [ ] **S04 — Build modes:** Full refresh and incremental repeat/update behavior
  produce equivalent intended results where incremental models are used.

## Local, warehouse, and orchestration

- [ ] **P01 — Backend parity:** The same representative fixtures execute in DuckDB
  and development Snowflake with equivalent keys, values, and expected types.
- [ ] **P02 — Warehouse rights:** Runtime roles can perform necessary operations;
  ownership supports repeat runs/rebuilds; development identities lack production
  write access. Record the grants required for each identity.
- [ ] **P03 — Orchestration:** The installed containerized flow loads and transforms
  successfully, reports failures, and supports downstream-only rerun.
- [ ] **P04 — Contributor workflow:** Two isolated developer/CI runs can build and
  test their changes; release/configuration changes do not overwrite shared state.
- [ ] **P05 — Deployment rehearsal:** Exercise installation and a changed model in
  development; distinguish reverting code from reversing data/schema changes.

## Reconciliation and readiness

- [ ] **R01 — Comparison:** Define reference, cohort, observation times, and measures;
  compare identity sets and values as well as counts. Explain known reference limits.
- [ ] **R02 — Convergence:** Measure initial, overlapping, and reconciled results;
  retain unexplained differences instead of hiding them in net aggregate totals.
- [ ] **R03 — Freshness vs validity:** Frozen fixtures can pass validity checks;
  operational checks assess the selected environment's actual freshness expectation.
- [ ] **R04 — Publication:** Consumers can distinguish provisional, last-good, and
  failed/incomplete data according to the source's declared publication behavior.
- [ ] **R05 — Readiness:** Representative-volume, repeated runs establish the intended
  availability deadline and runtime headroom across the applicable scenarios above.
  Report timing distribution, misses, failures, and backlog; record remaining gaps
  before claiming production readiness.
