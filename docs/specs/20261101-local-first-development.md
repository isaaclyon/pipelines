# Local-first single-user analytics development

## Decision and boundary

This specification implements the policy/design stage of
[ADR 0028](../adrs/0028-develop-locally-before-shared-snowflake-release.md).
The owner approves local DuckDB with the same Cube definitions until shared
Snowflake use is needed. A later stage implements the remote-safe consumer gate
and synthetic demo, and another adds a bounded copy/export/serve command for already built
and validated orders marts; see the [owning procedure](../../packages/metrics/README.md#worktree-local-copy-and-serving).
That command does not fetch sources or run dbt. The fuller refresh sequence below
remains the contract for future integration. No real-data acceptance or shared
Snowflake readiness is implied by its synthetic checks.

The first implementation serves the orders group needed by the governed order
metrics from data already available through an explicitly approved local
acquisition/copy. Customer profiles and catalog enrichment keep their separate
scope and owners.
Do not fetch remote data merely because the local file is missing.

## Existing reusable components

- `packages/metrics/tests/orders-model.test.cjs` starts the pinned Cube server
  with the stock DuckDB driver, memory cache and scheduled refresh disabled.
  `tests/model-directory.cjs` copies the maintained models and reviewed dbt
  metadata. Reuse that connection seam and `reporting-timezone.cjs`; do not fork
  metric definitions or ship the test secret.
- `dbt/profiles.yml` already has a one-thread local DuckDB target;
  `scripts/platform local dbt` mounts the checkout. Existing candidate checks
  cover contracts, declarations, required tests and output types.
- `cube.js` and `publication.cjs` accept governed consumers only with explicit
  local DuckDB selection. Remote services remain Snowflake-configured and default
  off for these consumers. Do not turn a synthetic environment override into a
  development configuration contract.
- The current local Compose scaffold checks metadata only. It neither exports
  analytical files nor implements stop/refresh/restart. Existing local volumes
  are not evidence that a safe serving lifecycle is present.

## State and file ownership

Each worktree selects explicit absolute paths to its own working database,
analytical serving database and ingestion state. No automatic shared default
file across branches, hard links to another branch's writable database, or
implicit path outside the selected worktree data directory. Secrets/data stay
ignored and outside images. Give each stack distinct ports/project and check
image tags; a Compose project name alone does not isolate hardcoded image tags.

To copy a development database: stop the source's writers and readers, acquire
exclusive access, checkpoint successfully, close the checkpoint connection and
verify no managed process still holds a handle. Copy to a new destination with
no open handles; never overwrite another branch's file. Reopen/validate the copy
before use. Refuse copying when quiescence cannot be established. Do not copy a
live WAL or use a filesystem copy while Cube/dlt/dbt is connected. Keep the
source unchanged. The source owner prepares the checkpointed, closed input;
the copy command mounts it read-only, refuses any WAL, and never checkpoints
or repairs it. A read-only DuckDB attachment validates the private copy before
export. Record source file/generation, copy time and branch privately;
copying a database is not proof of source completeness. Destination/pipeline
state must agree before ingestion resumes; never share a dlt checkpoint between
branches.

The working file may contain raw/restricted data. Cube must not mount it or its
parent directory. Export only approved analytical relations/columns into a
separate serving file; no raw, staging, intermediate, restricted contact tables,
free-text PII or attached source databases may survive in it. Inspect the actual
file catalog and columns, not only the Cube allowlist. Existing legacy
`dim_order_contacts` contains restricted fields: do not export it wholesale.
Supply only its approved geography columns when the unchanged model uses that
relation name, or use the approved geography relation with the corresponding
model configuration. The exact export allowlist derives from reviewed mart
metadata and existing public Cube lineage, with explicit privacy review; it
must not infer that every column of an analytical mart is safe. Exclude columns
with restricted or missing/unknown privacy classification. Preserve legacy
unknown classification as unknown in provenance; never fill it with false or
reinterpret current declarations as proof of a historical privacy decision.

## Stop–refresh–validate–restart

1. Select the local target, worktree paths, loopback port and non-fixture secret
   explicitly. Reject remote targets, ambiguous paths and concurrent operations.
2. Stop the local Cube process and close all database handles. Confirm it exited.
3. Refresh existing local data only within the authorized source scope, then run
   the selected dbt build and required error-severity tests/type/privacy checks.
   Use one writer. No implicit source fetch or remote fallback.
4. Build the analytical-only serving file while Cube remains stopped. Give its
   marts and one-row `_publication` marker the same new ID; availability evidence
   must match that ID. Preserve real coverage limitations, not fixture status.
   Validate schema, values needed by the selected models and the actual export
   allowlist before startup. A local marker identifies this validated build;
   it is not a Snowflake publication or a completeness/freshness certificate.
5. Close/checkpoint the serving file and release all writer handles. Mount it
   read-only and open the driver read-only. If the pinned driver cannot do that
   without writing, stop and resolve the constraint; never mount raw data or
   silently enable writable serving. Keep caches process-local and restart them
   with Cube; no live file replacement or generation-watching driver.
6. Start the same Cube definitions, loopback only, authenticated with a private
   non-fixture secret, scheduled refresh off. A bounded startup probe must fail
   if required columns/marker/metadata are invalid. Do not serve before checks
   finish. Query errors remain errors, never zero or fabricated completeness.

Any failure leaves Cube stopped and retains private diagnostics. There is no
promise of uninterrupted service, automatic rollback to the old file, or a
multi-user refresh protocol. Validate copying, failure cleanup and restart using
synthetic data before handling private real data.

## Prevent automatic remote consumer activation

A workflow that automatically deploys each `main` commit to a shared environment
changes code, not warehouse models. Merging a metrics change can then expose new
references to unpublished columns even when no one explicitly runs a warehouse
publisher. This repository ships no such workflow, but any shared deployment you
add must respect this section.

Automatic publication also counts as shared activation. A deployment that
preserves previously enabled sensor states lets a successful input write
can then request a publication using the newly deployed transformation code
(`defs/development_publication.py`). Candidate validation still applies, but the
consumer gate alone does not hold warehouse writes. Actual remote sensor state
is unknown; do not infer a hold from a default-disabled declaration.

Before merging changes that can affect automated shared publication, require
completed applicable Snowflake acceptance or a separately approved, verified
publication hold covering those changes. Record the hold's target, scope and
evidence; inspecting or changing remote automation requires separate approval.
This policy PR authorizes neither inspection nor disabling services. If neither
acceptance nor a verified hold exists, keep the affected change merge-held.

Implement a separate gate before merging the consumer change:

- Add `GOVERNED_ORDER_METRICS_ENABLED`, strict boolean, default **false**. In the
  initial local-first implementation it can be true only with explicit local
  environment and DuckDB serving; remote targets reject true rather than accept
  an accidental environment override. A later shared-activation change can open
  it after the Snowflake release checks pass.
- Off must omit the new AOV/net-sales members, new eligibility dimensions and
  coverage members, and the entire line interface at model compilation. Do not
  merely hide `/meta` fields, catch database errors or return null. Preserve the
  pre-change remote surface and ensure generated SQL has no new column/table
  dependencies from this feature. This prevents new incompatibility; it does
  not claim to repair any older remote publication mismatch.
- Local serving uses `CUBEJS_DB_TYPE=duckdb` honestly, with the existing
  `ANALYTICS_ENVIRONMENT=local` target. Model helpers receive validated backend
  settings. No new top-level environment name, custom DuckDB driver or duplicated
  semantic model is needed. The feature gate is separate from backend selection.
- Update configuration validation, typed settings/rendering, model compilation,
  fixture variants, and generated exposures for both gate states. Keep remote
  settings and CI scaffold default off. Prove remote disabled queries compile
  and execute against the actual supported predecessor shape, and local enabled
  queries exercise all new fields. Missing published fields must not be needed
  merely to compile disabled models. Generated exposures must not introduce new
  consumer dependencies when the gate is off. Test explicit local/CI/development/
  production configurations, absent/false/true/invalid flags and backend mismatches;
  only explicit local + DuckDB + true may activate this feature initially. Tests
  must prove the old remote members and generated SQL remain unchanged, not just
  assert that the default environment value is false.
- Until this gate is implemented, reviewed and passing in the proposed release,
  #242 stays draft/merge-held. Landing this policy PR alone does not release it.
  Do not disable the remote deployment workflow or change remote settings as a
  substitute for the gate. The policy PR itself changes no service behavior.

## Implementation slices and acceptance

1. **This policy/design PR:** accepted ADR, standards/delivery changes,
   implementation status and release-safety design; documentation checks and
   independent review. No runtime/config changes.
2. **Remote-safe consumer gate:** bounded follow-up in metrics/config/exposure
   ownership, with failing tests for default-off legacy shape, local enabled
   semantics and rejection on remote targets. Rebase/update #242 to include the
   proven boundary before requesting its merge. Backend enablement must not be
   assumed implemented by the flag alone.
3. **Local serving lifecycle:** reuse existing containers, dbt checks and vendor
   driver; add explicit per-worktree file selection/copy, analytical export,
   read-only loopback serving and bounded stop/refresh/validate/restart. Test
   active-handle copy refusal, branch isolation, private-file permissions,
   excluded PII, invalid marker/schema, failed build/export/startup leaving Cube
   stopped, and successful refresh with unchanged metric semantics. No daemon,
   scheduler, live replacement or multi-user coordination subsystem.
4. **Local outcome acceptance:** explicitly authorized local real-data use with
   provenance/coverage disclosure and owner validation. This is useful local
   acceptance, not shared Snowflake readiness. No new fetching is implied.
5. **Before shared Snowflake use:** separately execute adapter type/value parity,
   actual-predecessor compatibility, candidate publication/failure/concurrency,
   persistent-reader and PUBLIC access/denial checks, approved PII boundaries and
   intended-user access/availability. Budget and target each live run. Only then
   approve remote consumer activation; never mark these checks passed from local
   results or skip them because local development succeeded.

Shared release compatibility, source semantics, personal-data classification
and source-access authorization remain intact. Local copies do not create
negative-history proof or resolve unknown eligibility. This
specification defines behavior, not a status tracker.
