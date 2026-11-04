---
name: using-dlt
description: >
  Build, modify, debug, and review dlt (data load tool) ingestion pipelines.
  Use for source selection, verified connectors, REST API configuration,
  database and filesystem sources, custom Python or GraphQL extraction,
  DuckDB and Snowflake loading, incremental cursors, merge and replay behavior,
  schema evolution, pipeline state, recovery, and ingestion performance.
---

# Using dlt

Implement ingestion under [ADR 0017](../../../docs/adrs/0017-ingest-data-with-dlt.md)
and the [engineering standards](../../../engineering-standards.md). dlt owns
extraction and raw loading; dbt owns warehouse transformations. Load
[dagster-expert](../dagster-expert/SKILL.md) when changing orchestration.

## Before changing a pipeline

1. Read the owning package's instructions, dependency lock, source code, and
   tests. Reuse its commands and configuration. Keep dlt and destination extras
   in that package's independent uv project; execute through its container
   commands. Do not create a shared root environment or install tools on run
   startup.
2. Identify the requested source entities, keys, update/deletion behavior, auth,
   pagination, history, and intended target from the task and existing code.
   Ask only when missing information changes implementation.
3. Default local and ordinary CI work to fixtures and isolated DuckDB files.
   Live access requires an explicit development target. Select production only
   when authorized; invalid configuration must fail before writes.
4. Read current dlt docs and authoritative provider docs for the behavior being
   changed. Use Context7 when available and the
   [official docs index](https://dlthub.com/docs/llms.txt) to find specific pages.
   Check syntax against the package's pinned version; avoid copying examples
   from a newer version without verification.

## Choose a source

Use the first suitable option, checking that it actually delivers the required
records:

| Input | Approach |
| --- | --- |
| Native share or vendor export | Use the existing relation or filesystem loading when it reduces extraction work; declare the downstream source contract |
| Service with a maintained verified connector | Check API version, required resources, auth, incremental and deletion support; reuse or adapt it |
| Database or files | Use the built-in `sql_database` or `filesystem` source |
| Standard HTTP/JSON API | Use `rest_api_source` with explicit endpoint and pagination configuration |
| GraphQL, asynchronous bulk jobs, or unusual protocols | Use custom `@dlt.source` / `@dlt.resource` code |

Source catalogs and generated templates are starting points. Verify provider
behavior and connector maintenance. When using `dlt init`, check the installed
CLI help, scaffold in an isolated scratch directory, and review generated files
before incorporating them into the owning package.

## Implement and verify

1. **State the source contract.** Record row identity, requested entities,
   cursor type and boundaries, write disposition, nested data, schema policy,
   and history/replay limitations. Parse configuration and extraction control
   fields at boundaries while preserving unknown raw fields.
2. **Write a focused failing test for changed behavior.** Use deterministic
   responses or files. Exercise the material risk, such as a second page,
   same-timestamp updates, nested children, or a failed load.
3. **Build the smallest complete extraction.** Reuse dlt clients, paginators,
   retries, and destination loading. Keep business transformations in dbt.
   Yield bounded batches; avoid buffering an entire export in memory.
4. **Use a bounded scratch load when it helps diagnosis.** Keep its destination
   and state separate from maintained pipelines. `add_limit` limits yielded
   items/batches, not necessarily rows. A limited run proves only that sample.
5. **Finish extraction and repeat-run behavior.** Verify all pages and nested
   resources, correct keys and cursor boundaries, and deliberate handling of
   updates, deletions, and overlapping windows. Remove sample limits from the
   maintained path. Use stable identities and datasets for incremental tests;
   `dev_mode=True` creates fresh datasets and hides continuity bugs.
6. **Verify loading and recovery.** Inspect loaded rows, expected types,
   normalized child relations, load outcomes, and state. Test a repeat run and
   relevant failure/retry path. Apply source reconciliation where available;
   a green run alone cannot prove completeness.
7. **Run the owning package's checks.** Use its containerized formatting,
   linting, strict typing, and focused behavior tests. For destination changes,
   execute representative fixtures in DuckDB. Under ADR 0028, development
   Snowflake execution remains required before shared activation but is deferred
   during local-only development. Report it as outstanding, never passed by local
   results; follow the publication/release boundary before merging changes that
   can run automatically in shared environments.

## Configuration and state

- Keep secrets out of chat, tracked files, images, and logs. Use existing
  runtime secret injection; local secret files must be ignored. Read secret
  values only through the approved secret tooling when required by the task.
- Keep pipeline names, dataset names, destination configuration, and working
  directories explicit and environment-specific. Serialize runs sharing state
  or a writable dataset; isolate independent tests and developers.
- Use `merge` for keyed mutable entities, `append` for immutable records with
  defined replay handling, and `replace` for complete intentional snapshots.
  Incremental extraction and destination write behavior are separate choices.
- Preserve recoverable state outside ephemeral containers. Document how local
  working state and destination-synchronized state recover after failure.
- Treat state resets, dataset drops, full refreshes, and history rewrites as
  data operations. Establish affected targets, replay coverage, and recovery
  before performing them; obtain confirmation for destructive actions unless
  already authorized.

## Read references as needed

| Task | Reference |
| --- | --- |
| REST auth, pagination, selectors, dependent resources | [REST API sources](references/rest-api.md) |
| Cursor boundaries, overlap, merge, backfills, checkpoints | [Incremental loading and state](references/incremental-state.md) |
| Python sources, GraphQL, bulk exports, boundary parsing | [Custom sources](references/custom-sources.md) |
| Configuration, load failures, schema changes, performance | [Operations](references/operations.md) |

See [provenance](PROVENANCE.md) for upstream attribution and maintenance scope.
