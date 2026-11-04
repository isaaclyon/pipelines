# Configuration, recovery, and performance

Read the relevant official documentation:

- [Credentials and configuration](https://dlthub.com/docs/general-usage/credentials/setup)
- [Pipeline](https://dlthub.com/docs/general-usage/pipeline)
- [Schema contracts](https://dlthub.com/docs/general-usage/schema-contracts)
- [Performance](https://dlthub.com/docs/reference/performance)
- [Snowflake destination](https://dlthub.com/docs/dlt-ecosystem/destinations/snowflake)
- [DuckDB destination](https://dlthub.com/docs/dlt-ecosystem/destinations/duckdb)
- [CLI reference](https://dlthub.com/docs/reference/command-line-interface)

## Configuration and credentials

Resolve configuration through the owning package's established target interface.
Verify the final destination, dataset, pipeline identity, and working directory
before writes. Fail closed on missing or invalid target configuration.

Supply credentials at runtime and expose them only to the services that need
them. Keep non-secret settings separate from credentials. Do not print resolved
secrets or put them in exception messages. Inspect only redacted configuration
when debugging. Use established Snowflake service authentication and grants;
successful DuckDB loading says nothing about those remote settings.

Keep dependencies in the package lockfile. Add destination extras only where
required. Dashboards and workspace tools are optional diagnostics; core ingestion
must not depend on installing the dltHub harness or a paid runtime.

## Diagnose the failing stage

1. Reproduce with the smallest representative fixture and capture a redacted
   trace, pipeline identity, target, and load-package outcome.
2. **Extract:** check authentication, API version, filtering, pagination,
   response selection, retries, and source-contract errors.
3. **Normalize:** inspect schema changes, nested table relationships, precision,
   and inferred types. Select an explicit schema policy; do not silently discard
   new source fields to make a contract pass.
4. **Load:** inspect failed jobs, destination permissions, staging configuration,
   type compatibility, and merge behavior. A returned load summary still needs
   verification of failed-job status and expected rows.
5. **Recover:** establish which packages and state were committed, retry through
   the maintained path, and verify destination rows and cursor continuity.
   Avoid deleting local state or packages as a routine debugging shortcut.

Use the installed CLI help before issuing diagnostic commands. Commands such
as sync, drop, and refresh can change state or data; establish their exact scope
and authorization first.

## Tune the actual bottleneck

Measure extraction, normalization, and loading separately. Check batch sizes,
memory, API budgets, worker counts, and destination load times before tuning.

Prefer streaming and dlt's existing buffering and parallelism settings over
custom queues. Increase concurrency only when source rate limits, ordering,
resource dependencies, and destination write limits permit it. Serialize shared
DuckDB writers and checkpoint owners. Confirm that tuning preserves completeness
and repeat-run behavior with representative fixtures.

Record source row counts or reconciliation totals, resource outcomes, cursor
ranges, and run/target identifiers without exposing sensitive payloads. Define
raw retention and checkpoint recovery as operational contracts; deployment
rollback does not undo loaded warehouse data.
