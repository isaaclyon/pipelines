# Metrics agent instructions

Scope: `packages/metrics/`. Inherit the root and parent-folder agent instructions.

Owns Cube semantic definitions and service configuration consuming published marts.
Start with `packages/metrics/README.md` and `packages/metrics/package.json`.

## Commands

- `scripts/platform local check metrics` runs this package's checks.

Keep semantic definitions in `model/`, consuming only published marts. Read
tables and the refresh key from `publishedMarts()` in `publication.cjs`; model
files run in a sandbox without `process`. Build dimensions that read a mart column under its
own name with `dbtDimensions()` from `mart-columns.cjs`; dbt owns their
description, type and PII flag (ADR 0014). Expose consumer fields through views
and keep restricted contact details out of them. Approved city and postal-code
geography stays available, including when customers are enabled. Follow ADR 0028:
single-user local DuckDB serving uses the same definitions and a separate
analytical-only file with stop-refresh-validate-restart. `scripts/platform local
cube` implements a bounded copy/export/serve command for already built, validated
orders marts; source refresh and dbt build orchestration are not included. Do not restore hot snapshot replacement or mount raw/restricted
working files. Keep deterministic fixtures separate from real-data acceptance.
Snowflake publication and permission-denial checks remain deferred requirements
before shared activation, not evidence supplied by local checks. Automatic remote
deployments require a tested default-off/local-only gate for unpublished consumers.
Publication does not guarantee simultaneous refresh across dashboard queries.
