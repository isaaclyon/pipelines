# Ingestion agent instructions

Scope: `packages/ingestion/`. Inherit the root and parent-folder agent instructions.

Owns source extraction and loading, ingestion state and recovery, and its code location.
Start with `packages/ingestion/README.md` and the local project configuration.

Enforce `engineering-standards.md#freshness-ownership`: source/loader code owns
mechanics, progress and timing evidence. Keep business freshness expectations,
thresholds and verdicts out of it. Dagster definitions in this package follow
orchestration ownership and consume transformation-owned freshness contracts.

## Commands

- `scripts/platform local check ingestion` runs this package's checks.
- `scripts/platform local dg ingestion <arguments>` runs `dg` against this project,
  for example `list defs`.

Keep pipeline identities and state directories explicit. Add sources through
the dlt integration after defining their extraction and recovery contracts.
`scaffold_check` is an infrastructure diagnostic, not a data asset.
