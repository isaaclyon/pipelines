# Dagster control plane agent instructions

Scope: `deploy/dagster/`. Inherit the root and parent-folder agent instructions.

Owns instance configuration and code-location registration.
Start with `deploy/dagster/README.md`. Component definitions belong in their packages.

Enforce `engineering-standards.md#freshness-ownership`: orchestration schedules
work from transformation-owned freshness contracts and measured execution
capacity. Own cadence, catch-up, execution deadlines and responses to check
results. Keep freshness thresholds with their data contracts.

Use `scripts/platform local up` and `smoke` to verify both code locations and
fresh run containers. Preserve matching runtime pins and serialized DuckDB
writes until tested resource-specific concurrency limits are introduced.
