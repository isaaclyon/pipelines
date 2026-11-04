# Transformations agent instructions

Scope: `packages/transformations/`. Inherit the root and parent-folder agent instructions.

Owns the dbt project, transformation models and tests, and published mart contracts.
Start with `packages/transformations/README.md` and the local project configuration.

Enforce `engineering-standards.md#freshness-ownership`: this package owns source
and model freshness expectations, measurement rules and warning/failure checks,
including contracts on raw sources. Dagster runs those checks and consumes their
policy when scheduling; ingestion supplies execution and coverage evidence.

## Commands

- `scripts/platform local check transformations` runs this package's checks,
  including `dbt parse` and SQLFluff.
- `scripts/platform <local|development> dbt <arguments>` runs dbt against the
  checked-out project; `development` uses the isolated Snowflake target.
- `scripts/platform local dg transformations <arguments>` runs `dg` against this project.

A test module that executes models of the real dbt project declares them with
`pytestmark = pytest.mark.dbt(<selectors>)`; the suite fails undeclared execution.
CI uses the declarations to skip tests a change cannot affect (ADR 0026,
`tests/dbt_selection.py`).

Keep the dbt project inside this package and include it in the built wheel.
Define asset-key mappings with the first source/model contract. Publish through
`analytics_transformations.publication` so candidate builds and required checks
finish before replacing consumer relations. Keep the reviewed mart-contract
snapshot aligned with the manifest. See the package README for publication,
compatibility, consumer grants, and interrupted-publication recovery.
