# Metrics agent instructions

Scope: `packages/metrics/`. Inherit the root and parent-folder agent instructions.

Owns Cube semantic definitions and service configuration.
Start with `packages/metrics/README.md` and `packages/metrics/package.json`.

## Commands

- `scripts/platform local check metrics` runs this package's checks.

Keep semantic definitions in `model/`. There are no marts yet: the model holds
one placeholder cube that reads no warehouse table, so the service compiles and
the smoke check sees a cube. The first generic marts and Cube measures arrive in
Roadmap Phase 4; Cube will read them directly from the serving schema (ADR 0037).
Model files run in a sandbox without `process`. Every member declares a boolean
`meta.pii`, restricted PII stays out of Cube, and every public cube, view and
member has a description; `tests/model.test.cjs` checks this.
