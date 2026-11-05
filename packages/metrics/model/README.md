# Semantic definitions

There are no published marts yet, so the model holds one placeholder.

| File | Public interface | Reads |
| --- | --- | --- |
| `placeholder.js` | cube `placeholder` | A constant `SELECT 1 AS id`; no warehouse table |

The placeholder exists only so the service compiles and the platform smoke
check sees a cube. Replace it with the first generic mart's cube in Roadmap
Phase 4.

Every dimension, measure and segment needs an explicit boolean `meta.pii`,
including hidden members. Generated lists carry a decision per entry; do not
add a default that silently classifies future fields. Every public cube, view
and member needs a `description`. `tests/model.test.cjs` checks both rules.

Model files cannot read `process.env` directly: Cube compiles them in a
sandbox.
