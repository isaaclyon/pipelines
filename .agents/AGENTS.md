# Analytics Platform agent instructions

Paths in this guide are relative to the repository root. Read the scoped
`AGENTS.md` in each folder you work in; scoped instructions supplement this guide.

## Repository context

This monorepo contains dlt ingestion, dbt Core transformations, Cube metrics,
and Dagster orchestration, using Snowflake and local DuckDB targets.
Read `README.md`, `engineering-standards.md`, and the relevant accepted records
in `docs/adrs/` before changing a component. Accepted ADRs and engineering
standards define the direction.
Verify implementation in source and configuration rather than assuming an
accepted decision or a scaffolding README describes completed functionality.

## Current stage: no users or deployments

Nothing is deployed, and no person or system depends on this repository.
The platform runs only locally, including in Docker on a developer machine.

- Make breaking changes freely when they produce a simpler design. Do not keep
  shims, fallbacks, deprecated paths or migration windows for existing users.
- Prefer strict, uniform checks. A failing check costs a local rerun, not an
  outage.
- Keep building the release and compatibility safeguards that
  accepted ADRs describe; they are part of the product. Do not weaken a design
  to protect current behavior or data.
- Update this section when the first shared deployment or consumer exists.

## Folder guidance

| Folder | Responsibility |
| --- | --- |
| `packages/` | Independently buildable components and dependency boundaries |
| `packages/ingestion/` | Source extraction, loading, state, and recovery |
| `packages/transformations/` | dbt project, models, tests, and published marts |
| `packages/metrics/` | Cube definitions and service configuration |
| `deploy/` | Deployment configuration and runtime topology |
| `deploy/dagster/` | Control-plane configuration and code-location registration |
| `deploy/compose/` | Container builds and environment-specific service wiring |
| `docs/` | Architecture decisions and specifications |
| `docs/adrs/` | Architectural decisions and their index |
| `docs/specs/` | Implementation specifications as they are introduced |
| `scripts/` | Shared validation and container helpers |

## Working and validating changes

- Preserve unrelated work in the checkout and keep changes scoped to the request.
- Keep package dependencies, lockfiles, and environments independent. Declare
  cross-package dependencies and use public APIs rather than sibling internals.
- Follow `engineering-standards.md` for typing, boundary parsing, SQL portability,
  environment isolation, and container execution. Use the relevant local skill
  for dlt, Dagster, dbt, Snowflake, or architectural decision work.
- For nontrivial behavior changes, start with a meaningful failing test. Run
  relevant checks in the owning component's container and update its documentation.
  `scripts/check-python.sh` contains the Python validation gates;
  `packages/metrics/package.json` defines the metrics check. Check the current
  deployment files for supported build and execution commands.
- Before pushing, run relevant checks locally through `scripts/platform` and fix
  failures. GitHub runs only a secret scan, so local checks are the gate. Use
  focused package checks for package changes; run `ci verify` for container,
  dependency, deployment, or orchestration changes. Follow the
  [local CI procedure](../deploy/compose/README.md#local-validation-before-pushing).
  Report checks you could not run.
- For documentation-only changes, verify paths, links, and instruction symlinks.
  Report checks actually run and distinguish local validation from deployment.
- Keep secrets out of source and output. Live integration and deployment commands
  require explicit environment selection; ordinary checks use offline fixtures.

## Shared agent configuration

Edit all agentic items, including skills, MCP configuration, and agent instructions,
in `.agents/`. Treat `.agents/` as the canonical source and symlink the corresponding
files or directories into `.pi/` and `.claude/` for parity across agents.

Apply the work-tracking policy in `engineering-standards.md` (Documentation and
work tracking) before making changes.

Add and update skills in `.agents/skills/`; `.pi/skills` and `.claude/skills` link
to that directory. Keep shared changes in the canonical files and verify that
the symlinks resolve after changes.

Codex discovers repository skills in `.agents/skills/`. The root `AGENTS.md`,
`.pi/AGENTS.md`, and `.claude/AGENTS.md` link to this file. Scoped folder guides
live in `.agents/instructions/`, mirroring their repository folder paths, with
an `AGENTS.md` symlink in each owning folder. Edit the canonical targets and
keep placeholders small until component-specific guidance is established.

`REVIEW.md` holds review guidance for any agent or person reviewing a change.
Keep it a regular file at the repository root, not a symlink: review tools read
it from the base branch and reject linked guidance.

Use `.agents/` as the only shared agent directory; do not create `.agent/`.
Keep platform-specific skill paths linked to it.
