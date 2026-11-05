# Review guidance

These rules apply to any agent or person reviewing a change in this
repository. They add to [`AGENTS.md`](AGENTS.md) and
[`engineering-standards.md`](engineering-standards.md); they do not repeat them.
Check the change against those standards and the accepted
[ADRs](docs/adrs/README.md), then apply the rules below.

## Report findings with evidence

- Cite a file and line for every finding. Quote the code or output that shows
  the problem; do not infer behavior from a name.
- Reproduce a defect when you can, with a fixture, a focused test or a command,
  and give the command. Say when a finding is unverified.
- State the impact: who or what breaks, and when.
- Order findings by impact. Mark each one **blocking** or **non-blocking**.
- Report at most three non-blocking style or naming comments. Do not report
  what a formatter, linter or type checker already enforces.
- Report no findings when you find none.

## Blocking findings

A finding blocks the merge when the change:

- lets personal or `restricted` data reach Cube, the docs site, logs, or any
  reader that the contract does not allow;
- puts a secret in source, images, build arguments, output or logs;
- lets a live, deploy or warehouse command run without an explicit
  environment, or fall back to production;
- loses, duplicates or silently drops source rows, or breaks replay or
  recovery from a failed load;
- removes or skips a mart's required error-severity tests, or downgrades a
  correctness check to a warning without preserving its guarantee;
- changes shared models without portable implementations, treats local execution
  or compilation as proof of Snowflake compatibility, or activates shared serving
  without its deferred Snowflake checks under
  [ADR 0028](docs/adrs/0028-develop-locally-before-shared-snowflake-release.md);
- makes a shared deployment reference mart columns that its serving schema
  does not have yet;
- imports another package's internals instead of its public API, or joins
  package dependencies or lockfiles;
- changes a published contract (mart columns, Cube members, raw tables)
  without a compatible release path;
- builds functionality that the standard library, a dependency the project
  already has, or a widely used library provides, or would provide with small
  configuration. See [below](#prefer-existing-tools).

## Prefer existing tools

Before accepting new framework code, ask what already does the job. Check, in
order: this repository, the language's standard library, current
dependencies (dlt, dbt, Dagster, Cube), then well-known maintained libraries.
Require a concrete prevented failure and a contributor-cost justification for
new governance declarations or validators. Naming preferences are review guidance,
not blocking checks. Preserve privacy, layer boundaries and consumer contracts.
Flag new code that rebuilds features those tools offer, such as state,
retries, pagination, incremental loading, schema handling, scheduling or test
types. The finding should name the existing feature and the code it replaces.

[ADR 0024](docs/adrs/0024-load-shopify-orders-with-dlt-incremental-merge.md)
records why: the first Shopify orders pipeline grew to about 6,000 lines of
batch ledgers, locks and version guards around dlt, then was rebuilt on dlt's
own incremental merge. Custom code is acceptable when the change shows that
the existing tool cannot meet a stated requirement.

## What failure looks like here

Weigh findings by the harm each area can do:

| Area | Failure |
| --- | --- |
| Ingestion (`packages/ingestion/`) | Rows are lost, duplicated or loaded twice after a retry |
| Transformations (`packages/transformations/`) | A mart holds wrong numbers or exposes restricted columns |
| Metrics (`packages/metrics/`) | A Cube member returns a number that differs from its definition |
| Deployment (`deploy/`, `scripts/`) | A command touches the wrong environment or leaks a secret |
| Documentation (`docs/`, READMEs) | It describes behavior the code does not have |

## Generated and vendored files

Review the generator and its check, not the output. Skim lockfiles (`uv.lock`,
`package-lock.json`) only to confirm that they were regenerated, and review them
through the dependency change that produced them.

## Tests and documentation

- A behavior change needs a test that fails without it. Flag tests that only
  restate the implementation or test private helpers.
- A change to behavior, contracts or procedures updates the owning
  documentation in the same change.
