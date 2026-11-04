# 0026: Scale dbt checks with the change

Date: 2026-10-30

## Status

Accepted. Builds on [0025](0025-use-duckdb-for-deterministic-tests.md).

## Context

The transformations check ran for about 15 minutes on GitHub-hosted runners,
then about 10 after an earlier speedup.
About 20 tests each built the whole dbt project, so the check grew faster than
the project. Larger or more runners would only buy time; the work itself grew
with the number of models times the number of full-build tests. Changes that
cannot affect dbt results, such as deployment timing or ingestion code, also
ran every dbt test.

## Decision

The cost of dbt checks follows the change, not the project.

- CI runs dbt checks only for dbt-affecting changes: the dbt project, the
  Python code that builds, validates or publishes it, the transformations
  dependencies, the check image and check scripts, and the dbt tests. Other
  changes skip dbt checks, including after merge on main.
- A pull request that changes only dbt project files or test modules checks
  the dbt nodes it changed and their descendants (`state:modified+`), compared
  with the base branch's project parsed offline. Changed test modules always
  run. SQL lint covers the changed model files, or every model when the
  macros change. Any other dbt-affecting change, and every dbt-affecting merge
  to main, runs every check.
- A test that executes models of the real dbt project declares the nodes it
  exercises with a dbt selector. The suite fails a test that executes nodes
  outside its declaration, so change selection cannot silently skip it.
  A test that runs a sample project with copies of real project files, such as
  the macros, declares those files and runs only when they or its code change.
- Each rule is tested at its cheapest reliable level: dbt unit tests for model
  logic, one shared synthetic build for data tests and contracts, edited copies
  of that build for failure cases, a small sample project for publication and
  execution code, and parsed-manifest checks for project-wide conventions.
  A test builds the whole real project only when the whole project is its
  subject.

## Consequences

- A typical pull request's dbt check time depends on how much it changes.
- Change selection can miss an effect dbt does not track. The full run after a
  dbt-affecting merge catches it before Release deploys.
- Test modules carry a dbt selector, and model logic moves from Python fixtures
  to dbt unit tests.
- Routing rules decide which changes can affect dbt; their tests must cover
  each rule.
