# Engineering standards

Each rule below is something a reviewer or an automated check can verify.
Rationale lives in the linked [ADRs](docs/adrs/README.md); this file does not
restate it. Rules marked *(checked)* name the gate that enforces them; the rest
are enforced in review.

## Package isolation

[ADR 0007](docs/adrs/0007-use-a-monorepo.md),
[ADR 0008](docs/adrs/0008-isolate-package-dependencies.md),
[ADR 0003](docs/adrs/0003-isolate-dagster-code-locations.md).

- Each Python package has a `src/` layout, its own `uv.lock`, and its own
  environment. No uv or npm workspace joins packages.
- Every imported dependency is declared. Cross-package imports use the other
  package's public API; no underscore modules, relative paths into sibling
  packages, or `PYTHONPATH`/`sys.path` changes.
- Tests run against the installed package, not only an editable install.
- A runtime image contains only its own component's dependency closure.

## Ingestion

[ADR 0017](docs/adrs/0017-ingest-data-with-dlt.md); use the
[dlt skill](.agents/skills/using-dlt/SKILL.md).

- Each source declares its keys, cursor, update and deletion behavior, and
  replay guarantees, and lands source-shaped raw data with load metadata.
- Tests cover complete pagination, nested resources, and recovery from a failed
  load before incremental continuity is relied on.
- Pipeline state and datasets are isolated per environment. Runs that share a
  checkpoint or writable dataset are serialized.

## Freshness ownership

[ADR 0012](docs/adrs/0012-orchestrate-assets-with-dagster.md),
[ADR 0013](docs/adrs/0013-transform-data-with-dbt-core.md),
[ADR 0017](docs/adrs/0017-ingest-data-with-dlt.md).

| Owner | Owns | Must not contain |
| --- | --- | --- |
| Ingestion | Windows, cursors, completeness, checkpoints, recovery, and load evidence (durations, ranges, coverage gaps) | Business freshness targets, schedules, stale-data verdicts |
| Orchestration (Dagster) | Cadence, window planning, concurrency, catch-up, running checks and alerting | Freshness thresholds of its own |
| Transformations (dbt) | Acceptable age, deadlines, warn/fail thresholds, source freshness checks | Extraction logic |

- Changing a freshness threshold requires no ingestion change.
- A freshness check treats missing or unknown evidence explicitly; a recent load
  timestamp alone does not pass it.
- Freshness checks are tested with a completed-but-stale load and an empty
  successful extraction.

## Parse at boundaries

[ADR 0016](docs/adrs/0016-parse-data-at-trust-boundaries.md).

- Configuration, API responses, files, and persisted state are parsed into typed
  values where they enter the code. A parser returns a value or a specific error.
- A `cast` is not parsing. Untyped dictionaries do not pass through business code.
- Money keeps exact decimal precision; timestamps carry an explicit time zone.
- Parse errors name the source and field and exclude credentials and payloads.

## Strict typing

- *(checked: `scripts/check-python.sh`)* basedpyright runs in strict mode with
  `Any` reporting, per package, over source and tests.
- Untyped third-party code is confined to small adapters with typed outputs.
- A suppression names its diagnostic and gives a reason. No blanket ignores or
  error baselines.
- TypeScript packages enable `strict`, `noUncheckedIndexedAccess`, and
  `exactOptionalPropertyTypes`.

## Tooling

| Concern | Tool |
| --- | --- |
| Python environments | uv, one lock per package |
| Python types | basedpyright (strict) |
| Python lint and format | Ruff |
| Python tests | pytest |
| SQL lint | SQLFluff with dbt templating |
| Transformation tests | dbt unit and data tests |

- Tool versions are pinned and configured in the repository.
  Local commands run the same gates in containers.
- Release startup never resolves or installs dependencies.
- A lint exception is local, names its rule, and says why.

## Tests

- Nontrivial behavior changes start with a failing test.
- Tests assert observable contracts, not private helpers.
- The ordinary suite is offline and deterministic: fixed inputs,
  injected clocks, temporary storage. Live checks need an explicit development
  target.

## Governance enforcement

Governance has three categories. Do not give organizational preferences the same
blocking weight as consumer safety.

| Category | Enforcement |
| --- | --- |
| Security and correctness | Block missing privacy decisions, unsafe access, failing required tests, incorrect values and namespace/identifier collisions. Keep forward-only layer enforcement. |
| Consumer interfaces | Marts and Cube members are the consumer interfaces; review every change to them. Internal models are not stable consumer contracts. |
| Internal conventions | Prefer native dbt/SQLFluff configuration and lightweight review. Descriptive naming preferences do not block a change. |

- Before adding a custom declaration or validator, name the concrete failure it
  prevents and explain why native tooling or an existing check is insufficient.
  Its benefit must justify the contributor's authoring and maintenance burden.
- Generate mechanically derivable metadata. Native test severity determines
  blocking behavior; composite-key tests are matched by their columns. Generate
  proposed source-field inventories.
- Generation does not approve a change. Review source retirements; never
  silently replace the reviewed inventory. Privacy decisions,
  business meaning, grain and consequential correctness checks remain explicit.

## Transformation naming

- Prefer `stg_<source>__<entity>`, `int_<entity>__<purpose>`,
  `fct_<event_or_process>` and `dim_<entity>`, with plural entities and familiar
  business terms. Prefer descriptive names over execution-order prefixes,
  lifecycle placeholders and ad hoc copy/version suffixes. Review these choices;
  do not maintain a custom vocabulary validator.
- *(checked: layer dependency check)* Models stay in their layer folders, with
  matching layer prefixes. Staging retains `stg_<source>__<entity>` because the
  checker uses the source segment to enforce source-local staging dependencies.
  Aliases do not determine logical layer or grant access.
- *(checked: identifier and schema checks)* Persisted identifiers fit the
  portable 255-character limit and do not collide case-insensitively. Schemas use
  `<namespace>_<layer>`; seeds, snapshots and stored test failures use
  `<namespace>_seeds`, `<namespace>_snapshots` and `<namespace>_test_failures`.
  Raw schemas belong to ingestion. Schemas use lowercase unquoted names.
- Every model output column declares its name, description and explicit privacy
  decision. Actual outputs must match this inventory, including ephemeral
  ancestors, so omitting an internal type never hides an unclassified output.
- Published marts declare every type. Internal types are optional except where
  correctness metadata needs them: money, counts, quantities and explicit
  currency/temporal associations. When supplied, types must match actual outputs,
  including decimal precision and scale; they are not compatibility promises.
- Prefer `<entity>_id`, `<event>_at`, `<event>_date`, `is_`/`has_` booleans,
  `<meaning>_amount`, `<entity>_count` and `<thing>_quantity`. Prefix/suffix style
  does not block a change. Recognized amounts, counts and quantities still
  require exact-decimal, integer and numeric types respectively.
- Money keeps exact decimals. Published amounts expose their currency locally.
  Internal currency and temporal metadata are optional: document meaning and
  test conversions where they originate rather than repeating metadata through
  every model. Explicit associations are validated, including parent currency
  join keys and a correctness test when used.
- Published instants declare UTC; published dates declare a source-calendar-date
  basis or a source instant and conversion zone. Tests establish actual conversion
  correctness, including relevant midnight, daylight-saving and fallback cases.
- Published identities distinguish source identities from explicitly defined
  analytical identities. Source IDs remain source-qualified at the consumer
  interface. Use the meanings in [the glossary](GLOSSARY.md).
- Published-name changes follow
  [ADR 0020](docs/adrs/0020-keep-consecutive-releases-compatible.md), regardless
  of relaxed internal naming preferences.

*(checked: [model checks](packages/transformations/README.md#model-checks) and
output validation in fixture builds)* Ordinary container checks run static
checks and executed DuckDB fixtures. Development Snowflake acceptance verifies
identifier isolation before shared activation; local evidence does not replace
that deferred gate.

## Transformation contracts and tests

[ADR 0022](docs/adrs/0022-publish-marts-through-forward-only-transformation-layers.md),
[ADR 0020](docs/adrs/0020-keep-consecutive-releases-compatible.md).

- *(checked: [layer dependency check](packages/transformations/README.md#layer-dependency-check))*
  Models sit in their layer folder and depend only forward.
- Each model documents its grain and key, and tests the key for uniqueness and
  non-null where it promises them.
- Marts declare every column type, test consequential business rules, and
  reconcile against upstream data.
- Incremental models have repeat-run, update, and deletion tests alongside a
  full-refresh test.
- Each check uses native dbt severity: `error` fails the build and `warn` is
  informational. No duplicate blocking flag is required. A warning never
  satisfies a required correctness check.
- *(checked: `packages/transformations/tests/conftest.py`)* CI cost follows the
  change ([ADR 0026](docs/adrs/0026-scale-dbt-checks-with-the-change.md)). A
  test that executes real dbt project nodes declares them with
  `pytest.mark.dbt`. Test each rule at its cheapest reliable level, and build
  the whole project only when the whole project is the subject.

## Personal-data classification and Cube exposure

- Every root-project dbt model, source, seed and snapshot column declares
  `config.meta.pii: true` or `config.meta.pii: false`. Every Cube dimension,
  measure and segment declares boolean `meta.pii`, including hidden members.
  Strings, nulls, missing declarations and model-wide defaults are not decisions.
- The initial restricted PII category covers personal names, email, phone,
  street addresses, precise coordinates and free text that can contain personal
  details. Treat tax identifiers and unstructured customer metadata as restricted.
  Customer/source IDs, city and postal code are outside this initial category,
  subject to explicit exposure review. `pii: false` is not a claim of anonymity
  or legal certification.
- Authors make explicit classifications; reviewers assess their accuracy.
  Derived fields need their own decision. Do not infer or propagate classifications
  through lineage.
- *(checked: transformation model checks; metrics model compilation)* Missing
  classifications fail, including disabled
  definitions. The current declared-field inventory has no legacy exceptions.
  New or changed fields require declarations and existing declarations cannot be
  removed while keeping the field. The reviewed transformation
  `source_fields.json` inventory guards against deleting source declarations,
  including disabled sources. Generate proposed additions alongside new source declarations;
  remove entries only for reviewed retirement from ingestion and consumers.
  These checks do not discover arbitrary undeclared warehouse columns.
- Classification, exposure approval and a user's access grant are separate.
  Until the platform has named-user
  authentication, approved-field access and minimal auditing, restricted PII
  cannot be modeled in Cube, even as a hidden member. Public Cube members mean
  query interface exposure, not necessarily internet access.
- The data owner approves initial classifications and exposure decisions and
  records the approval with the change. Existing exposure is not
  automatic approval. Later privacy/legal review, retention, deletion requests
  and general raw-warehouse access governance remain separate work.

## Materialization

[ADR 0037](docs/adrs/0037-build-marts-in-place-in-the-serving-warehouse.md).

- Staging and intermediate models are views unless measured cost says otherwise.
- dbt builds marts in place in the serving schema, as tables. Tests run in the
  same `dbt build`; a failing test stops the models downstream of it.
- Incremental models are introduced only on measured need, with keys, deletion
  behavior, and full-refresh recovery defined.

## DuckDB and Snowflake

[ADR 0028](docs/adrs/0028-develop-locally-before-shared-snowflake-release.md).

- Single-user development uses local DuckDB files. Deterministic CI fixtures
  remain separate from private real-data experiments.
- Each worktree owns its writable database and ingestion state. Stop all handles,
  checkpoint and close before making an independent copy. No hot filesystem copy,
  shared writable file, or shared source checkpoint across branches.
- Keep one set of transformation models and governed metrics across backends.
  Database-specific SQL remains in adapter-dispatched macros with implementations
  for both adapters. Ordinary executed checks use DuckDB; compilation does not
  prove Snowflake compatibility.
- Before shared Snowflake activation, execute representative type/value parity
  and privacy checks in explicit development targets with an approved compute
  budget. These checks are deferred, not waived or passed by local results.
- Local-only work must not make a shared deployment reference mart columns that
  its serving schema does not have yet. Remote inspection/changes need approval.

## Containers and releases

[ADRs 0001 to 0005](docs/adrs/README.md),
[ADR 0019](docs/adrs/0019-deploy-releases-through-one-source-agnostic-workflow.md),
[ADR 0020](docs/adrs/0020-keep-consecutive-releases-compatible.md),
[ADR 0021](docs/adrs/0021-run-data-changes-as-orchestrated-pipeline-work.md),
[ADR 0023](docs/adrs/0023-maintain-dagster-storage-in-an-operator-window.md).

- Images build from pinned bases and locked dependencies, run as non-root, and
  carry no developer tools.
- Secrets never appear in source, images, build arguments, or logs.
  *(checked: gitleaks in CI and the `.githooks/pre-commit` hook)* Allowlist
  only documented fixture values in `.gitleaks.toml`.
- Only launcher services mount the Docker socket.
- *(checked: release workflow)* Releases are recorded as image digests in a
  manifest and validated on the deployment architecture.
- Recoverable state lives outside container layers, with a documented backup or
  rebuild path.
- One deploy workflow changes code only and never names a data source.
- Each release is compatible with the one before it; breaking changes ship as
  expand, migrate, and contract releases.
- Shared-environment data changes are stated in the PR and run as Dagster runs
  after deploy. Single-user local experiments follow ADR 0028; they authorize
  no remote or source-access changes.
- *(checked: `scripts/storage_assessment.py`)* Dagster storage dependency
  changes carry an updated `deploy/dagster/storage-upgrade.json` assessment.

## Environment isolation

[ADR 0006](docs/adrs/0006-isolate-development-and-production-environments.md).

- Local and ordinary CI commands default to fixtures.
- Live integration and deploy commands require an explicit target and never fall
  back to production.
- Each environment has its own credentials, writable targets, metadata store,
  ingestion state, caches, networks, and volumes. Development identities cannot
  write production.
- Logs include run and target identifiers and never secrets.

## Documentation and work tracking

- Follow the [delivery policy](docs/delivery-policy.md) for business outcomes,
  stage acceptance, active-work limits, simplicity, and issue structure.
  Technical requirements remain in these standards.
- Behavior, contract, or procedure changes update the owning documentation in
  the same PR.
- *(checked: ingestion, transformations and metrics package checks)* Every
  maintained data definition has a nonblank description beside its definition:
  raw tables and columns (including evidence tables), dbt layers, models,
  sources and output columns, and Cube public views, measures and dimensions.
  Describe meaning, not the name; measures state units and important
  exclusions. Checks cover disabled-by-default definitions. Checks enforce
  presence; review checks accuracy.
- Architectural decisions go in numbered ADRs; superseded decisions are marked,
  not edited away.
- Meaningful engineering changes (features, fixes, substantive refactors,
  infrastructure, and policy) have an issue in the project's tracker before
  work starts. Typos, formatting, and brief read-only exploration do not.
- One outcome per issue. The PR and the final handoff cite the issue.
- Do not put tracker identifiers in code, test names, environment variables or
  durable documentation. Describe the behavior instead; the tracker and pull
  requests hold the history.
- Shared agent instructions and skills live in `.agents/`
  ([repository instructions](.agents/AGENTS.md)).
