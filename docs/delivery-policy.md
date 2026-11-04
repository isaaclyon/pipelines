# Delivery policy

Deliver useful, trusted answers through ingestion, dbt, and Cube. About 20
reliable sources is a coverage target, not the definition of product success.
Deliver one business outcome through the required layers before expanding scope.

This policy owns delivery and acceptance. [Engineering standards](../engineering-standards.md)
own technical requirements. Use whichever issue tracker your team prefers;
GitHub Issues works well. "Issue" below means an item in that tracker.

## Plan around business questions

- Use a tracker project or milestone for a larger business initiative, not for each connector
  or technical layer.
- Use a parent issue for one useful outcome: a named user can answer a specific
  question and make a decision.
- Use sub-issues for independently verifiable work needed for that outcome.
  Small steps can remain a checklist; do not create tickets for every action.
- Track shared work once and link it as a dependency of each affected outcome.
  Reuse existing issues instead of duplicating them.
- Scope the required entities, history, refresh needs, models, and metrics before
  implementation. Do not ingest every table or expose every table in Cube by default.
- Keep fixes and necessary platform work as concrete issues. State the current
  delivery blocker or risk they address; do not invent a business parent.

Use this outline for an outcome issue:

```text
Question and intended user:
Decision this answer supports:
Named acceptance owner:
Included / excluded:
Required sources, entities, history, and freshness:
Required models and metrics:
Availability: ready by / time zone / reporting coverage / maximum data age:
Availability: access surface and environment / late-data behavior / later revisions:
Acceptance reference, comparison period, and agreed tolerance:
Acceptance checks and evidence:
Dependencies and implementation sub-issues:
```

Agree the scope and acceptance checks before work starts. Record acceptance
results in the issue, including the environment and any agreed limitations.
Do not treat development acceptance as production availability.

## Availability for every question

Every business-question specification and outcome issue has an explicit
availability section: when the user can rely on the answer, not merely when a
pipeline runs. Placeholders can say "Not yet agreed"; resolve the requirement
before implementation. Do not invent a default deadline or imply real-time data.

Record:

- Ready-by time or on-demand expectation, time zone, and applicable days.
- Reporting period and required coverage, plus the maximum acceptable data age.
- Where the user accesses the answer and in which environment.
- What the user sees when data is late, incomplete, or unavailable. Do not silently
  present stale or partial data as a complete answer.
- Whether later refunds, corrections, or late records revise prior answers, and
  the agreed update expectation.

For daily reporting, distinguish the deadline for yesterday's complete answer
from the age of the latest source changes it includes. For current inventory,
state a maximum data age rather than using "right now" without a measurable limit.
For monthly reporting, distinguish an initial answer from any later finalization.

Acceptance evidence checks the user-visible answer against this requirement.
A successful extraction or a recent load timestamp alone does not establish
availability. Follow the existing freshness ownership in engineering standards;
this section defines the business requirement, not a second scheduling policy.

## Definition of done

| Stage | Required evidence |
| --- | --- |
| Raw reliable | Required history and entities load; keys, updates, deletions, and coverage limits are defined; failed loads can recover; completeness and freshness evidence meet the agreed scope. |
| Modeled | Meaning, row grain, and keys are clear; required correctness checks pass; results reconcile to the agreed reference; validated outputs are published in the agreed environment. |
| Metrics available | Definitions are agreed; Cube queries return verified results; intended users have appropriate access; the agreed availability and freshness requirements are met. |
| Accepted | The named acceptance owner confirms that the answer supports the intended decision, with evidence linked in the issue. |

Apply the technical checks in engineering standards; these stages do not replace
or weaken them. A merged connector is not an accepted business outcome.

Under [ADR 0028](adrs/0028-develop-locally-before-shared-snowflake-release.md),
single-user outcomes can reach modeled, metrics-available and accepted stages in
an explicitly named local DuckDB/Cube environment. Record the local data copy,
coverage and user-visible evidence. Shared Snowflake parity, publication,
permissions and user readiness remain separate deferred release requirements;
never mark them passed from local checks. Owner-approved local acceptance does
not imply shared availability. Existing remote deployments must remain safe:
consumer merge holds remain until their default-off/local-only gate is implemented
and tested, not merely because the local-first policy is accepted.

A sub-issue can finish when its own acceptance checks pass. For example, an
ingestion sub-issue needs raw reliability, not a new metric. The parent remains
open until all required stages and business acceptance are complete. Mark a stage
not applicable only with a stated reason. Applicable PRs must merge before the
associated issue is Done; checked but unmerged work remains In Review.

## Keep focus

- Keep one business outcome in active delivery and one next outcome in preparation.
  Preparation covers access, source discovery, definitions, and acceptance scope,
  not a second implementation stream.
- Parallelize independent tasks within the active outcome and preparation work.
- New requests enter the backlog or explicitly replace a current priority.
  They do not silently expand active scope.
- When urgent reliability or security work interrupts delivery, record the reason,
  the paused outcome, and the condition for resuming it.
- Review weekly: what becomes usable, what blocks it, what stops or moves later,
  and whether the next outcome is ready.

## Keep the implementation simple

- Build only the data and metrics required for an agreed use case. Record a reason
  for exceptions, such as preserving history before a source removes it.
- Reuse the stage checklist and established source patterns. Keep source-specific
  exceptions local.
- Add shared abstractions only when repeated concrete needs justify them.
- Add infrastructure only for a current delivery blocker or a concrete risk.
- Give each metric one agreed definition and clear implementation ownership across
  dbt and Cube. Do not duplicate business rules across layers.
- Keep accepted architecture choices stable unless new evidence warrants review.
  Simplicity does not waive correctness, security, or recovery requirements.

## Track progress once

The issue tracker owns priorities, progress, blockers, and acceptance evidence. Repository
documentation owns durable behavior, contracts, and procedures. Do not maintain
parallel status reports in repository files or a spreadsheet.

Keep a source coverage table in one tracker document. Link to it from other
projects rather than copying it. Add known sources as their scope is assessed;
do not create placeholder tickets solely to reach 20.

| Source | Business outcome links | Now / next / later | Entities and history | Access ready | Raw reliable | Modeled | Metrics available | Owner and blocker |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |

Use separate stage fields because reliable ingestion does not imply usable
metrics. Link evidence to the relevant issues. Count each source system once for
coverage, not each table or connector job. State the accepted scope and any gaps.

At the weekly review, check reliable-source coverage against the target, accepted
business questions, and elapsed time from starting an outcome to acceptance.
Review freshness failures and manual recovery work alongside these indicators.
Do not use closed-ticket counts as a substitute for useful results.
