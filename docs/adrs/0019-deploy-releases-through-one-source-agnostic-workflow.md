# 0019: Deploy releases through one source-agnostic workflow

Date: 2026-10-09

## Status

Deprecated on 2026-11-03. This repository now ships only the local and CI
targets, and its remote release workflow was removed. The record stays as history.
Previously accepted.
Amended 2026-10-09 after review of the deploy workflow spec: the remote
smoke check is read-only, and development may skip superseded manifests.
Clarified 2026-10-14 by [ADR 0023](0023-maintain-dagster-storage-in-an-operator-window.md):
Dagster storage maintenance uses a separate operator window and the same deploy
lock. Ordinary deployment enforces the assessed release hold and storage baseline.

## Context

The platform has no remote deploy workflow yet. [ADR 0005](0005-promote-immutable-release-images.md)
defines what a release is: CI-built images identified by digest and recorded in
one release manifest. [ADR 0006](0006-isolate-development-and-production-environments.md)
separates development and production, and
[ADR 0004](0004-launch-dagster-runs-in-isolated-containers.md) runs each Dagster
run in its own container. None of them says what a deployment does, how it is
verified, or what it must leave alone.

A previous platform shows the cost of leaving that open. Its September 2026
deployment retrospective found that deploy runtime
code grew from 4,754 to 22,979 lines in 30 days, fail-closed checks grew from 92
to 893, and 39 releases in two weeks needed a person to recover them. The causes
it identified were:

- Each data source added its own deploy-time route mechanism and contract
  fingerprint, so adding a source changed the deploy and every change to a shared
  file could block another source.
- Many checks could fire only against production state. Dev route state differed
  for three of four live sources, so at least 12 of the 39 recoveries were
  failures that CI and dev could not catch.
- Warehouse migrations ran inside the release path and were blocked by source
  guards.
- A check that failed after services stopped left production Dagster down for 44
  and 6 minutes, and a rollback failed against data the newer release had written.

The retrospective also found that none of the 579 pull requests merged in 30 days
had a human review, and that two people changing one shared release path at the
same time blocked each other. This platform has a single owner, with
AI agents writing much of the code at a high change rate. One Docker Compose stack runs per environment on a VM
([ADR 0002](0002-operate-services-with-docker-compose.md)).

## Decision

Deploy every remote environment with one workflow whose only job is to change
which release is running. It takes two inputs: the target environment and a
release manifest.

### Fixed steps

The workflow runs the same steps in development and production:

1. Take the environment's deploy lock. Only one deployment runs per environment
   at a time; a second request waits or fails.
2. Validate the manifest and render the environment configuration
   ([ADR 0018](0018-manage-environment-secrets-with-1password.md)). Every image
   must be pinned by digest. A missing or invalid value stops the deployment
   before any running service changes.
3. Pull the release images.
4. Switch the Compose services to the release.
5. Verify the environment.
6. Record the manifest as the environment's current release and keep the
   previous manifest.

If verification fails, the workflow redeploys the previous manifest with the same
steps and reports the failure. Rolling back is a normal deployment of an older
manifest.

### Verification

Verification is fixed and identical in every remote environment: services report
healthy, every code location loads, schedules and sensors are in the state
recorded for that environment, and the smoke check passes. In remote
environments the smoke check is read-only: it confirms that services answer and
their definitions load, and it does not launch runs or write data. Local and CI
smoke checks may launch runs against fixtures. Every deployment check
must be runnable in local or CI and in remote development before it runs in
production. A check that can only fire against production state is not allowed.

Data correctness is verified continuously by pipeline checks, data tests, and
freshness checks. The deployment does not check data.

### Scope

- Deployment logic, meaning the workflow steps, scripts, and verification, does
  not name, branch on, or fingerprint a data source, dataset, or model. Adding a
  source changes service configuration such as credentials and environment
  variables, and never the deployment logic. Source-specific safety belongs to
  the pipeline as runtime checks and recorded evidence
  ([ADR 0016](0016-parse-data-at-trust-boundaries.md),
  [ADR 0017](0017-ingest-data-with-dlt.md)).
- The deployment does not change warehouse data or schemas.
  [ADR 0021](0021-run-data-changes-as-orchestrated-pipeline-work.md) defines
  where those changes run.
- The deployment does not wait for, stop, or cancel Dagster runs. Runs already in
  progress continue on their own image; runs launched after the switch use the
  new release. Runs must tolerate interruption and retry (ADR 0004). Images stay
  available while runs use them (ADR 0005).

### Promotion

CI publishes a release manifest for each change merged to `main`. Remote
development deploys the newest manifest automatically and may skip a manifest
that a newer one superseded before its deployment started
([ADR 0020](0020-keep-consecutive-releases-compatible.md)). Production deploys only a
manifest that has been deployed and verified in development, after a person
approves it. Production promotion runs the same workflow with a different target.

Changes to the deployment workflow are small pull requests that the platform
owner reads and approves before merge. Agents may prepare and merge them only
after that approval.

## Consequences

- Adding or changing a data source never requires a deployment change, so one
  source cannot block another source's release.
- Deployments are short and uniform, and a failure in development predicts a
  failure in production.
- Verification is shallow by design. A release that passes it can still produce
  wrong data, which pipeline and freshness checks must detect after deployment.
- Concerns that the previous platform enforced at deploy time, such as source
  route state and published provenance, must be expressed as runtime checks owned
  by the pipeline or service.
- Rollback depends on releases staying compatible with each other, which
  [ADR 0020](0020-keep-consecutive-releases-compatible.md) requires.
- The lock and the current and previous manifests need a durable home per
  environment. Automatic rollback needs tests for a failed health check and a
  failed configuration render before it is relied on.
- With one owner, GitHub cannot require a second approver, so review depends on
  the owner's explicit approval. The fitness checks below catch the regressions
  the retrospective found most often without needing a reviewer.
- Growth in deployment code is a review signal. A change that adds source or
  data awareness to the deployment contradicts this record and needs a new ADR.

### Fitness checks

- `scripts/test_deploy_contracts.py` checks that the platform commands, Dagster
  control-plane configuration, and CI workflows name no ingestion source, and
  that the remote `up` command rejects images without a digest. Compose service
  configuration may name sources because it carries their credentials.
- The deployment workflow adds offline tests for the lock, the rollback after failed verification, and
  the stop before service changes when configuration is invalid.
