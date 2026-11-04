# 0036: Upgrade clients one at a time on pinned toolkit versions

Date: 2026-11-04

## Status

Accepted by the owner on 2026-11-04.
Extends [0005](0005-promote-immutable-release-images.md). Replaces the
reference to [0020](0020-keep-consecutive-releases-compatible.md) in
[0028](0028-develop-locally-before-shared-snowflake-release.md); 0020 stays
deprecated.

## Context

Under [0030](0030-ship-a-toolkit-with-one-deployment-per-client.md) one toolkit
serves many clients. A bad toolkit release could break every client at once if
they all followed the latest version.

0020 required every release to work alongside its predecessor, so rollback was
always safe during zero-downtime deployments. That rule makes every removal a
multi-release process. Small brands can accept a short maintenance window, and
one operator controls when each client upgrades.

## Decision

Release the toolkit as numbered versions of immutable images, built and tested
under 0005. Each client repository pins one toolkit version.

To upgrade a client:

1. Bump the pinned version in the client repository.
2. Run the client's checks locally, then against the client's development target.
3. Stop scheduled runs, deploy the new version and run any data change it needs.
4. Resume schedules and confirm the next run succeeds.

Upgrade one client at a time. Rollback redeploys the previous pinned version.
Data written by the newer version is not guaranteed to work with the older one;
the release notes say when a version changes stored data, and those upgrades fix
forward instead of rolling back.

Releases need not stay compatible with their predecessor. Each release note
lists breaking changes and the steps a client upgrade needs.

## Consequences

- A faulty release reaches one client before it reaches the others.
- Breaking changes ship in one release instead of three.
- Upgrades include a short window without scheduled runs.
- Clients can fall behind. The operator must track each client's version and
  keep upgrade notes usable across several versions.
