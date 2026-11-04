# 0032: Read secrets from environment files

Date: 2026-11-04

## Status

Accepted by the owner on 2026-11-04.
Supersedes [0018](0018-manage-environment-secrets-with-1password.md) and the
developer-credentials rule in [0029](0029-load-every-source-into-local-duckdb.md).

## Context

0018 proposed 1Password as the platform's secret store, with templates and
render steps in the repository. The toolkit now serves many brands
([0030](0030-ship-a-toolkit-with-one-deployment-per-client.md)). Requiring one
paid secret manager, or shipping its templates, ties every client to a vendor
choice the toolkit does not need to make.

The containers already read settings from environment variables and mounted
files. How those values are produced is the operator's concern.

## Decision

The toolkit reads secrets only from an environment file and, where a value
cannot fit in one variable, from mounted read-only files such as private keys.
The toolkit documents every variable and file each service needs.

The repository contains no secret-manager templates, commands or dependencies.
An operator may produce the environment file however they like: by hand, from
1Password, from a cloud secret store or from any other tool.

Keep these rules from 0018 and 0029:

- Environment files and key files live outside the checkout and build contexts
  where practical, are ignored by Git and container builds, and are readable
  only by their owner and required services.
- Secret values never appear in logs, images, configuration previews or CI
  artifacts.
- Each target has its own file; nothing falls back to production credentials.
- Offline checks need no secrets. The `ci` target rejects live credentials.

## Consequences

- No client must buy or use a particular secret manager.
- `deploy/secrets/` and the 1Password instructions leave the repository.
- Rotation, refresh and audit history become the operator's responsibility.
  The toolkit cannot detect a stale secret before a service uses it.
- The toolkit must validate that required variables are present at startup and
  fail with a clear message that names the missing variable, never its value.
