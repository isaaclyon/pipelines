# 0018: Manage environment secrets with 1Password

Date: 2026-10-08

## Status

Superseded on 2026-11-04 by [0032](0032-read-secrets-from-environment-files.md)
before acceptance. Previously proposed.

## Context

The platform needs source credentials, Snowflake credentials and private keys,
service passwords, and API secrets across development and production. Managing
these independently in developer files, VM configuration, and CI invites drift
and makes rotation difficult to understand.

Developers should be able to run repeatedly without authenticating to 1Password
on every command. VM workloads also need predictable startup and operation when
1Password is temporarily unavailable. The existing deployment interface accepts
an environment file and host paths for key files, but secret retrieval and refresh
are not implemented.

Local copies reduce runtime dependencies but introduce plaintext files and a lag
between a central update and its use by an application. The platform needs an
explicit owner and refresh lifecycle for those copies.

## Decision

Use 1Password as the authoritative store and management surface for platform
environment secrets. Store application credentials there and render the required
values into environment-specific files through an explicit setup or refresh step.
Use the 1Password CLI's secret references and template rendering for this workflow.

Version templates containing references and ordinary configuration. Treat rendered
environment files and private-key files as disposable local copies. Update secrets
in 1Password and regenerate the copies; manual edits are not a second source of
truth. Keep non-secret configuration under normal repository and deployment control.

### Development and runtime consumption

- Developers authenticate with their own 1Password identity and render only the
  development credentials they are authorized to use. Repeated local commands
  consume those files without contacting 1Password. Offline fixture checks require
  neither credentials nor a 1Password session.
- VM setup and refresh use an environment-specific 1Password service account with
  read access only to the required vaults. Provision application secret files on
  the host, then supply each workload only its required values or read-only files.
  Ordinary application runs do not retrieve secrets from 1Password.
- Live CI and deployment automation follow the same environment boundaries and
  use narrowly scoped automation identities. Rendered CI files are temporary and
  are removed after use; ordinary fixture CI remains independent of 1Password.
- Preserve the explicit target selection and isolated identities required by
  [0006](0006-isolate-development-and-production-environments.md). Development
  rendering must not silently select production references.

### Bootstrap authentication

The VM's **1Password service-account token** authenticates secret retrieval. It is
distinct from an application credential, such as a Snowflake service user's
private key, that retrieval produces.

Provision the token through a protected host bootstrap mechanism, accessible only
to the identity performing refresh. Supply it to the CLI during that operation;
do not distribute it to application containers or generated application environment
files. Its initial delivery cannot depend on authenticating with that same token.
The exact host bootstrap mechanism and token rotation procedure must be specified
before unattended deployment is implemented.

### File and refresh lifecycle

Keep rendered files outside source and build contexts where practical; any files
inside a checkout must be explicitly ignored by Git and container build inputs.
Restrict directories and files to the owning identity and required runtime readers.
Never include rendered values in logs, configuration previews, CI artifacts, or
container images. Preserve private-key formatting and verify that rendered values
are parsed correctly by their actual consumer.

A refresh must validate the complete required output before replacing the active
files. Failed retrieval or validation leaves the previous files intact and reports
failure; it must not deploy partially refreshed credentials. Record the target and
last successful refresh time without secret values.

Refresh explicitly during setup, credential rotation, and deployments that need
updated credentials. Recreate or reload affected workloads so they consume the new
values. Define any additional scheduled refresh only when an operational need
exists. Existing workloads may continue using their last rendered credentials
while those credentials remain valid; a failed refresh must not be reported as a
successful rotation.

Rotation includes updating the issuing system, updating 1Password, refreshing
consumers, and retiring old credentials in an order supported by that system.
Revocation at the issuing system remains necessary when immediate loss of access
is required. Removing access to 1Password alone does not invalidate copies already
retrieved. Remove local copies when environments or hosts are retired.

## Consequences

- People manage secrets through one surface, with explicit environment access.
  Repository templates describe what a workload requires without containing values.
- Local iteration and application startup use prepared files, reducing repeated
  authentication and runtime dependence on 1Password availability.
- Hosts and developer machines hold sensitive material on disk. File permissions,
  build exclusions, cleanup, and rotation are part of the operating procedure.
- Secret changes do not propagate automatically to running applications. Refresh
  and consumer reload are observable operations that must be exercised in the
  platform spike.
- Service-account bootstrap remains a small external trust requirement. The host
  mechanism, vault layout, file paths, and refresh command remain implementation
  work; this proposal does not establish that they already exist.
- Fetching secrets on every invocation would reduce persistent copies but add an
  authentication and availability dependency to routine work. Manually maintained
  environment files would avoid retrieval tooling but retain multiple management
  surfaces. Explicit rendering accepts local copies to simplify normal operation.

References verified on 2026-10-08:

- [1Password configuration-file rendering](https://www.1password.dev/cli/secrets-config-files)
- [Secret references](https://www.1password.dev/cli/secret-references)
- [Service-account permissions](https://www.1password.dev/cli/reference/management-commands/service-account)
