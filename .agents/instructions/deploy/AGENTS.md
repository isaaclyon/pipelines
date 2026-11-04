# Deployment agent instructions

Scope: `deploy/`. Inherit the root and parent-folder agent instructions.

Owns deployment configuration, container builds, and runtime topology.
Read the scoped guides and READMEs in `deploy/dagster/` and `deploy/compose/`.

Validate fully resolved configurations with `scripts/platform <target> config`.
Keep runtime and check stages separate, and verify runtime changes with local
startup and smoke checks.
