# Compose deployment agent instructions

Scope: `deploy/compose/`. Inherit the root and parent-folder agent instructions.

Owns container builds and development and production service wiring.
Start with `deploy/compose/README.md` and the relevant deployment ADRs.

Use `scripts/platform <target> config`, `up`, and `smoke` as applicable.
Resolve native Cube Store architecture in the dispatcher. Remote overrides
require explicit configuration and immutable application image digests.
