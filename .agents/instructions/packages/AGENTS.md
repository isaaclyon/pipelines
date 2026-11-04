# Packages agent instructions

Scope: `packages/`. Inherit the root and parent-folder agent instructions.

Owns independently buildable platform components and their dependency boundaries.
Read each component's README and scoped guide. Keep dependency declarations,
lockfiles, and build environments owned by that component.

Run `scripts/platform local check [ingestion|transformations|metrics]` for
installed-package checks. Maintain independent locks; shared runtime versions
must pass `scripts/check-versions.sh`.
