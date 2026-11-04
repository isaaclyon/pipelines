# Documentation agent instructions

Scope: `docs/`. Inherit the root and parent-folder agent instructions.

Owns architecture decisions and implementation specifications. Verify
implementation claims against the repository.

- `adrs/`: sequentially numbered ADRs and their index. Follow the conventions in
  `docs/adrs/README.md` and use the repository ADR skill.
- `specs/`: implementation specifications. Link each to the relevant accepted
  ADRs and acceptance checks.
- When a specification is superseded, delete it or reduce it to what stays in
  force, and update inbound links. Git history keeps the old text.
