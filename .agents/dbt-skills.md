# dbt skills

Vendored from [dbt-labs/dbt-agent-skills](https://github.com/dbt-labs/dbt-agent-skills)
at revision `168a2b0b92da59be88866257140907c206ff0e44`.
Each installed directory includes the upstream Apache-2.0 license.

Installed skills:

- `using-dbt-for-analytics-engineering`
- `running-dbt-commands`
- `adding-dbt-unit-test`
- `fetching-dbt-docs`
- `maintaining-dbt-documentation`
- `working-with-dbt-model-governance`

The first five come from `skills/dbt/skills/<skill-name>/` and have local
single-project dbt Core guidance. Their governance references use the local name.
The governance skill is adapted from upstream `working-with-dbt-mesh`: its entry
point is rewritten for Core and its three governance guides retain the applicable
single-project guidance. Cross-project setup instructions are omitted.

These are maintained local copies. Review upstream changes before updating;
preserve the Core scope and local governance name. Canonical files live in
`.agents/skills/`; `.pi/skills` and `.claude/skills` link to that directory.
