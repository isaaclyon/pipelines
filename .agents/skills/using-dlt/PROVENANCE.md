# Provenance and maintenance

This is a maintained local adaptation of the workflow and reference structure in
[Untitled Data Company's dlt skill](https://github.com/untitled-data-company/data-skills/tree/639351f1838d3e2a5e625d2b1290753ed9313cf2/skills/dlt-skill),
reviewed at revision `639351f1838d3e2a5e625d2b1290753ed9313cf2` on 2026-10-07.
The upstream MIT notice is preserved in [LICENSE](LICENSE).

The local entry point and references are rewritten for the accepted dlt,
Dagster, dbt Core, Snowflake, and DuckDB architecture. They use the repository's
container execution, independent uv projects, fixture-first testing, typed
boundaries, environment isolation, and state recovery requirements. Upstream
installation scripts, templates, dashboard requirements, and code examples are
not vendored.

The dltHub `create-rest-api-pipeline` skill was evaluated at harness revision
`c3f4f7803498bb3ab65502ed77bf8959fe5e5d1e`. Its
[license](https://github.com/dlt-hub/dlthub-ai-harness/blob/c3f4f7803498bb3ab65502ed77bf8959fe5e5d1e/LICENSE)
restricts use to dltHub Services under an agreement and explicitly excludes
third-party runtimes. Its materials are not incorporated into this skill.
Technical guidance here is grounded in public dlt documentation and local
engineering requirements.

Review upstream updates deliberately. Verify API details against the owning
package's pinned dlt version and current authoritative provider docs. Keep
orchestration guidance in `dagster-expert` and warehouse transformation guidance
in the dbt skills. Add source-specific references only for maintained connectors
that need them. Canonical files live in `.agents/skills/using-dlt/`; the existing
`.pi/skills` and `.claude/skills` symlinks expose the same files.
