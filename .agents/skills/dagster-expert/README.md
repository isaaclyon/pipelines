# Local Dagster skill

Vendored from [dagster-io/skills](https://github.com/dagster-io/skills),
`plugins/dagster/skills/dagster-expert`, on the `release-stable` branch at
commit `b08dd8e6dac65829c829f2a185ff9b6eb2c11ab3` (2026-10-07).
The upstream Apache-2.0 license is included in [LICENSE](LICENSE).

This copy provides guidance for open-source Dagster and local `dg` commands.
It retains dbt Core guidance and only the integration references this repository
uses: dbt, dlt, Docker, DuckDB, Postgres, and Snowflake. Other upstream
integration references were removed on 2026-10-13. Hosted orchestration, hosted API and MCP tools,
managed deployment configuration, hosted-only asset selection filters, and
dbt hosted-service guidance have been removed. Mixed reference files were
adapted, and one upstream CLI link was corrected. Modified files carry notices.

Edit this copy in `.agents/skills/dagster-expert/`. The existing `.pi/skills`
and `.claude/skills` symlinks expose it to Pi and Claude; Codex discovers it
directly. When updating from upstream, reapply these changes and check all local
reference links before replacing the installed copy.
