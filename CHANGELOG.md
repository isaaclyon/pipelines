# Changelog

Each toolkit version is one set of images built from one commit by
`./scripts/platform local release` ([ADR 0036](docs/adrs/0036-upgrade-clients-one-at-a-time-on-pinned-toolkit-versions.md)).
The release command refuses a version without a section here. Each section
lists breaking changes and the steps a client upgrade needs.

## 0.1.0

First toolkit version: the toolkit and its clients are separate.

- Each client keeps one `client.yaml` that turns on sources and sets
  schedules, the reporting time zone, the warehouse and Slack alerts.
- A client's dbt project installs the toolkit's dbt project as the
  `analytics_toolkit` package and adds its own models on top.
- A client's Cube models sit in their own folder beside the toolkit's.
- Client images build on the pinned toolkit images.
- Every source reads its credentials and settings from one folder of JSON
  files, one per system: `shopify.json` (orders and customers),
  `loop_returns.json`, `goodday.json`, `google_sheets.json` and
  `impact_radius.json`.

**Upgrade steps:** none. This is the first version.
