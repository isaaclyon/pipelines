# Custom Python sources

Read the [source](https://dlthub.com/docs/general-usage/source),
[resource](https://dlthub.com/docs/general-usage/resource), and
[advanced REST API](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/advanced) docs before
choosing the adapter's interface.

## Keep the adapter small

Group related extraction resources with `@dlt.source` and expose independently
loadable entities with `@dlt.resource`. Make configuration and extraction ranges
explicit. Use dlt's loading, incremental, HTTP, and pagination facilities where
they fit; write custom code for provider behavior they do not cover.

Yield bounded batches or streamed records. Keep import-time code free of network
requests and loads. Separate source construction from execution so fixtures can
exercise the same maintained path without credentials.

Follow [ADR 0016](../../../../docs/adrs/0016-parse-data-at-trust-boundaries.md):
parse configuration, IDs used in requests, pagination tokens, cursor timestamps,
and bulk-job states before application code relies on them. Confine untyped
library responses to the boundary adapter. Preserve source-shaped payloads and
unknown fields; establish business-facing shapes in dbt staging.

## GraphQL and asynchronous bulk exports

Use custom resources when query construction, nested pagination, asynchronous
jobs, or provider errors need explicit control.

- A GraphQL HTTP 200 can contain errors or incomplete data. Inspect both the
  transport result and the provider's GraphQL error contract.
- Follow each connection's cursor. Fetching the top-level orders connection
  does not guarantee complete line items, refunds, or other nested connections.
- Handle provider cost budgets and throttling with bounded retries and pacing.
- For bulk jobs, represent pending, running, completed, failed, and cancelled
  states explicitly. Persist job identity when retries must resume polling.
- Require an expected result artifact or a documented valid empty result.
  Failed jobs or missing downloads must not become successful zero-row loads.
- Stream downloads and parse JSONL or other export formats incrementally.
  Check parent/child association and the provider's current grouping format.

For Shopify, verify the current Admin API version, authentication model,
connection behavior, and bulk-operation limits in official provider docs.
Assess verified-source compatibility before reuse; a catalog entry alone is
insufficient evidence. Avoid freezing changing API limits into this skill.

## Tests

Use fixtures that exercise provider-specific behavior: multiple nested pages,
partial GraphQL errors, failed bulk jobs, empty valid exports, interrupted
downloads, and parent/child association. Assert expected source records and
destination behavior, then add reconciliation for critical orders, refunds,
and transactions. Keep live provider checks separate and explicitly targeted.
