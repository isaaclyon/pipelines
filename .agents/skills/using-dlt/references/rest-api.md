# REST API sources

Read the [REST API source docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/basic)
and the provider's endpoint reference before changing configuration. Confirm
which options the pinned dlt version supports.

## Configuration

Use `RESTAPIConfig` to describe the client, resource defaults, and resources.
Keep the base URL, headers, and authentication at the appropriate scope. Put
endpoint-specific paths, parameters, selectors, pagination, and incremental
behavior in the endpoint configuration. Put primary keys and write disposition
on the resource or resource defaults.

- Select the actual record list with `data_selector`; do not load the response
  wrapper as if it were a record.
- Configure the documented paginator explicitly: next-link, cursor, offset,
  page number, or a genuinely single-page response. Verify termination and the
  meaning of empty pages rather than assuming every API behaves the same way.
- Check endpoint-specific auth scopes, API versions, limits, and filters.
- Use dlt's HTTP helpers for supported retry behavior. Verify bounded retries
  and provider-specific handling of 429 responses and rate-limit headers.
- Do not convert unexpected authorization, transport, or response errors into
  empty datasets. Distinguish a documented missing child from a failed request.

## Dependent resources

Declare relationships explicitly. A child path can refer to a parent field,
such as `posts/{resources.posts.id}/comments`. A bare placeholder such as
`posts/{id}/comments` needs a corresponding resolver; it does not establish a
parent relationship by itself.

Verify parent keys, child keys, and any fields carried from the parent. Test
multiple parents and a paginated child. Parent pagination and child pagination
are separate completeness requirements. A parent update cursor does not prove
that independently changed child records will be fetched.

## Incremental parameters

Use the documented endpoint `incremental` configuration and reference its
`{incremental.start_value}` in the provider's query parameter where appropriate.
The cursor path refers to a field in each selected record. Verify the provider's
time format, filtering inclusivity, ordering, and support for updates.

Read [incremental loading and state](incremental-state.md) before choosing
lookback, deduplication, or early termination.

## Meaningful checks

- A multi-page fixture loads every expected ID exactly as intended.
- Empty final pages or absent next links terminate without dropping valid data.
- Multiple parents produce the right children and provenance.
- Rate limits retry within the configured budget; permanent failures remain
  visible and cannot masquerade as a complete extraction.
- Repeat runs and updated records have the expected destination behavior.
