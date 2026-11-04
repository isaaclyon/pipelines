{% set batches = source('shopify_orders', 'ingestion_batches') %}
{% set relation = adapter.get_relation(
    database=batches.database, schema=batches.schema, identifier=batches.identifier
) if execute else none %}

-- Initial datasets may have no evidence table. Absence is unknown, never a
-- successful empty window. A present but malformed table fails closed.
{% if relation is not none %}
with evidence as (
    select
        shop_id,
        batch_id,
        kind as batch_kind,
        window_field,
        {{ utc_timestamp('window_start') }} as window_started_at,
        {{ utc_timestamp('window_end') }} as window_ended_at,
        {{ utc_timestamp('observed_started_at') }} as observation_started_at,
        {{ utc_timestamp('observed_finished_at') }} as observation_finished_at,
        {{ utc_timestamp('completed_at') }} as completed_at,
        status,
        completeness,
        cast(exceptions as bigint) as exception_count,
        cast(orders_observed as bigint) as observed_order_count,
        cast(extraction_revision as bigint) as extraction_revision,
        query_version as source_query_version
    from {{ source('shopify_orders', 'ingestion_batches') }}
)

select
    *,
    coalesce(
        status = 'complete'
        and completeness = 'complete'
        and window_field = 'updated_at'
        and window_started_at < window_ended_at
        and observation_started_at <= observation_finished_at
        and observation_finished_at <= completed_at
        and window_ended_at <= observation_finished_at
        and exception_count = 0
        and observed_order_count >= 0
        and extraction_revision >= 3
        and length(trim(source_query_version)) > 0,
        false
    ) as is_coverage_verified
from evidence
{% else %}
    select
        cast(null as varchar) as shop_id,
        cast(null as varchar) as batch_id,
        cast(null as varchar) as batch_kind,
        cast(null as varchar) as window_field,
        cast(null as timestamp) as window_started_at,
        cast(null as timestamp) as window_ended_at,
        cast(null as timestamp) as observation_started_at,
        cast(null as timestamp) as observation_finished_at,
        cast(null as timestamp) as completed_at,
        cast(null as varchar) as status,
        cast(null as varchar) as completeness,
        cast(null as bigint) as exception_count,
        cast(null as bigint) as observed_order_count,
        cast(null as bigint) as extraction_revision,
        cast(null as varchar) as source_query_version,
        cast(false as boolean) as is_coverage_verified
    where false
{% endif %}
