with source as (
    select * from {{ source('shopify_customers', 'customers') }}
)

select
    shop_id,
    customer_id,
    legacy_id,
    first_name,
    last_name,
    display_name,
    locale,
    account_state,
    product_subscriber_status,
    customer_note,
    default_address_id,
    email_address,
    email_marketing_state,
    email_marketing_opt_in_level,
    email_marketing_source_location_id,
    email_open_tracking_level,
    phone_number,
    shopify_lifetime_spent_currency_code,
    shopify_last_order_id,
    shopify_predicted_spend_tier,
    shopify_rfm_group,
    tax_id,
    _batch_id as source_batch_id,
    _extraction_kind as source_extraction_kind,
    _query_version as source_query_version,
    _api_version as source_api_version,
    {{ utc_timestamp('created_at') }} as created_at,
    {{ utc_timestamp('updated_at') }} as updated_at,
    {{ utc_timestamp(
        'email_marketing_updated_at'
    ) }} as email_marketing_updated_at,
    {{ utc_timestamp('_observed_at') }} as source_observed_at,
    is_email_verified,
    is_data_sale_opted_out,
    is_tax_exempt,
    is_email_format_valid,
    cast(shopify_order_count as bigint) as shopify_order_count,
    shopify_lifetime_spent_amount,
    cast(_extraction_revision as bigint) as source_extraction_revision,
    sms_marketing_state,
    sms_marketing_opt_in_level,
    sms_marketing_collected_from,
    {{ utc_timestamp('sms_marketing_updated_at') }} as sms_marketing_updated_at,
    sms_marketing_source_location_id,
    whatsapp_marketing_state,
    whatsapp_marketing_opt_in_level,
    whatsapp_marketing_collected_from,
    {{ utc_timestamp(
        'whatsapp_marketing_updated_at'
    ) }} as whatsapp_marketing_updated_at,
    whatsapp_marketing_source_location_id
from source
