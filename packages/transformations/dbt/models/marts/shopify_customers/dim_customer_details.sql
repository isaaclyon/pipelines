with customers as (
    select * from {{ ref('stg_shopify_customers__customers') }}
),

access as (
    select * from {{ ref('stg_shopify_customers__profile_access') }}
)

select
    customers.shop_id as shopify_shop_id,
    customers.customer_id as shopify_customer_id,
    customers.legacy_id as shopify_customer_legacy_id,
    customers.first_name,
    customers.last_name,
    customers.display_name,
    customers.locale,
    customers.account_state,
    customers.product_subscriber_status,
    customers.customer_note,
    customers.default_address_id as shopify_default_address_id,
    customers.email_address,
    customers.email_marketing_state,
    customers.email_marketing_opt_in_level,
    customers.email_marketing_source_location_id
        as shopify_email_marketing_source_location_id,
    customers.email_open_tracking_level,
    customers.phone_number,
    customers.shopify_lifetime_spent_currency_code,
    customers.shopify_last_order_id,
    customers.shopify_predicted_spend_tier,
    customers.shopify_rfm_group,
    customers.tax_id as shopify_tax_id,
    customers.created_at,
    customers.updated_at,
    customers.email_marketing_updated_at,
    customers.source_observed_at,
    customers.is_email_verified,
    customers.is_data_sale_opted_out,
    customers.is_tax_exempt,
    customers.is_email_format_valid,
    customers.shopify_order_count,
    customers.shopify_lifetime_spent_amount,
    customers.sms_marketing_state,
    customers.sms_marketing_opt_in_level,
    customers.sms_marketing_collected_from,
    customers.sms_marketing_updated_at,
    customers.sms_marketing_source_location_id
        as shopify_sms_marketing_source_location_id,
    customers.whatsapp_marketing_state,
    customers.whatsapp_marketing_opt_in_level,
    customers.whatsapp_marketing_collected_from,
    customers.whatsapp_marketing_updated_at,
    customers.whatsapp_marketing_source_location_id
        as shopify_whatsapp_marketing_source_location_id,
    coalesce(access.profile_access_status, 'unknown') as profile_access_status,
    coalesce(access.names_access_status, 'unknown') as names_access_status,
    coalesce(access.email_access_status, 'unknown') as email_access_status,
    coalesce(access.phone_access_status, 'unknown') as phone_access_status,
    coalesce(access.whatsapp_consent_access_status, 'unknown')
        as whatsapp_consent_access_status,
    coalesce(access.addresses_access_status, 'unknown')
        as addresses_access_status,
    coalesce(access.summaries_access_status, 'unknown')
        as summaries_access_status,
    coalesce(access.statistics_access_status, 'unknown')
        as statistics_access_status,
    coalesce(access.tags_access_status, 'unknown') as tags_access_status,
    coalesce(access.metafields_access_status, 'unknown')
        as metafields_access_status,
    coalesce(access.tax_id_access_status, 'unknown') as tax_id_access_status,
    coalesce(access.company_contacts_access_status, 'unknown')
        as company_contacts_access_status,
    coalesce(access.external_identities_access_status, 'unknown')
        as external_identities_access_status
from customers
left join access
    on
        customers.shop_id = access.shop_id
        and customers.source_batch_id = access.batch_id
