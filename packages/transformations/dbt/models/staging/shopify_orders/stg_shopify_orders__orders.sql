with source as (
    select * from {{ source('shopify_orders', 'orders') }}
),

renamed as (
    select
        shop_id,
        order_id,
        name as order_name,
        customer_id,
        {{ utc_timestamp('created_at') }} as created_at,
        {{ utc_timestamp('processed_at') }} as processed_at,
        {{ utc_timestamp('updated_at') }} as updated_at,
        {{ utc_timestamp('cancelled_at') }} as cancelled_at,
        {{ utc_timestamp('closed_at') }} as closed_at,
        -- Shopify can omit processed_at; order_date then uses created_at and
        -- order_date_source records the fallback.
        {{ utc_timestamp('coalesce(processed_at, created_at)') }} as ordered_at,
        case
            when processed_at is null then 'created_at' else 'processed_at'
        end as order_date_source,
        {{ local_date(
            utc_timestamp('coalesce(processed_at, created_at)'),
            reporting_time_zone()
        ) }} as order_date,
        cancel_reason,
        test as is_test,
        source_name,
        display_financial_status as financial_status,
        display_fulfillment_status as fulfillment_status,
        taxes_included as is_tax_included,
        currency_code as shop_currency_code,
        presentment_currency_code,
        total_price_shop_amount,
        total_price_presentment_amount,
        current_total_price_shop_amount,
        current_total_price_presentment_amount,
        original_total_price_shop_amount,
        original_total_price_presentment_amount,
        subtotal_price_shop_amount,
        subtotal_price_presentment_amount,
        current_subtotal_price_shop_amount,
        current_subtotal_price_presentment_amount,
        total_discounts_shop_amount,
        total_discounts_presentment_amount,
        total_tax_shop_amount,
        total_tax_presentment_amount,
        current_total_tax_shop_amount,
        current_total_tax_presentment_amount,
        total_shipping_price_shop_amount,
        total_shipping_price_presentment_amount,
        total_refunded_shop_amount,
        total_refunded_presentment_amount,
        email as order_email,
        phone as order_phone,
        billing_address_first_name,
        billing_address_last_name,
        billing_address_company,
        billing_address_address1,
        billing_address_address2,
        billing_address_city,
        billing_address_province,
        billing_address_province_code,
        billing_address_zip,
        billing_address_country,
        billing_address_country_code,
        billing_address_phone,
        shipping_address_first_name,
        shipping_address_last_name,
        shipping_address_company,
        shipping_address_address1,
        shipping_address_address2,
        shipping_address_city,
        shipping_address_province,
        shipping_address_province_code,
        shipping_address_zip,
        shipping_address_country,
        shipping_address_country_code,
        shipping_address_phone,
        {{ utc_timestamp('_observed_at') }} as source_observed_at,
        _query_version as source_query_version,
        cast(transactions_count as bigint) as transactions_count,
        transactions_count_precision,
        -- Null warehouse revisions mean the original extraction contract
        -- (revision 1), which did not select tags, discount codes or email.
        cast(coalesce(_extraction_revision, 1) as bigint) as extraction_revision
    from source
)

select * from renamed
