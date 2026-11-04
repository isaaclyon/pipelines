select
    shop_id,
    batch_id,
    max(case when field_group = 'profile' then status end)
        as profile_access_status,
    max(case when field_group = 'names' then status end)
        as names_access_status,
    max(case when field_group = 'email' then status end)
        as email_access_status,
    max(case when field_group = 'phone' then status end)
        as phone_access_status,
    max(case when field_group = 'whatsapp_consent' then status end)
        as whatsapp_consent_access_status,
    max(case when field_group = 'addresses' then status end)
        as addresses_access_status,
    max(case when field_group = 'summaries' then status end)
        as summaries_access_status,
    max(case when field_group = 'statistics' then status end)
        as statistics_access_status,
    max(case when field_group = 'tags' then status end)
        as tags_access_status,
    max(case when field_group = 'metafields' then status end)
        as metafields_access_status,
    max(case when field_group = 'tax_id' then status end)
        as tax_id_access_status,
    max(case when field_group = 'company_contacts' then status end)
        as company_contacts_access_status,
    max(case when field_group = 'external_identities' then status end)
        as external_identities_access_status
from {{ ref('stg_shopify_customers__field_access') }}
group by shop_id, batch_id
