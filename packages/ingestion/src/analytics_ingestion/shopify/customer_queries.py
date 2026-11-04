"""Exact Admin GraphQL documents for the raw customer source.

Selections are grouped by field group so access can be probed and recorded per
group. Changing a fragment changes ``CUSTOMER_QUERY_VERSION`` automatically.
"""

from __future__ import annotations

import hashlib

from analytics_ingestion.shopify.customer_model import FIELD_GROUPS
from analytics_ingestion.shopify.queries import API_VERSION, PAGE_INFO

CUSTOMER_PAGE_SIZE = 50
ADDRESS_FIRST_PAGE = 10
ADDRESS_FOLLOW_PAGE = 50
METAFIELD_FIRST_PAGE = 25
METAFIELD_FOLLOW_PAGE = 100
ROLE_FIRST_PAGE = 10
ROLE_FOLLOW_PAGE = 50
IDS_PER_READ = 100

ADDRESS_FIELDS = (
    "id firstName lastName name company phone address1 address2 zip city province "
    "provinceCode country countryCodeV2 timeZone latitude longitude "
    "coordinatesValidated validationResultSummary"
)
METAFIELD_FIELDS = "id namespace key type value createdAt updatedAt"
_CONSENT = "state optInLevel collectedFrom updatedAt sourceLocation { id }"

# Required group: a failure here fails the run.
PROFILE_FRAGMENT = (
    "id legacyResourceId createdAt updatedAt locale state verifiedEmail "
    "productSubscriberStatus dataSaleOptOut taxExempt taxExemptions "
    "defaultAddress { id }"
)

GROUP_FRAGMENTS: dict[str, str] = {
    "names": "firstName lastName displayName note",
    "email": (
        "defaultEmailAddress { emailAddress validFormat marketingState "
        "marketingOptInLevel marketingUpdatedAt sourceLocation { id } "
        "openTrackingLevel }"
    ),
    "phone": (
        f"defaultPhoneNumber {{ phoneNumber smsMarketingConsent {{ {_CONSENT} }} }}"
    ),
    "whatsapp_consent": (
        f"defaultPhoneNumber {{ whatsAppMarketingConsent {{ {_CONSENT} }} }}"
    ),
    "addresses": (
        f"addressesV2(first: {ADDRESS_FIRST_PAGE}) {{ nodes {{ {ADDRESS_FIELDS} }} "
        f"{PAGE_INFO} }}"
    ),
    "summaries": "numberOfOrders amountSpent { amount currencyCode } lastOrder { id }",
    "statistics": "statistics { predictedSpendTier rfmGroup }",
    "tags": "tags",
    "metafields": (
        f"metafields(first: {METAFIELD_FIRST_PAGE}) {{ nodes {{ {METAFIELD_FIELDS} }} "
        f"{PAGE_INFO} }}"
    ),
    "tax_id": "taxSettings { taxId }",
    "company_contacts": (
        "companyContactProfiles { id title isMainContact company { id } "
        f"roleAssignments(first: {ROLE_FIRST_PAGE}) {{ nodes {{ "
        f"companyLocation {{ id }} }} {PAGE_INFO} }} }}"
    ),
}
# Groups that are selected in the customer query, in selection order. The
# ``whatsapp_consent`` check group merges into ``defaultPhoneNumber``.
PROBED_GROUPS = tuple(GROUP_FRAGMENTS)
ALL_RECORDED_GROUPS = (*FIELD_GROUPS, "whatsapp_consent")

SHOP_IDENTITY = (
    "query { shop { id myshopifyDomain } currentAppInstallation { "
    "accessScopes { handle } } }"
)


def customer_fragment(groups: frozenset[str]) -> str:
    parts = [PROFILE_FRAGMENT]
    phone = [g for g in ("phone", "whatsapp_consent") if g in groups]
    for name, fragment in GROUP_FRAGMENTS.items():
        if name in ("phone", "whatsapp_consent"):
            continue
        if name in groups:
            parts.append(fragment)
    if phone:
        inner: list[str] = []
        if "phone" in phone:
            inner.append(f"phoneNumber smsMarketingConsent {{ {_CONSENT} }}")
        if "whatsapp_consent" in phone:
            inner.append(f"whatsAppMarketingConsent {{ {_CONSENT} }}")
        parts.append(f"defaultPhoneNumber {{ {' '.join(inner)} }}")
    return " ".join(parts)


def customers_page_query(groups: frozenset[str]) -> str:
    return (
        "query($search: String, $after: String) { customers("
        f"first: {CUSTOMER_PAGE_SIZE}, after: $after, query: $search, "
        "sortKey: UPDATED_AT) { nodes { "
        + customer_fragment(groups)
        + f" }} {PAGE_INFO} }} }}"
    )


def probe_query(group: str) -> str:
    return (
        "query { customers(first: 1) { nodes { id " + GROUP_FRAGMENTS[group] + " } } }"
    )


def address_page_query() -> str:
    return (
        "query($id: ID!, $after: String) { node(id: $id) { id ... on Customer { "
        f"addressesV2(first: {ADDRESS_FOLLOW_PAGE}, after: $after) {{ nodes {{ "
        f"{ADDRESS_FIELDS} }} {PAGE_INFO} }} }} }} }}"
    )


def metafield_page_query() -> str:
    return (
        "query($id: ID!, $after: String) { node(id: $id) { id ... on Customer { "
        f"metafields(first: {METAFIELD_FOLLOW_PAGE}, after: $after) {{ nodes {{ "
        f"{METAFIELD_FIELDS} }} {PAGE_INFO} }} }} }} }}"
    )


def role_page_query() -> str:
    return (
        "query($id: ID!, $after: String) { node(id: $id) { id ... on CompanyContact { "
        f"roleAssignments(first: {ROLE_FOLLOW_PAGE}, after: $after) {{ nodes {{ "
        f"companyLocation {{ id }} }} {PAGE_INFO} }} }} }} }}"
    )


# Direct read used to confirm that a customer still exists.
CUSTOMER_PRESENCE = (
    "query($ids: [ID!]!) { nodes(ids: $ids) { ... on Customer { id updatedAt } } }"
)


def query_version() -> str:
    material = "|".join(
        [
            API_VERSION,
            PROFILE_FRAGMENT,
            *(f"{name}={fragment}" for name, fragment in GROUP_FRAGMENTS.items()),
            str(CUSTOMER_PAGE_SIZE),
        ]
    )
    return hashlib.sha256(material.encode()).hexdigest()[:12]


CUSTOMER_QUERY_VERSION = query_version()
