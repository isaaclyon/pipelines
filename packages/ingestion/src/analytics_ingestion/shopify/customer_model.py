"""Raw customer contract: table and column declarations, and row parsing.

Physical tables in the customer dataset (``SHOPIFY_CUSTOMERS_DATASET``):

======================================  =====================================
Table                                   Content
======================================  =====================================
``customers``                           One row per (shop_id, customer_id)
``customers__tax_exemptions``           Source tax-exemption codes
``customers__addresses``                Every ``addressesV2`` address
``customers__tags``                     Tags, original values
``customers__metafields``               Accessible metafield values
``customers__company_contacts``         B2B contact/location links
``customer_batch_evidence``             One row per successful run
``customer_field_access``               Access result per field group per run
``customer_metafield_inventory``        Namespace/key counts per run, no values
``customer_source_status``              Availability per customer, no PII
``shopify_customer_rejects``            Customers that broke the contract
======================================  =====================================

Every child row carries ``shop_id`` and ``customer_id``; consumers never need
dlt's ``_dlt_*`` keys. Columns of a field group Shopify denied are NULL; the
``customer_field_access`` table distinguishes that from a source NULL.
"""

from __future__ import annotations

from analytics_ingestion.shopify.pipeline import Column

ROOT_TABLE = "customers"
REJECTS_TABLE = "shopify_customer_rejects"
EVIDENCE_TABLE = "customer_batch_evidence"
ACCESS_TABLE = "customer_field_access"
INVENTORY_TABLE = "customer_metafield_inventory"
STATUS_TABLE = "customer_source_status"

# Field groups whose access is checked on every run. ``profile`` is required.
FIELD_GROUPS = (
    "profile",
    "names",
    "email",
    "phone",
    "addresses",
    "summaries",
    "statistics",
    "tags",
    "metafields",
    "tax_id",
    "company_contacts",
    "external_identities",
)
EXCLUDED_GROUPS = frozenset({"external_identities"})
ACCESS_STATUSES = ("available", "access_denied", "unsupported", "excluded")
EXTRACTION_KINDS = ("refresh", "lookback", "reextract", "sweep")


def _t(name: str, description: str, nullable: bool = True) -> Column:
    return Column(name, "text", nullable, description=description)


def _ts(name: str, description: str, nullable: bool = True) -> Column:
    return Column(name, "timestamp", nullable, description=description)


def _b(name: str, description: str, nullable: bool = True) -> Column:
    return Column(name, "bool", nullable, description=description)


def _group(description: str, group: str) -> str:
    """Columns of an optional field group are null when it was not read."""
    return f"{description} Null unless the {group} field group was available."


def _consent(prefix: str, channel: str, group: str) -> list[Column]:
    return [
        _t(f"{prefix}_state", _group(f"{channel} marketing consent state.", group)),
        _t(
            f"{prefix}_opt_in_level",
            _group(f"{channel} opt-in level, such as SINGLE_OPT_IN.", group),
        ),
        _t(
            f"{prefix}_collected_from",
            _group(f"Where {channel} consent was collected, such as SHOPIFY.", group),
        ),
        _ts(
            f"{prefix}_updated_at",
            _group(f"UTC instant {channel} consent last changed.", group),
        ),
        _t(
            f"{prefix}_source_location_id",
            _group(f"Shopify location that collected {channel} consent.", group),
        ),
    ]


SHOP_ID = _t("shop_id", "Shopify global ID of the shop the row was read from.", False)
CUSTOMER_ID = _t("customer_id", "Shopify global ID of the customer.", False)
BATCH_ID = _t("batch_id", "Dagster run ID of the customer ingestion run.", False)

METADATA_COLUMNS = [
    _t("_batch_id", "Dagster run ID of the run that loaded the row.", False),
    _ts("_observed_at", "UTC instant ingestion read the customer.", False),
    _t(
        "_extraction_kind",
        "How the run selected the customer: refresh, lookback, reextract or sweep.",
        False,
    ),
    _t("_query_version", "Fingerprint of the GraphQL query documents used.", False),
    Column(
        "_extraction_revision",
        "bigint",
        False,
        description="Raw contract revision that parsed the customer.",
    ),
    _t("_api_version", "Shopify Admin API version that served the customer.", False),
]

CUSTOMER_COLUMNS = [
    SHOP_ID,
    CUSTOMER_ID,
    _t("legacy_id", "Numeric customer ID shown in Shopify admin URLs."),
    _t("first_name", _group("First name. Restricted personal data.", "names")),
    _t("last_name", _group("Last name. Restricted personal data.", "names")),
    _t("display_name", _group("Full display name. Restricted personal data.", "names")),
    _ts("created_at", "UTC instant Shopify created the customer.", False),
    _ts("updated_at", "UTC instant Shopify last changed the customer.", False),
    _t("locale", "Customer's preferred language, such as en or fr-CA."),
    _t("account_state", "Customer account state, such as ENABLED or DISABLED."),
    _b("is_email_verified", "Whether the customer verified their email address."),
    _t(
        "product_subscriber_status",
        "Product subscription status, such as NEVER_SUBSCRIBED.",
    ),
    _b(
        "is_data_sale_opted_out",
        "Whether the customer opted out of having their data sold or shared.",
    ),
    _b("is_tax_exempt", "Whether the customer is exempt from all taxes."),
    _t(
        "customer_note",
        _group("Staff note on the customer. Restricted personal data.", "names"),
    ),
    _t(
        "default_address_id",
        "Shopify ID of the default address in customers__addresses.",
    ),
    _t(
        "email_address",
        _group("Default email address. Restricted personal data.", "email"),
    ),
    _b(
        "is_email_format_valid",
        _group("Whether the default email address is well formed.", "email"),
    ),
    _t(
        "email_marketing_state",
        _group("Email marketing consent state, such as SUBSCRIBED.", "email"),
    ),
    _t(
        "email_marketing_opt_in_level",
        _group("Email opt-in level, such as CONFIRMED_OPT_IN.", "email"),
    ),
    _ts(
        "email_marketing_updated_at",
        _group("UTC instant email consent last changed.", "email"),
    ),
    _t(
        "email_marketing_source_location_id",
        _group("Shopify location that collected email consent.", "email"),
    ),
    _t(
        "email_open_tracking_level",
        _group("Whether email opens may be tracked, such as OPTED_IN.", "email"),
    ),
    _t(
        "phone_number",
        _group("Default phone number. Restricted personal data.", "phone"),
    ),
    *_consent("sms_marketing", "SMS", "phone"),
    *_consent("whatsapp_marketing", "WhatsApp", "whatsapp_consent"),
    Column(
        "shopify_order_count",
        "bigint",
        description=_group("Shopify's count of the customer's orders.", "summaries"),
    ),
    Column(
        "shopify_lifetime_spent_amount",
        "decimal",
        description=_group(
            "Shopify's lifetime spend in the customer's currency.", "summaries"
        ),
    ),
    _t(
        "shopify_lifetime_spent_currency_code",
        _group("Currency of shopify_lifetime_spent_amount.", "summaries"),
    ),
    _t(
        "shopify_last_order_id",
        _group("Shopify global ID of the customer's latest order.", "summaries"),
    ),
    _t(
        "shopify_predicted_spend_tier",
        _group("Shopify's predicted spend tier, such as HIGH.", "statistics"),
    ),
    _t(
        "shopify_rfm_group",
        _group("Shopify's recency-frequency-monetary segment.", "statistics"),
    ),
    _t("tax_id", _group("Customer tax registration ID.", "tax_id")),
    *METADATA_COLUMNS,
]

_KEYS = [SHOP_ID, CUSTOMER_ID]
_POSITION = Column(
    "position",
    "bigint",
    False,
    description="Zero-based position in the source list; not a durable source ID.",
)


def _address(name: str, description: str) -> Column:
    return _t(name, f"{description} Restricted personal data.")


# Child path under the customers root -> columns (physical ``customers__<path>``).
NESTED_TABLES: dict[tuple[str, ...], list[Column]] = {
    ("tax_exemptions",): [
        *_KEYS,
        _POSITION,
        _t("code", "Shopify tax exemption code.", False),
    ],
    ("addresses",): [
        *_KEYS,
        _t("address_id", "Shopify ID of the mailing address.", False),
        _b(
            "is_default_address",
            "Whether this is the customer's default address.",
            False,
        ),
        _address("first_name", "First name on the address."),
        _address("last_name", "Last name on the address."),
        _address("full_name", "Full name on the address."),
        _t("company", "Company name on the address."),
        _address("phone_number", "Phone number on the address."),
        _address("address_line_1", "First street line."),
        _address("address_line_2", "Second street line."),
        _t("postal_code", "Postal or ZIP code."),
        _t("city", "City."),
        _t("province_name", "Province or state name."),
        _t("province_code", "Province or state code."),
        _t("country_name", "Country name."),
        _t("country_code", "ISO 3166-1 alpha-2 country code."),
        _t("time_zone", "IANA time zone of the address."),
        Column(
            "latitude",
            "decimal",
            description="Latitude Shopify geocoded. Restricted personal data.",
        ),
        Column(
            "longitude",
            "decimal",
            description="Longitude Shopify geocoded. Restricted personal data.",
        ),
        _b("are_coordinates_validated", "Whether Shopify validated the coordinates."),
        _t(
            "address_validation_status",
            "Shopify address validation summary, such as NO_ISSUES.",
        ),
    ],
    ("tags",): [*_KEYS, _POSITION, _t("tag", "Customer tag as entered.", False)],
    ("metafields",): [
        *_KEYS,
        _t("metafield_id", "Shopify global ID of the metafield.", False),
        _t("namespace", "Metafield namespace.", False),
        _t("key", "Metafield key within its namespace.", False),
        _t("type", "Metafield value type, such as single_line_text_field.", False),
        _t("value", "Metafield value as Shopify serializes it."),
        _ts("created_at", "UTC instant the metafield was created."),
        _ts("updated_at", "UTC instant the metafield last changed."),
    ],
    ("company_contacts",): [
        *_KEYS,
        _t(
            "company_contact_id", "Shopify global ID of the B2B company contact.", False
        ),
        _t("company_id", "Shopify global ID of the contact's company.", False),
        # Empty string when the contact has no role assignment, so the
        # composite grain (contact, location) never contains a NULL key.
        _t(
            "company_location_id",
            "Company location the contact is assigned to; empty without one.",
            False,
        ),
        _b("is_main_contact", "Whether the contact is the company's main contact."),
        _t("title", "Contact's job title."),
    ],
}

REJECT_COLUMNS = [
    SHOP_ID,
    CUSTOMER_ID,
    _ts("updated_at", "Shopify updated_at of the rejected customer version.", False),
    _t("run_id", "Dagster run ID of the run that rejected the version.", False),
    _t("error", "Why the version failed the field contract.", False),
    _ts("rejected_at", "UTC instant the run recorded the reject.", False),
]
EVIDENCE_COLUMNS = [
    SHOP_ID,
    BATCH_ID,
    _t("kind", "Run kind: refresh, lookback, reextract or sweep.", False),
    _t("window_field", "Customer timestamp the window filters: updated_at.", False),
    _ts("window_start", "Inclusive UTC start of the covered window.", False),
    _ts(
        "window_end",
        "Exclusive UTC end of the covered window; freshness reads it.",
        False,
    ),
    _t("status", "Run status; complete, since only successful runs write rows.", False),
    _t(
        "completeness",
        "complete, or partial when customers changed during the read and were skipped.",
        False,
    ),
    _b("is_full_sweep", "Whether the run read every customer.", False),
    _ts("observed_started_at", "UTC instant extraction started."),
    _ts("observed_finished_at", "UTC instant extraction finished."),
    _ts("completed_at", "UTC instant the run recorded this evidence after its load."),
    Column(
        "customers_observed",
        "bigint",
        description="Complete customers the run read and loaded.",
    ),
    Column(
        "rejects",
        "bigint",
        description="Customers still rejected after the run, including older rejects.",
    ),
    Column("pages", "bigint", description="Source pages the run read."),
    _t("load_ids", "Comma-separated dlt load IDs the run committed."),
    _t("api_version", "Shopify Admin API version the run used.", False),
    _t("query_version", "Fingerprint of the GraphQL query documents used.", False),
    Column(
        "extraction_revision",
        "bigint",
        description="Raw contract revision of the run's parser.",
    ),
]
ACCESS_COLUMNS = [
    SHOP_ID,
    BATCH_ID,
    _t("field_group", "Customer field group, such as email or addresses.", False),
    _t(
        "status",
        "Access result: available, access_denied, unsupported or excluded.",
        False,
    ),
    _ts("checked_at", "UTC instant the run checked access.", False),
]
INVENTORY_COLUMNS = [
    SHOP_ID,
    BATCH_ID,
    _t("namespace", "Metafield namespace seen on customers.", False),
    _t("key", "Metafield key within its namespace.", False),
    _t("type", "Metafield value type.", False),
    Column(
        "customers",
        "bigint",
        False,
        description="Customers the run saw with this metafield.",
    ),
    _b("collected", "Whether the run kept this metafield's values.", False),
]
STATUS_COLUMNS = [
    SHOP_ID,
    CUSTOMER_ID,
    _t(
        "status",
        "confirmed_unavailable, unknown (a direct read returned it) or available.",
        False,
    ),
    _ts("checked_at", "UTC instant the reconciliation recorded the status.", False),
    _t("batch_id", "Dagster run ID of the reconciliation run.", False),
    _t("detail", "Evidence for the status, such as the direct-read result.", False),
]

DATASET_DESCRIPTION = (
    "Raw current Shopify customer profiles and complete child collections, "
    "with run, access and availability evidence. Restricted personal data."
)
# Physical table -> meaning. Columns are described where they are declared.
TABLE_DESCRIPTIONS = {
    ROOT_TABLE: "One row per (shop_id, customer_id): the latest complete profile.",
    "customers__tax_exemptions": "One row per tax exemption code, by position.",
    "customers__addresses": "One row per customer address, replaced with the customer.",
    "customers__tags": "One row per customer tag, by position.",
    "customers__metafields": (
        "One row per customer metafield whose namespace and key are approved."
    ),
    "customers__company_contacts": (
        "One row per (B2B company contact, location) assignment."
    ),
    REJECTS_TABLE: "One row per customer version that failed the field contract.",
    EVIDENCE_TABLE: (
        "One row per successful customer run, written after its customers load."
    ),
    ACCESS_TABLE: "One row per field group per run: whether Shopify granted it.",
    INVENTORY_TABLE: (
        "One row per metafield namespace, key and type seen in a run; no values."
    ),
    STATUS_TABLE: (
        "One row per customer the sweep reconciliation found missing or back. "
        "Holds no personal data."
    ),
}


# Namespace prefixes never collected: they can hold credentials or tokens.
EXCLUDED_METAFIELD_PREFIXES = ("secret", "$app:", "multipass", "password", "token")


ApprovedMetafields = frozenset[tuple[str, str]]


def metafield_is_collected(
    namespace: str,
    key: str,
    approved: ApprovedMetafields = frozenset(),
) -> bool:
    """Fail closed: only an exact approved (namespace, key) pair is collected.

    The credential prefix denylist applies even to an approved pair.
    """
    if namespace.lower().startswith(EXCLUDED_METAFIELD_PREFIXES):
        return False
    return (namespace, key) in approved
