"""Source-shaped synthetic customer fixtures; no live personal data."""

from decimal import Decimal
from pathlib import Path

import duckdb

CUSTOMER_TABLES: dict[str, dict[str, str]] = {
    "customers": {
        "shop_id": "varchar",
        "customer_id": "varchar",
        "legacy_id": "varchar",
        "first_name": "varchar",
        "last_name": "varchar",
        "display_name": "varchar",
        "locale": "varchar",
        "account_state": "varchar",
        "product_subscriber_status": "varchar",
        "customer_note": "varchar",
        "default_address_id": "varchar",
        "email_address": "varchar",
        "email_marketing_state": "varchar",
        "email_marketing_opt_in_level": "varchar",
        "email_marketing_source_location_id": "varchar",
        "email_open_tracking_level": "varchar",
        "phone_number": "varchar",
        "shopify_lifetime_spent_currency_code": "varchar",
        "shopify_last_order_id": "varchar",
        "shopify_predicted_spend_tier": "varchar",
        "shopify_rfm_group": "varchar",
        "tax_id": "varchar",
        "_batch_id": "varchar",
        "_extraction_kind": "varchar",
        "_query_version": "varchar",
        "_api_version": "varchar",
        "created_at": "timestamptz",
        "updated_at": "timestamptz",
        "email_marketing_updated_at": "timestamptz",
        "_observed_at": "timestamptz",
        "is_email_verified": "boolean",
        "is_data_sale_opted_out": "boolean",
        "is_tax_exempt": "boolean",
        "is_email_format_valid": "boolean",
        "shopify_order_count": "bigint",
        "shopify_lifetime_spent_amount": "decimal(38, 9)",
        "_extraction_revision": "bigint",
        "sms_marketing_state": "varchar",
        "sms_marketing_opt_in_level": "varchar",
        "sms_marketing_collected_from": "varchar",
        "sms_marketing_updated_at": "timestamptz",
        "sms_marketing_source_location_id": "varchar",
        "whatsapp_marketing_state": "varchar",
        "whatsapp_marketing_opt_in_level": "varchar",
        "whatsapp_marketing_collected_from": "varchar",
        "whatsapp_marketing_updated_at": "timestamptz",
        "whatsapp_marketing_source_location_id": "varchar",
    },
    "customers__addresses": {
        "shop_id": "varchar",
        "customer_id": "varchar",
        "address_id": "varchar",
        "first_name": "varchar",
        "last_name": "varchar",
        "full_name": "varchar",
        "company": "varchar",
        "phone_number": "varchar",
        "address_line_1": "varchar",
        "address_line_2": "varchar",
        "postal_code": "varchar",
        "city": "varchar",
        "province_name": "varchar",
        "province_code": "varchar",
        "country_name": "varchar",
        "country_code": "varchar",
        "time_zone": "varchar",
        "address_validation_status": "varchar",
        "is_default_address": "boolean",
        "are_coordinates_validated": "boolean",
        "latitude": "decimal(38, 9)",
        "longitude": "decimal(38, 9)",
    },
    "customers__tags": {
        "shop_id": "varchar",
        "customer_id": "varchar",
        "position": "bigint",
        "tag": "varchar",
    },
    "customers__tax_exemptions": {
        "shop_id": "varchar",
        "customer_id": "varchar",
        "position": "bigint",
        "code": "varchar",
    },
    "customers__metafields": {
        "shop_id": "varchar",
        "customer_id": "varchar",
        "metafield_id": "varchar",
        "namespace": "varchar",
        "key": "varchar",
        "type": "varchar",
        "value": "varchar",
        "created_at": "timestamptz",
        "updated_at": "timestamptz",
    },
    "customers__company_contacts": {
        "shop_id": "varchar",
        "customer_id": "varchar",
        "company_contact_id": "varchar",
        "company_id": "varchar",
        "company_location_id": "varchar",
        "is_main_contact": "boolean",
        "title": "varchar",
        "_dlt_list_idx": "bigint",
    },
    "customer_field_access": {
        "shop_id": "varchar",
        "batch_id": "varchar",
        "field_group": "varchar",
        "status": "varchar",
        "checked_at": "timestamptz",
    },
}
FIELD_GROUPS = (
    "profile",
    "names",
    "email",
    "phone",
    "whatsapp_consent",
    "addresses",
    "summaries",
    "statistics",
    "tags",
    "metafields",
    "tax_id",
    "company_contacts",
    "external_identities",
)


def write_customers(path: Path) -> None:
    with duckdb.connect(str(path)) as db:
        db.execute("create schema raw_customers")
        for table, columns in CUSTOMER_TABLES.items():
            db.execute(
                f"create table raw_customers.{table} ("
                + ", ".join(f'"{name}" {kind}' for name, kind in columns.items())
                + ")"
            )
        for shop, customer, count in (
            ("shop-1", "customer-1", 2),
            ("shop-1", "customer-2", 0),
            ("shop-2", "customer-1", 1),
        ):
            row: dict[str, object] = {
                "shop_id": shop,
                "customer_id": customer,
                "legacy_id": "123",
                "first_name": "Synthetic",
                "last_name": "Customer",
                "display_name": "Synthetic Customer",
                "email_address": "fixture@example.com",
                "created_at": "2026-10-01 00:00:00+00",
                "updated_at": "2026-10-17 00:00:00+00",
                "_observed_at": "2026-10-18 00:00:00+00",
                "_batch_id": "batch-1",
                "_extraction_kind": "sweep",
                "_api_version": "2026-10",
                "_query_version": "customers-v1",
                "_extraction_revision": 1,
                "locale": "en-US",
                "account_state": "ENABLED",
                "is_email_verified": True,
                "is_data_sale_opted_out": False,
                "is_tax_exempt": False,
                "is_email_format_valid": True,
                "shopify_order_count": count,
                "shopify_lifetime_spent_amount": Decimal("12.123456789"),
                "shopify_lifetime_spent_currency_code": "USD",
                "default_address_id": "address-1",
            }
            columns = CUSTOMER_TABLES["customers"]
            db.execute(
                "insert into raw_customers.customers values ("
                + ", ".join("?" for _ in columns)
                + ")",
                [row.get(name) for name in columns],
            )
            for group in FIELD_GROUPS:
                db.execute(
                    "insert into raw_customers.customer_field_access values (?, ?, "
                    "?, 'available', '2026-10-18 00:00:00+00')",
                    [shop, "batch-1", group],
                ) if customer == "customer-1" else None
            db.execute(
                "insert into raw_customers.customers__addresses (shop_id, "
                "customer_id, address_id, is_default_address, country_name, "
                "country_code, province_name, province_code, time_zone, city, "
                "postal_code, address_line_1) values (?, ?, 'address-1', true, "
                "'United States', 'US', 'Utah', 'UT', 'America/Denver', "
                "'Salt Lake City', '84101', '1 Synthetic Street')",
                [shop, customer],
            )
            db.execute(
                "insert into raw_customers.customers__tags values (?, ?, 0, "
                "'synthetic-tag')",
                [shop, customer],
            )
            db.execute(
                "insert into raw_customers.customers__tax_exemptions values (?, ?, "
                "0, 'CA_STATUS_CARD_EXEMPTION')",
                [shop, customer],
            )
            db.execute(
                "insert into raw_customers.customers__metafields values (?, ?, "
                "'meta-1', 'custom', 'preference', 'single_line_text_field', "
                "'synthetic', '2026-10-01 00:00:00+00', '2026-10-17 00:00:00+00')",
                [shop, customer],
            )
            db.execute(
                "insert into raw_customers.customers__company_contacts values (?, "
                "?, 'contact-1', 'company-1', 'location-1', true, 'Buyer', 0)",
                [shop, customer],
            )
        db.execute("checkpoint")
