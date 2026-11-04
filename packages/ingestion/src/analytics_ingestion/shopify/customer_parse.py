"""Parse one complete source customer into raw rows.

A field of a group that was not selected (denied or unsupported) is NULL; the
selected groups must return every selected field, so a missing value raises.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation

from analytics_ingestion.shopify.customer_model import (
    ApprovedMetafields,
    metafield_is_collected,
)
from analytics_ingestion.shopify.errors import ContractError
from analytics_ingestion.shopify.jsonvalue import (
    DECIMAL_SCALE,
    JsonObject,
    as_list,
    as_object,
    gid,
    optional_instant,
    optional_ref,
    optional_str,
    parse_decimal,
    required_instant,
    required_str,
    selected_value,
)
from analytics_ingestion.shopify.pipeline import Row

# Shopify documents MailingAddress IDs with an optional model_name parameter.
_ADDRESS_GID = re.compile(
    r"gid://shopify/MailingAddress/\d+(?:\?model_name=CustomerAddress)?"
)


def address_gid(value: object, field: str) -> str:
    """Validate a MailingAddress ID, keeping the documented parameter."""
    if not isinstance(value, str) or not _ADDRESS_GID.fullmatch(value):
        raise ContractError(f"Expected MailingAddress ID: {field}")
    return value


def _default_address_id(node: JsonObject) -> str | None:
    value = selected_value(node, "defaultAddress")
    if value is None:
        return None
    return address_gid(as_object(value, "defaultAddress").get("id"), "defaultAddress")


@dataclass(frozen=True)
class ParsedCustomer:
    customer_id: str
    updated_at: datetime
    content: Row


def _bool_or_none(data: JsonObject, field: str) -> bool | None:
    value = selected_value(data, field)
    if value is None:
        return None
    if not isinstance(value, bool):
        raise ContractError(f"Expected boolean or null: {field}")
    return value


def _coordinate(data: JsonObject, field: str) -> Decimal | None:
    value = selected_value(data, field)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ContractError(f"Expected number or null: {field}")
    try:
        return round(Decimal(str(value)), DECIMAL_SCALE)
    except InvalidOperation:
        raise ContractError(f"Invalid coordinate: {field}") from None


def _consent(prefix: str, node: JsonObject | None) -> Row:
    names = (
        "state",
        "opt_in_level",
        "collected_from",
        "updated_at",
        "source_location_id",
    )
    if node is None:
        return {f"{prefix}_{name}": None for name in names}
    return {
        f"{prefix}_state": optional_str(node, "state"),
        f"{prefix}_opt_in_level": optional_str(node, "optInLevel"),
        f"{prefix}_collected_from": optional_str(node, "collectedFrom"),
        f"{prefix}_updated_at": optional_instant(node, "updatedAt"),
        f"{prefix}_source_location_id": optional_ref(
            node, "sourceLocation", "Location"
        ),
    }


def _object_or_none(data: JsonObject, field: str) -> JsonObject | None:
    value = selected_value(data, field)
    return None if value is None else as_object(value, field)


def parse_address(
    shop_id: str, customer_id: str, node: JsonObject, default: str | None
) -> Row:
    address_id = address_gid(node.get("id"), "address.id")
    validated = _bool_or_none(node, "coordinatesValidated")
    return {
        "shop_id": shop_id,
        "customer_id": customer_id,
        "address_id": address_id,
        "is_default_address": address_id == default,
        "first_name": optional_str(node, "firstName"),
        "last_name": optional_str(node, "lastName"),
        "full_name": optional_str(node, "name"),
        "company": optional_str(node, "company"),
        "phone_number": optional_str(node, "phone"),
        "address_line_1": optional_str(node, "address1"),
        "address_line_2": optional_str(node, "address2"),
        "postal_code": optional_str(node, "zip"),
        "city": optional_str(node, "city"),
        "province_name": optional_str(node, "province"),
        "province_code": optional_str(node, "provinceCode"),
        "country_name": optional_str(node, "country"),
        "country_code": optional_str(node, "countryCodeV2"),
        "time_zone": optional_str(node, "timeZone"),
        "latitude": _coordinate(node, "latitude"),
        "longitude": _coordinate(node, "longitude"),
        "are_coordinates_validated": validated,
        "address_validation_status": optional_str(node, "validationResultSummary"),
    }


def parse_metafield(shop_id: str, customer_id: str, node: JsonObject) -> Row:
    return {
        "shop_id": shop_id,
        "customer_id": customer_id,
        "metafield_id": gid(node.get("id"), "Metafield", "metafield.id"),
        "namespace": required_str(node, "namespace"),
        "key": required_str(node, "key"),
        "type": required_str(node, "type"),
        "value": optional_str(node, "value"),
        "created_at": optional_instant(node, "createdAt"),
        "updated_at": optional_instant(node, "updatedAt"),
    }


def parse_contact(
    shop_id: str,
    customer_id: str,
    node: JsonObject,
    locations: Sequence[JsonObject],
) -> list[Row]:
    contact_id = gid(node.get("id"), "CompanyContact", "contact.id")
    company = as_object(node.get("company"), "company")
    company_id = gid(company.get("id"), "Company", "company.id")
    location_ids = sorted(
        {
            gid(
                as_object(item.get("companyLocation"), "location").get("id"),
                "CompanyLocation",
                "location.id",
            )
            for item in locations
            if item.get("companyLocation") is not None
        }
    )
    return [
        {
            "shop_id": shop_id,
            "customer_id": customer_id,
            "company_contact_id": contact_id,
            "company_id": company_id,
            "company_location_id": location,
            "is_main_contact": _bool_or_none(node, "isMainContact"),
            "title": optional_str(node, "title"),
        }
        for location in location_ids or [""]
    ]


def parse_customer(
    shop_id: str,
    node: JsonObject,
    groups: frozenset[str],
    addresses: Sequence[JsonObject],
    metafields: Sequence[JsonObject],
    contacts: Sequence[tuple[JsonObject, list[JsonObject]]],
    approved_metafields: ApprovedMetafields = frozenset(),
) -> ParsedCustomer:
    """Content for one customer. ``groups`` are the groups the query selected.

    Only metafields whose (namespace, key) is in ``approved_metafields`` keep
    their values.
    """
    customer_id = gid(node.get("id"), "Customer", "customer.id")
    updated_at = required_instant(node, "updatedAt")
    default_address = _default_address_id(node)
    exemptions = [
        {
            "shop_id": shop_id,
            "customer_id": customer_id,
            "position": index,
            "code": str(code),
        }
        for index, code in enumerate(
            as_list(node.get("taxExemptions"), "taxExemptions")
        )
    ]
    row: Row = {
        "shop_id": shop_id,
        "customer_id": customer_id,
        "legacy_id": required_str(node, "legacyResourceId"),
        "created_at": required_instant(node, "createdAt"),
        "updated_at": updated_at,
        "locale": optional_str(node, "locale"),
        "account_state": optional_str(node, "state"),
        "is_email_verified": _bool_or_none(node, "verifiedEmail"),
        "product_subscriber_status": optional_str(node, "productSubscriberStatus"),
        "is_data_sale_opted_out": _bool_or_none(node, "dataSaleOptOut"),
        "is_tax_exempt": _bool_or_none(node, "taxExempt"),
        "default_address_id": default_address,
        "first_name": None,
        "last_name": None,
        "display_name": None,
        "customer_note": None,
        "email_address": None,
        "is_email_format_valid": None,
        "email_marketing_state": None,
        "email_marketing_opt_in_level": None,
        "email_marketing_updated_at": None,
        "email_marketing_source_location_id": None,
        "email_open_tracking_level": None,
        "phone_number": None,
        **_consent("sms_marketing", None),
        **_consent("whatsapp_marketing", None),
        "shopify_order_count": None,
        "shopify_lifetime_spent_amount": None,
        "shopify_lifetime_spent_currency_code": None,
        "shopify_last_order_id": None,
        "shopify_predicted_spend_tier": None,
        "shopify_rfm_group": None,
        "tax_id": None,
        "tax_exemptions": exemptions,
        "addresses": [],
        "tags": [],
        "metafields": [],
        "company_contacts": [],
    }
    if "names" in groups:
        row["first_name"] = optional_str(node, "firstName")
        row["last_name"] = optional_str(node, "lastName")
        row["display_name"] = optional_str(node, "displayName")
        row["customer_note"] = optional_str(node, "note")
    if "email" in groups:
        email = _object_or_none(node, "defaultEmailAddress")
        if email is not None:
            row["email_address"] = optional_str(email, "emailAddress")
            row["is_email_format_valid"] = _bool_or_none(email, "validFormat")
            row["email_marketing_state"] = optional_str(email, "marketingState")
            row["email_marketing_opt_in_level"] = optional_str(
                email, "marketingOptInLevel"
            )
            row["email_marketing_updated_at"] = optional_instant(
                email, "marketingUpdatedAt"
            )
            row["email_marketing_source_location_id"] = optional_ref(
                email, "sourceLocation", "Location"
            )
            row["email_open_tracking_level"] = optional_str(email, "openTrackingLevel")
    if groups & {"phone", "whatsapp_consent"}:
        phone = _object_or_none(node, "defaultPhoneNumber")
        if phone is not None:
            if "phone" in groups:
                row["phone_number"] = optional_str(phone, "phoneNumber")
                row.update(
                    _consent(
                        "sms_marketing", _object_or_none(phone, "smsMarketingConsent")
                    )
                )
            if "whatsapp_consent" in groups:
                row.update(
                    _consent(
                        "whatsapp_marketing",
                        _object_or_none(phone, "whatsAppMarketingConsent"),
                    )
                )
    if "summaries" in groups:
        count = optional_str(node, "numberOfOrders")
        if count is not None and not count.isdigit():
            raise ContractError("Expected unsigned integer: numberOfOrders")
        row["shopify_order_count"] = None if count is None else int(count)
        spent = _object_or_none(node, "amountSpent")
        if spent is not None:
            row["shopify_lifetime_spent_amount"] = parse_decimal(
                spent.get("amount"), "amountSpent.amount"
            )
            row["shopify_lifetime_spent_currency_code"] = required_str(
                spent, "currencyCode"
            )
        row["shopify_last_order_id"] = optional_ref(node, "lastOrder", "Order")
    if "statistics" in groups:
        stats = _object_or_none(node, "statistics")
        if stats is not None:
            row["shopify_predicted_spend_tier"] = optional_str(
                stats, "predictedSpendTier"
            )
            row["shopify_rfm_group"] = optional_str(stats, "rfmGroup")
    if "tax_id" in groups:
        settings = _object_or_none(node, "taxSettings")
        if settings is not None:
            row["tax_id"] = optional_str(settings, "taxId")
    if "tags" in groups:
        tags = as_list(node.get("tags"), "tags")
        if not all(isinstance(tag, str) for tag in tags):
            raise ContractError("Expected string tags")
        row["tags"] = [
            {
                "shop_id": shop_id,
                "customer_id": customer_id,
                "position": index,
                "tag": str(tag),
            }
            for index, tag in enumerate(tags)
        ]
    if "addresses" in groups:
        parsed = [
            parse_address(shop_id, customer_id, a, default_address) for a in addresses
        ]
        if default_address is not None and not any(
            a["is_default_address"] for a in parsed
        ):
            raise ContractError("Default address is missing from addresses")
        row["addresses"] = parsed
    if "metafields" in groups:
        row["metafields"] = [
            parse_metafield(shop_id, customer_id, m)
            for m in metafields
            if metafield_is_collected(
                required_str(m, "namespace"),
                required_str(m, "key"),
                approved_metafields,
            )
        ]
    if "company_contacts" in groups:
        row["company_contacts"] = [
            item
            for contact, locations in contacts
            for item in parse_contact(shop_id, customer_id, contact, locations)
        ]
    return ParsedCustomer(customer_id, updated_at, row)
