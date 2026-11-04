"""The production source persists only approved metafield values."""

from pathlib import Path

from analytics_ingestion.shopify.customer_source import CustomersRun, load_customers
from tests.customer_fakes import (
    FakeCustomerShop,
    address,
    customer,
    customer_settings,
    metafield,
)
from tests.shopify_fakes import at
from tests.test_customer_source import query


def test_approval_controls_values_and_inventory(tmp_path: Path) -> None:
    shop = FakeCustomerShop()
    shop.add(
        customer(1, metafields=[metafield(1, key="loyalty"), metafield(2, key="other")])
    )
    settings = customer_settings(tmp_path)
    start, end = at("2026-10-01T00:00:00Z"), at("2026-10-01T01:00:00Z")
    load_customers(
        settings, CustomersRun("default", "reextract", start, end), transport=shop
    )
    assert query(
        tmp_path, "select count(*) from shopify_customers_raw.customers__metafields"
    ) == [(0,)]
    load_customers(
        settings,
        CustomersRun("approved", "reextract", start, end),
        transport=shop,
        approved_metafields=frozenset({("custom", "loyalty")}),
    )
    assert query(
        tmp_path, "select key from shopify_customers_raw.customers__metafields"
    ) == [("loyalty",)]
    assert query(
        tmp_path,
        "select key, collected from "
        "shopify_customers_raw.customer_metafield_inventory "
        "where batch_id = 'approved' order by key",
    ) == [("loyalty", True), ("other", False)]


def test_parameterized_address_survives_load(tmp_path: Path) -> None:
    shop = FakeCustomerShop()
    item = address(1)
    address_id = "gid://shopify/MailingAddress/1?model_name=CustomerAddress"
    item["id"] = address_id
    shop.add(customer(1, addresses=[item]))
    report = load_customers(
        customer_settings(tmp_path),
        CustomersRun(
            "parameterized",
            "reextract",
            at("2026-10-01T00:00:00Z"),
            at("2026-10-01T01:00:00Z"),
        ),
        transport=shop,
    )
    assert report.rejects == 0
    assert query(
        tmp_path,
        "select address_id from shopify_customers_raw.customers__addresses",
    ) == [(address_id,)]
    assert query(
        tmp_path, "select default_address_id from shopify_customers_raw.customers"
    ) == [(address_id,)]
