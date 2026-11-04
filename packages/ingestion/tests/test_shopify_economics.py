"""Order economics evidence must survive parsing into the declared rows."""

from decimal import Decimal

import pytest

from analytics_ingestion.shopify.jsonvalue import JsonObject, objects
from analytics_ingestion.shopify.model import (
    NESTED_TABLES,
    ORDER_COLUMNS,
    OrderException,
    ParsedOrder,
    RefundChildren,
    parse_order,
)
from tests.shopify_fakes import (
    SHOP,
    FakeShop,
    line,
    money,
    order,
    refund,
    transaction,
)


def test_classification_and_original_money_are_preserved() -> None:
    source = order(1)
    source.update(
        tags=["Replacement", "Wholesale"],
        discountCodes=["WELCOME"],
        email=None,
        phone=None,
        billingAddress=None,
        shippingAddress=None,
        taxesIncluded=True,
        originalTotalPriceSet=money("12.123456789"),
        totalTaxSet=money("2.00"),
        totalShippingPriceSet=money("0.00"),
        taxLines=[],
        discountApplications=[],
        shippingLines=[],
        agreements=[],
    )
    item = line(1)
    item.update(
        title="Blanket",
        variantTitle=None,
        isGiftCard=False,
        discountAllocations=[],
        taxLines=[],
    )
    parsed = parse_order(SHOP, source, [item], {})
    assert isinstance(parsed, ParsedOrder)
    assert parsed.content["original_total_price_shop_amount"] == Decimal("12.123456789")
    assert parsed.content["taxes_included"] is True
    assert parsed.content["tags"] == [
        {"shop_id": SHOP, "order_id": source["id"], "tag": "Replacement"},
        {"shop_id": SHOP, "order_id": source["id"], "tag": "Wholesale"},
    ]


def test_missing_economics_is_not_an_empty_complete_collection() -> None:
    source = order(1)
    del source["email"]
    parsed = parse_order(SHOP, source, [line(1)], {})
    assert isinstance(parsed, OrderException)


def tax() -> JsonObject:
    return {
        "title": "Sales tax",
        "rate": 0.075,
        "channelLiable": None,
        "priceSet": money("0.75"),
    }


def discount(index: int) -> JsonObject:
    return {
        "__typename": "DiscountCodeApplication",
        "index": index,
        "code": f"CODE{index}",
        "allocationMethod": "ACROSS",
        "targetSelection": "ALL",
        "targetType": "LINE_ITEM",
        "value": {"__typename": "PricingPercentageValue", "percentage": 10.0},
    }


def shipping(index: int) -> JsonObject:
    return {
        "id": f"gid://shopify/ShippingLine/{index}",
        "title": "Standard",
        "code": None,
        "source": "shopify",
        "isRemoved": index == 0,
        "originalPriceSet": money("5.00"),
        "discountedPriceSet": money("4.00"),
        "currentDiscountedPriceSet": money("0.00" if index == 0 else "4.00"),
        "discountAllocations": [
            {
                "allocatedAmountSet": money("1.00"),
                "discountApplication": {"index": index},
            }
        ],
        "taxLines": [tax()],
    }


def sale(index: int) -> JsonObject:
    return {
        "__typename": "ProductSale",
        "id": f"gid://shopify/Sale/{index}",
        "actionType": "ORDER",
        "lineType": "PRODUCT",
        "quantity": 1,
        "lineItem": {"id": "gid://shopify/LineItem/1"},
        "totalAmount": money("9.75"),
        "totalDiscountAmountAfterTaxes": money("0.00"),
        "totalDiscountAmountBeforeTaxes": money("1.00"),
        "totalTaxAmount": money("0.75"),
        "taxes": [
            {
                "id": f"gid://shopify/SaleTax/{index}",
                "amount": money("0.75"),
                "taxLine": tax(),
            }
        ],
    }


def rich_shop() -> FakeShop:
    shop = FakeShop()
    source = order(
        1, refunds=[refund(1)], transactions=[transaction(n) for n in range(27)]
    )
    source.update(
        tags=["Fixture"],
        discountCodes=["CODE0"],
        taxLines=[tax()],
        email="fixture@example.invalid",
        discountApplications=[discount(n) for n in range(7)],
        shippingLines=[shipping(n) for n in range(7)],
        agreements=[
            {
                "__typename": "OrderEditAgreement",
                "id": f"gid://shopify/SalesAgreement/{n}",
                "reason": "ORDER_EDIT",
                "happenedAt": "2026-10-01T00:00:00Z",
                "sales": [sale(n * 10 + s) for s in range(7)],
            }
            for n in range(7)
        ],
    )
    item = line(1)
    item.update(
        taxLines=[tax()],
        discountAllocations=[
            {
                "allocatedAmountSet": money("1.123456789"),
                "discountApplication": {"index": 0},
            }
        ],
    )
    refund_item = objects(source["refunds"], "refunds")[0]
    refund_item["duties"] = [
        {"originalDuty": {"id": "gid://shopify/Duty/1"}, "amountSet": money("2.25")}
    ]
    source["refunds"] = [refund_item]
    shop.add(source, [item])
    shop.refund_children["gid://shopify/Refund/1"] = {
        "orderAdjustments": [
            {
                "id": "gid://shopify/OrderAdjustment/1",
                "reason": "REFUND_DISCREPANCY",
                "amountSet": money("-1.25"),
                "taxAmountSet": money("0.00"),
            }
        ],
        "refundShippingLines": [
            {
                "id": "gid://shopify/RefundShippingLine/1",
                "shippingLine": {"id": "gid://shopify/ShippingLine/0"},
                "subtotalAmountSet": money("4.00"),
                "taxAmountSet": money("0.75"),
            }
        ],
        "transactions": [
            {"id": f"gid://shopify/OrderTransaction/{n}"} for n in range(27)
        ],
    }
    return shop


def parse(shop: FakeShop) -> ParsedOrder:
    order_id = "gid://shopify/Order/1"
    children = {
        refund_id: RefundChildren(
            objects(child.get("refundLineItems", []), "refundLineItems"),
            objects(child.get("orderAdjustments", []), "orderAdjustments"),
            objects(child.get("refundShippingLines", []), "refundShippingLines"),
            objects(child.get("transactions", []), "transactions"),
        )
        for refund_id, child in shop.refund_children.items()
    }
    parsed = parse_order(SHOP, shop.orders[order_id], shop.lines[order_id], children)
    assert isinstance(parsed, ParsedOrder)
    return parsed


@pytest.mark.parametrize(
    "kind",
    [
        "ProductSale",
        "GiftCardSale",
        "TipSale",
        "ShippingLineSale",
        "DutySale",
        "AdditionalFeeSale",
        "FeeSale",
        "AdjustmentSale",
        "UnknownSale",
    ],
)
def test_sales_keep_signed_money_and_type_specific_context(kind: str) -> None:
    entry = sale(1)
    entry.update(
        __typename=kind,
        quantity=None,
        totalAmount=money("-1.123456789"),
        actionType="RETURN",
    )
    entry.update(shippingLine=None, duty=None, additionalFee=None, fee=None)
    source = order(1)
    source["agreements"] = [
        {
            "__typename": "RefundAgreement",
            "id": "gid://shopify/SalesAgreement/1",
            "reason": "REFUND",
            "happenedAt": "2026-10-01T00:00:00Z",
            "refund": {"id": "gid://shopify/Refund/1"},
            "sales": [entry],
        }
    ]
    parsed = parse_order(SHOP, source, [], {})
    assert isinstance(parsed, ParsedOrder)
    agreement = objects(parsed.content["agreements"], "agreements")[0]
    assert agreement["refund_id"] == "gid://shopify/Refund/1"
    mapped = objects(agreement["sales"], "sales")[0]
    assert mapped["total_amount_shop_amount"] == Decimal("-1.123456789")
    assert mapped["quantity"] is None
    assert mapped["type"] == kind


def test_discounts_tax_gift_cards_and_payment_reversal_remain_separate() -> None:
    shop = rich_shop()
    source = shop.orders["gid://shopify/Order/1"]
    source["taxesIncluded"] = True
    txs = [transaction(1, "SALE", "FAILURE"), transaction(2, "REFUND", "SUCCESS")]
    txs[1]["parentTransaction"] = {"id": txs[0]["id"]}
    txs[1]["gateway"] = "gift_card"
    source["transactions"] = txs
    source["transactionsCount"] = {"count": 2, "precision": "EXACT"}
    shop.refund_children["gid://shopify/Refund/1"]["transactions"] = [
        {"id": txs[1]["id"]}
    ]
    item = line(1)
    item.update(
        isGiftCard=True,
        taxLines=[tax()],
        discountAllocations=[
            {
                "allocatedAmountSet": money("0.123456789"),
                "discountApplication": {"index": 0},
            }
        ],
    )
    shop.lines[str(source["id"])] = [item]
    parsed = parse(shop)
    # The source-shaped parent link and failure survive; ingestion derives no
    # business refund flag from either the gateway or the refund object's presence.
    transactions = objects(parsed.content["transactions"], "transactions")
    assert transactions[0]["status"] == "FAILURE"
    assert objects(parsed.content["lines"], "lines")[0]["is_gift_card"] is True


def test_international_tax_inclusive_order_keeps_source_currency_and_address() -> None:
    source = order(1)
    source.update(taxesIncluded=True, presentmentCurrencyCode="EUR")
    source["originalTotalPriceSet"] = {
        "shopMoney": {"amount": "12.123456789", "currencyCode": "USD"},
        "presentmentMoney": {"amount": "10.00", "currencyCode": "EUR"},
    }
    source["shippingAddress"] = {
        "firstName": "Fixture",
        "lastName": "Customer",
        "company": None,
        "address1": "1 Fixture Street",
        "address2": None,
        "city": "Paris",
        "province": None,
        "provinceCode": None,
        "zip": "75001",
        "country": "France",
        "countryCodeV2": "FR",
        "phone": None,
    }
    manual = discount(0)
    manual.update(
        __typename="ManualDiscountApplication",
        title="Manual",
        description=None,
        value={"__typename": "MoneyV2", "amount": "1.25", "currencyCode": "EUR"},
    )
    source["discountApplications"] = [manual]
    parsed = parse_order(SHOP, source, [], {})
    assert isinstance(parsed, ParsedOrder)
    assert parsed.content["original_total_price_shop_amount"] == Decimal("12.123456789")
    assert parsed.content["original_total_price_presentment_amount"] == Decimal("10.00")
    assert parsed.content["original_total_price_presentment_currency"] == "EUR"
    assert parsed.content["shipping_address_country_code"] == "FR"
    assert parsed.content["shipping_address_province_code"] is None
    assert parsed.content["taxes_included"] is True
    application = objects(parsed.content["discount_applications"], "discounts")[0]
    assert application["value_amount"] == Decimal("1.25")
    assert application["value_currency"] == "EUR"


def test_parsed_rows_fill_exactly_the_declared_columns() -> None:
    shop = rich_shop()
    shop.refund_children["gid://shopify/Refund/1"]["refundLineItems"] = [
        {
            "id": "gid://shopify/RefundLineItem/1",
            "lineItem": {"id": line(1)["id"]},
            "quantity": 1,
            "subtotalSet": money("10.00"),
            "totalTaxSet": money("0.00"),
        }
    ]
    parsed = parse(shop)
    seen: set[tuple[str, ...]] = set()

    def walk(path: tuple[str, ...], row: JsonObject) -> None:
        nested = {key for key, value in row.items() if isinstance(value, list)}
        columns = NESTED_TABLES[path] if path else ORDER_COLUMNS
        declared = {
            column.name for column in columns if not column.name.startswith("_")
        }
        assert set(row) - nested == declared, path
        seen.add(path)
        for key in nested:
            for child in objects(row[key], key):
                walk((*path, key), child)

    walk((), parsed.content)
    assert seen == {(), *NESTED_TABLES}


@pytest.mark.parametrize(
    ("quantity", "current", "unfulfilled", "non_fulfillable"),
    [(4, 3, 0, 1), (1, 1, 1, 0), (3, 2, 1, 1)],
)
def test_line_fulfillment_evidence_preserves_source_quantities(
    quantity: int, current: int, unfulfilled: int, non_fulfillable: int
) -> None:
    item = line(1, quantity=quantity)
    item.update(
        currentQuantity=current,
        unfulfilledQuantity=unfulfilled,
        nonFulfillableQuantity=non_fulfillable,
        requiresShipping=True,
    )
    parsed = parse_order(SHOP, order(1), [item], {})
    assert isinstance(parsed, ParsedOrder)
    actual = objects(parsed.content["lines"], "lines")[0]
    assert actual["current_quantity"] == current
    assert actual["unfulfilled_quantity"] == unfulfilled
    assert actual["non_fulfillable_quantity"] == non_fulfillable
    assert actual["requires_shipping"] is True


@pytest.mark.parametrize(
    "field", ["unfulfilledQuantity", "nonFulfillableQuantity", "requiresShipping"]
)
def test_missing_line_fulfillment_evidence_rejects_new_observation(field: str) -> None:
    item = line(1)
    item.update(unfulfilledQuantity=1, nonFulfillableQuantity=0, requiresShipping=True)
    del item[field]
    assert isinstance(parse_order(SHOP, order(1), [item], {}), OrderException)
