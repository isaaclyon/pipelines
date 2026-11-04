"""Loop Returns header contract: exact parsing and the non-PII column set."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from analytics_ingestion.loop_returns.contract import (
    RETURN_COLUMNS,
    ReturnHeader,
    parse_return,
    to_row,
)
from analytics_ingestion.loop_returns.errors import ContractError
from tests.loop_returns_fakes import SHOP_ID, api_return

CREATED = datetime(2026, 7, 22, 17, 34, 36, tzinfo=UTC)


def test_header_keeps_natural_key_provider_order_and_exact_money() -> None:
    header = parse_return(api_return("119036512", "2026-07-22T17:34:36+00:00"), SHOP_ID)

    assert header == ReturnHeader(
        shop_id=SHOP_ID,
        return_id="119036512",
        shopify_order_id="12600441897332",
        return_state="closed",
        created_at=CREATED,
        updated_at=CREATED,
        currency="USD",
        return_total=Decimal("189.48"),
        refund=Decimal("174.48"),
        handling_fee=Decimal("15"),
        gift_card=Decimal("0"),
        exchange_total=None,
        upsell=Decimal("0.00"),
    )


def test_integer_identifier_becomes_its_exact_text() -> None:
    header = parse_return(api_return(119036512, "2026-07-22T17:34:36+00:00"), SHOP_ID)

    assert header.return_id == "119036512"


@pytest.mark.parametrize("absent", ["N/A", "", None])
def test_absent_money_is_unknown_not_zero(absent: str | None) -> None:
    record = api_return("1", "2026-07-22T17:34:36+00:00", refund=absent)

    assert parse_return(record, SHOP_ID).refund is None


def test_instants_convert_to_utc_from_any_explicit_offset() -> None:
    record = api_return(
        "1",
        "2026-07-22T10:34:36-07:00",
        updated_at="2026-07-22T17:34:36Z",
    )

    header = parse_return(record, SHOP_ID)

    assert header.created_at == CREATED
    assert header.created_at.utcoffset() == timedelta(0)
    assert header.updated_at == CREATED


def test_naive_instant_is_rejected() -> None:
    record = api_return("1", "2026-07-22T17:34:36")

    with pytest.raises(ContractError, match="created_at"):
        _ = parse_return(record, SHOP_ID)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("refund", 174.48),
        ("refund", "1e3"),
        ("refund", "1.0000000001"),
        ("refund", "1234567890123456789012345678901234567890"),
        ("provider_order_id", "gid://shopify/Order/1"),
        ("currency", "usd"),
    ],
)
def test_malformed_scalars_are_rejected_by_name(field: str, value: object) -> None:
    record = api_return("1", "2026-07-22T17:34:36+00:00", **{field: value})

    with pytest.raises(ContractError, match=field):
        _ = parse_return(record, SHOP_ID)


@pytest.mark.parametrize("missing", ["id", "state", "created_at", "updated_at"])
def test_required_fields_must_be_present(missing: str) -> None:
    record = api_return("1", "2026-07-22T17:34:36+00:00")
    del record[missing]

    with pytest.raises(ContractError, match=missing):
        _ = parse_return(record, SHOP_ID)


def test_row_carries_only_the_declared_non_pii_columns() -> None:
    header = parse_return(api_return("1", "2026-07-22T17:34:36+00:00"), SHOP_ID)

    row = to_row(header)

    assert set(row) == {column.name for column in RETURN_COLUMNS}
    assert {"email", "customer", "tracking_number", "line_items"}.isdisjoint(row)
    assert row["shop_id"] == SHOP_ID
