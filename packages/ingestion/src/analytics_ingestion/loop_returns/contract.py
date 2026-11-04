"""Header contract for Loop return records: exact keys, exact money, no PII.

The API returns customer, address, tracking and line-level data. This module
reads only the header fields in ``RETURN_COLUMNS`` and drops everything else.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal

from analytics_ingestion.loop_returns.errors import ContractError

ColumnType = Literal["text", "timestamp", "decimal", "bigint"]
DECIMAL_PRECISION = 38
DECIMAL_SCALE = 9
ABSENT = ("", "N/A")

_IDENTIFIER = re.compile(r"[A-Za-z0-9_-]{1,64}")
_DIGITS = re.compile(r"[0-9]{1,20}")
_CURRENCY = re.compile(r"[A-Z]{3}")
_MONEY = re.compile(r"-?[0-9]{1,29}(\.[0-9]{1,9})?")


@dataclass(frozen=True)
class Column:
    name: str
    data_type: ColumnType
    nullable: bool = True


RETURN_COLUMNS: tuple[Column, ...] = (
    Column("shop_id", "text", nullable=False),
    Column("return_id", "text", nullable=False),
    Column("shopify_order_id", "text", nullable=False),
    Column("return_state", "text", nullable=False),
    Column("created_at", "timestamp", nullable=False),
    Column("updated_at", "timestamp", nullable=False),
    Column("currency", "text", nullable=False),
    Column("return_total", "decimal"),
    Column("refund", "decimal"),
    Column("handling_fee", "decimal"),
    Column("gift_card", "decimal"),
    Column("exchange_total", "decimal"),
    Column("upsell", "decimal"),
)


@dataclass(frozen=True)
class ReturnHeader:
    shop_id: str
    return_id: str
    shopify_order_id: str
    return_state: str
    created_at: datetime
    updated_at: datetime
    currency: str
    return_total: Decimal | None
    refund: Decimal | None
    handling_fee: Decimal | None
    gift_card: Decimal | None
    exchange_total: Decimal | None
    upsell: Decimal | None


def parse_return(record: Mapping[str, object], shop_id: str) -> ReturnHeader:
    """Validate one list-endpoint record. Unknown and PII fields are ignored."""
    return ReturnHeader(
        shop_id=shop_id,
        return_id=_identifier(record, "id"),
        shopify_order_id=_digits(record, "provider_order_id"),
        return_state=_text(record, "state"),
        created_at=_instant(record, "created_at"),
        updated_at=_instant(record, "updated_at"),
        currency=_currency(record, "currency"),
        return_total=_money(record, "return_total"),
        refund=_money(record, "refund"),
        handling_fee=_money(record, "handling_fee"),
        gift_card=_money(record, "gift_card"),
        exchange_total=_money(record, "exchange_total"),
        upsell=_money(record, "upsell"),
    )


def to_row(header: ReturnHeader) -> dict[str, object]:
    """One load row, keyed by ``RETURN_COLUMNS`` in declared order."""
    values: dict[str, object] = {
        "shop_id": header.shop_id,
        "return_id": header.return_id,
        "shopify_order_id": header.shopify_order_id,
        "return_state": header.return_state,
        "created_at": header.created_at,
        "updated_at": header.updated_at,
        "currency": header.currency,
        "return_total": header.return_total,
        "refund": header.refund,
        "handling_fee": header.handling_fee,
        "gift_card": header.gift_card,
        "exchange_total": header.exchange_total,
        "upsell": header.upsell,
    }
    return {column.name: values[column.name] for column in RETURN_COLUMNS}


def _present(record: Mapping[str, object], field: str) -> object:
    value = record.get(field)
    if value is None:
        raise ContractError(f"{field} is required")
    return value


def _text(record: Mapping[str, object], field: str) -> str:
    value = _present(record, field)
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{field} must be a non-empty string")
    return value


def _identifier(record: Mapping[str, object], field: str) -> str:
    value = _present(record, field)
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ContractError(f"{field} must be a string or integer")
    text = str(value)
    if not _IDENTIFIER.fullmatch(text):
        raise ContractError(f"{field} has an unexpected form")
    return text


def _digits(record: Mapping[str, object], field: str) -> str:
    text = _text(record, field)
    if not _DIGITS.fullmatch(text):
        raise ContractError(f"{field} must be a numeric Shopify order id")
    return text


def _currency(record: Mapping[str, object], field: str) -> str:
    text = _text(record, field)
    if not _CURRENCY.fullmatch(text):
        raise ContractError(f"{field} must be an ISO 4217 code")
    return text


def _instant(record: Mapping[str, object], field: str) -> datetime:
    text = _text(record, field)
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        raise ContractError(f"{field} must be an ISO 8601 instant") from None
    if moment.tzinfo is None:
        raise ContractError(f"{field} must include a UTC offset")
    return moment.astimezone(UTC)


def _money(record: Mapping[str, object], field: str) -> Decimal | None:
    """Return exact money, or None for the provider's absent markers."""
    value = record.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ContractError(f"{field} must be a string amount")
    if value.strip() in ABSENT:
        return None
    if not _MONEY.fullmatch(value):
        raise ContractError(f"{field} is not an exact decimal amount")
    return Decimal(value)
