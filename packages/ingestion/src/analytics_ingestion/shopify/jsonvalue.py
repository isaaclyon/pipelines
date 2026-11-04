"""Typed parsing of untrusted JSON into values the rest of the source can use."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import cast

from analytics_ingestion.shopify.errors import ContractError, SourceError

JsonObject = dict[str, object]

_DECIMAL = re.compile(r"-?\d+(\.\d+)?")
_GID = re.compile(r"gid://shopify/([A-Za-z]+)/(\d+)")
# Raw decimal columns are NUMBER/DECIMAL(38, 9) on every destination.
DECIMAL_PRECISION = 38
DECIMAL_SCALE = 9


def load_json(raw: str | bytes, what: str) -> object:
    try:
        # json.loads is dynamically typed; callers parse the result immediately.
        value: object = json.loads(raw)  # pyright: ignore[reportAny]
    except (ValueError, UnicodeError):
        raise SourceError(f"Invalid JSON in {what}") from None
    return value


def as_object(value: object, field: str) -> JsonObject:
    if not isinstance(value, dict):
        raise SourceError(f"Expected object: {field}")
    result: JsonObject = {}
    for key, child in cast(dict[object, object], value).items():
        if not isinstance(key, str):
            raise SourceError(f"Expected string keys: {field}")
        result[key] = child
    return result


def as_list(value: object, field: str) -> list[object]:
    if not isinstance(value, list):
        raise SourceError(f"Expected array: {field}")
    return list(cast(list[object], value))


def objects(value: object, field: str) -> list[JsonObject]:
    return [as_object(item, field) for item in as_list(value, field)]


def load_object(raw: str | bytes, what: str) -> JsonObject:
    return as_object(load_json(raw, what), what)


def required_str(data: JsonObject, field: str) -> str:
    value = data.get(field)
    if not isinstance(value, str) or not value:
        raise ContractError(f"Expected nonempty string: {field}")
    return value


def selected_value(data: JsonObject, field: str) -> object:
    """A selected nullable field must be present even when its value is null."""
    if field not in data:
        raise ContractError(f"Selected field missing: {field}")
    return data[field]


def optional_str(data: JsonObject, field: str) -> str | None:
    value = selected_value(data, field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ContractError(f"Expected string or null: {field}")
    return value


def required_bool(data: JsonObject, field: str) -> bool:
    value = data.get(field)
    if not isinstance(value, bool):
        raise ContractError(f"Expected boolean: {field}")
    return value


def required_int(data: JsonObject, field: str) -> int:
    value = data.get(field)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ContractError(f"Expected integer: {field}")
    return value


def parse_instant(value: str, field: str) -> datetime:
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ContractError(f"Invalid timestamp: {field}") from None
    if stamp.tzinfo is None:
        raise ContractError(f"Timestamp without offset: {field}")
    return stamp.astimezone(UTC)


def required_instant(data: JsonObject, field: str) -> datetime:
    return parse_instant(required_str(data, field), field)


def optional_instant(data: JsonObject, field: str) -> datetime | None:
    value = optional_str(data, field)
    return None if value is None else parse_instant(value, field)


def parse_decimal(value: object, field: str) -> Decimal:
    """Exact decimal from Shopify's string form; reject lossy destination storage."""
    if not isinstance(value, str) or not _DECIMAL.fullmatch(value):
        raise ContractError(f"Expected decimal string: {field}")
    try:
        amount = Decimal(value)
    except InvalidOperation:
        raise ContractError(f"Invalid decimal: {field}") from None
    whole, _, fraction = value.lstrip("-").partition(".")
    if (
        len(fraction.rstrip("0")) > DECIMAL_SCALE
        or len(whole.lstrip("0")) > DECIMAL_PRECISION - DECIMAL_SCALE
    ):
        raise ContractError(f"Decimal exceeds raw precision: {field}")
    return amount


def gid(value: object, kind: str, field: str) -> str:
    """Validate a Shopify global ID of the expected type; safe to embed in SQL."""
    if not isinstance(value, str):
        raise ContractError(f"Expected {kind} ID: {field}")
    match = _GID.fullmatch(value)
    if not match or match[1] != kind:
        raise ContractError(f"Expected {kind} ID: {field}")
    return value


def optional_ref(data: JsonObject, field: str, kind: str) -> str | None:
    """A nullable `{ id }` object reference."""
    value = selected_value(data, field)
    if value is None:
        return None
    return gid(as_object(value, field).get("id"), kind, field)
