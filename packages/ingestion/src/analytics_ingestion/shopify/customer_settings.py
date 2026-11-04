"""Customer runtime configuration; reuses the orders destination and shop."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import replace
from typing import cast

from analytics_ingestion.shopify.customer_model import ApprovedMetafields
from analytics_ingestion.shopify.errors import ConfigurationError
from analytics_ingestion.shopify.settings import Settings, settings_from_env

DEFAULT_CUSTOMERS_DATASET = "SHOPIFY_CUSTOMERS_RAW"
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def customer_settings_from_env(env: Mapping[str, str]) -> Settings:
    """Settings for the customer pipeline.

    ``SHOPIFY_CUSTOMERS_CREDENTIALS_PATH`` selects a credential limited to
    customer scopes; without it the shared credential file is used. The
    destination is the orders destination, with its own dataset and dlt state.
    """
    selected = dict(env)
    own = env.get("SHOPIFY_CUSTOMERS_CREDENTIALS_PATH", "").strip()
    if own:
        selected["SHOPIFY_CREDENTIALS_PATH"] = own
    dataset = env.get("SHOPIFY_CUSTOMERS_DATASET", DEFAULT_CUSTOMERS_DATASET)
    if not _IDENTIFIER.fullmatch(dataset):
        raise ConfigurationError("SHOPIFY_CUSTOMERS_DATASET must be an identifier")
    return replace(settings_from_env(selected), dataset=dataset)


APPROVED_METAFIELDS_VAR = "SHOPIFY_CUSTOMERS_APPROVED_METAFIELDS"


def customer_metafields_from_env(env: Mapping[str, str]) -> ApprovedMetafields:
    """Exact approved (namespace, key) pairs from a JSON list of 2-string lists.

    Missing or blank means no values are collected. Wildcards and any other
    shape are rejected without echoing the configured text.
    """
    raw = env.get(APPROVED_METAFIELDS_VAR, "").strip()
    if not raw:
        return frozenset()
    message = (
        f'{APPROVED_METAFIELDS_VAR} must be a JSON list like [["namespace","key"]]'
    )
    try:
        data = cast(object, json.loads(raw))
    except ValueError:
        raise ConfigurationError(message) from None
    if not isinstance(data, list):
        raise ConfigurationError(message)
    pairs: set[tuple[str, str]] = set()
    for item in cast(list[object], data):
        if not isinstance(item, list) or len(cast(list[object], item)) != 2:
            raise ConfigurationError(message)
        namespace, key = cast(list[object], item)
        if not isinstance(namespace, str) or not isinstance(key, str):
            raise ConfigurationError(message)
        if not namespace or not key or "*" in namespace or "*" in key:
            raise ConfigurationError(message)
        pairs.add((namespace, key))
    return frozenset(pairs)
