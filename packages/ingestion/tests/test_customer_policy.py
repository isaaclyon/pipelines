"""Customer metafield approval and address ID policy."""

from __future__ import annotations

import pytest

from analytics_ingestion.shopify.customer_model import metafield_is_collected
from analytics_ingestion.shopify.customer_parse import ParsedCustomer, parse_customer
from analytics_ingestion.shopify.errors import ContractError
from analytics_ingestion.shopify.jsonvalue import JsonObject, objects
from tests.customer_fakes import address, customer, metafield

PARAM = "?model_name=CustomerAddress"


def parse(
    node: JsonObject,
    metafields: list[JsonObject] | None = None,
    approved: frozenset[tuple[str, str]] | None = None,
) -> ParsedCustomer:
    groups = frozenset({"addresses", "metafields"})
    addresses = objects(node["addressesV2"], "addresses")
    kwargs = {} if approved is None else {"approved_metafields": approved}
    return parse_customer(
        "shop", node, groups, addresses, metafields or [], [], **kwargs
    )


def test_parameterized_default_and_address_pair_parses() -> None:
    a = address(5)
    a["id"] = f"gid://shopify/MailingAddress/5{PARAM}"
    row = parse(customer(1, addresses=[a])).content
    assert row["default_address_id"] == a["id"]
    addresses = row["addresses"]
    assert isinstance(addresses, list)
    assert addresses[0]["address_id"] == a["id"]
    assert addresses[0]["is_default_address"] is True


def test_unparameterized_address_still_parses() -> None:
    row = parse(customer(1, addresses=[address(5)])).content
    assert row["addresses"][0]["is_default_address"] is True  # type: ignore[index]


@pytest.mark.parametrize(
    "bad",
    [
        "gid://shopify/Customer/5",
        "gid://shopify/MailingAddress/abc",
        "gid://shopify/MailingAddress/5?model_name=Other",
        "gid://shopify/MailingAddress/5?model_name=CustomerAddress&x=1",
        "gid://shopify/MailingAddress/5'; DROP TABLE x;--",
    ],
)
def test_bad_address_id_rejected_without_value(bad: str) -> None:
    a = address(5)
    a["id"] = bad
    with pytest.raises(ContractError) as error:
        parse(customer(1, addresses=[a]))
    assert bad not in str(error.value)


def test_metafield_without_approval_has_no_values() -> None:
    node = customer(1, metafields=[metafield(1)])
    assert parse(node, [metafield(1)]).content["metafields"] == []


def test_approved_pair_is_collected_only_with_namespace_and_key() -> None:
    approved = frozenset({("custom", "loyalty_tier")})
    wanted = metafield(1, "custom", "loyalty_tier")
    others = [metafield(2, "custom", "birthday"), metafield(3, "other", "loyalty_tier")]
    node = customer(1, metafields=[wanted, *others])
    rows = parse(node, [wanted, *others], approved).content["metafields"]
    assert [r["metafield_id"] for r in rows] == [wanted["id"]]  # type: ignore[index]


def test_denied_prefix_wins_over_approval() -> None:
    assert not metafield_is_collected("secret_x", "k", frozenset({("secret_x", "k")}))
    assert not metafield_is_collected("$app:x", "k", frozenset({("$app:x", "k")}))
    assert metafield_is_collected("custom", "k", frozenset({("custom", "k")}))
    assert not metafield_is_collected("custom", "k")
