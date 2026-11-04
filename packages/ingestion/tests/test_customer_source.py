"""Customer source behavior against the offline Admin GraphQL fake and DuckDB."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from analytics_ingestion.shopify.customer_model import FIELD_GROUPS, NESTED_TABLES
from analytics_ingestion.shopify.customer_source import (
    CustomersRun,
    RunReport,
    build_pipeline,
    load_customers,
)
from analytics_ingestion.shopify.errors import SourceError
from tests.customer_fakes import (
    FakeCustomerShop,
    address,
    contact,
    customer,
    customer_settings,
    metafield,
)
from tests.shopify_fakes import at

START = at("2026-10-01T00:00:00Z")
END = at("2026-10-01T01:00:00Z")


def run(
    root: Path,
    shop: FakeCustomerShop,
    kind: str = "reextract",
    run_id: str = "run-1",
    start: datetime = START,
    end: datetime = END,
) -> RunReport:
    return load_customers(
        customer_settings(root),
        CustomersRun(run_id, kind, start, end),  # pyright: ignore[reportArgumentType]
        transport=shop,
        sleeper=lambda _: None,
        # Synthetic fixtures explicitly exercise conditional tax ID.
        collect_tax_id=True,
        approved_metafields=frozenset(
            {("custom", "birthday"), *(("custom", f"k{n}") for n in range(1, 131))}
        ),
    )


def query(root: Path, sql: str, *args: object) -> list[tuple[object, ...]]:
    pipeline = build_pipeline(customer_settings(root))
    with pipeline.sql_client() as client:
        rows = client.execute_sql(sql, *args) or []
    return [tuple(r) for r in rows]


def test_initial_load_keeps_profile_and_every_child(tmp_path: Path) -> None:
    shop = FakeCustomerShop(page_size=2)
    shop.add(
        customer(
            1,
            addresses=[address(n) for n in range(11, 36)],
            metafields=[metafield(n, key=f"k{n}") for n in range(1, 131)],
            contacts=[contact(5, list(range(1, 61)))],
        )
    )
    shop.add(customer(2, addresses=[]))
    shop.add(customer(3))

    report = run(tmp_path, shop)

    assert report.customers == 3
    [row] = query(
        tmp_path,
        "select first_name, email_address, is_email_format_valid, sms_marketing_state,"
        " whatsapp_marketing_state, shopify_order_count, shopify_lifetime_spent_amount,"
        " tax_id, default_address_id from shopify_customers_raw.customers"
        " where customer_id = 'gid://shopify/Customer/1'",
    )
    assert row[:3] == ("Ada", "ada1@example.invalid", True)
    assert row[3] == "SUBSCRIBED"
    assert row[4] is None  # a missing channel object is never an explicit state
    assert row[5] == 3
    assert str(row[6]) == "123.456789012"
    assert row[7] == "TAX-1"
    [(addresses, defaults)] = query(
        tmp_path,
        "select count(*), sum(case when is_default_address then 1 else 0 end)"
        " from shopify_customers_raw.customers__addresses"
        " where customer_id = 'gid://shopify/Customer/1'",
    )
    assert (addresses, defaults) == (25, 1)
    assert query(
        tmp_path, "select count(*) from shopify_customers_raw.customers__metafields"
    ) == [(130,)]
    assert query(
        tmp_path,
        "select count(*) from shopify_customers_raw.customers__company_contacts",
    ) == [(60,)]
    assert query(tmp_path, "select count(*) from shopify_customers_raw.customers") == [
        (3,)
    ]
    # Customers without orders or addresses are kept.
    assert query(
        tmp_path,
        "select count(*) from shopify_customers_raw.customers__addresses"
        " where customer_id = 'gid://shopify/Customer/2'",
    ) == [(0,)]


def test_reload_replaces_children_and_updates_profile(tmp_path: Path) -> None:
    shop = FakeCustomerShop()
    shop.add(customer(1, addresses=[address(1), address(2)], tags=["a", "b"]))
    run(tmp_path, shop)

    shop.add(
        customer(
            1,
            updated="2026-10-01T00:45:00Z",
            addresses=[address(2, city="Salem")],
            tags=[],
        )
    )
    run(tmp_path, shop, run_id="run-2")

    assert query(
        tmp_path,
        "select city from shopify_customers_raw.customers__addresses",
    ) == [("Salem",)]
    assert query(
        tmp_path, "select count(*) from shopify_customers_raw.customers__tags"
    ) == [(0,)]
    assert query(
        tmp_path,
        "select count(*), max(_batch_id) from shopify_customers_raw.customers",
    ) == [(1, "run-2")]


def test_refresh_resumes_from_cursor_and_picks_up_profile_edit(tmp_path: Path) -> None:
    shop = FakeCustomerShop()
    shop.add(customer(1, updated="2026-10-01T00:10:00Z"))
    first = run(tmp_path, shop, "refresh", "run-1")
    assert first.customers == 1

    shop.add(customer(1, updated="2026-10-01T00:40:00Z"))
    shop.customers["gid://shopify/Customer/1"]["locale"] = "fr"
    second = run(tmp_path, shop, "refresh", "run-2")

    assert second.customers == 1
    assert second.window_start is not None
    assert second.window_start > START
    assert query(tmp_path, "select locale from shopify_customers_raw.customers") == [
        ("fr",)
    ]


@pytest.mark.parametrize("kind", ["reextract", "lookback"])
def test_manual_window_preserves_refresh_cursor(tmp_path: Path, kind: str) -> None:
    shop = FakeCustomerShop()
    shop.add(customer(1, updated="2026-10-01T00:10:00Z"))
    run(tmp_path, shop, "refresh", "initial-refresh")

    shop.add(customer(2, updated="2026-10-01T00:40:00Z"))
    repair_start = at("2026-10-01T00:30:00Z")
    repair = run(tmp_path, shop, kind, "manual-repair", start=repair_start)

    assert repair.window_start == repair_start
    assert repair.window_end == END
    assert repair.customers == 1
    assert query(
        tmp_path,
        "select customer_id from shopify_customers_raw.customers"
        " where _batch_id = 'manual-repair'",
    ) == [("gid://shopify/Customer/2",)]
    resumed = run(tmp_path, shop, "refresh", "resumed-refresh")
    assert resumed.window_start == at("2026-10-01T00:08:00Z")
    assert resumed.customers == 2


def test_empty_window_writes_coverage_evidence(tmp_path: Path) -> None:
    shop = FakeCustomerShop()

    report = run(tmp_path, shop)

    assert report.customers == 0
    [(kind, count, start, end, sweep)] = query(
        tmp_path,
        "select kind, customers_observed, window_start, window_end, is_full_sweep"
        " from shopify_customers_raw.customer_batch_evidence",
    )
    assert (kind, count, sweep) == ("reextract", 0, False)
    assert start is not None and end is not None


def test_failed_run_loads_nothing_and_recovers(tmp_path: Path) -> None:
    shop = FakeCustomerShop(page_size=1)
    shop.add(customer(1, updated="2026-10-01T00:10:00Z"))
    shop.add(customer(2, updated="2026-10-01T00:20:00Z"))
    seen: list[int] = []

    def fail_second(current: FakeCustomerShop) -> None:
        seen.append(1)
        if len(seen) == 2:
            current.page_size = 0  # next page returns a nonadvancing cursor

    shop.on_page = fail_second
    with pytest.raises(SourceError):
        run(tmp_path, shop)
    assert not _has_table(tmp_path, "customer_batch_evidence")

    shop.on_page = None
    shop.page_size = 1
    assert run(tmp_path, shop, run_id="run-2").customers == 2
    assert query(
        tmp_path,
        "select count(*) from shopify_customers_raw.customer_batch_evidence",
    ) == [(1,)]


def _has_table(root: Path, name: str) -> bool:
    try:
        query(root, f"select 1 from shopify_customers_raw.{name}")
    except Exception:
        return False
    return True


def test_denied_group_is_null_and_recorded(tmp_path: Path) -> None:
    shop = FakeCustomerShop()
    shop.denied = {"names", "email"}
    shop.unsupported = {"tax_id", "whatsapp_consent"}
    shop.add(customer(1))

    report = run(tmp_path, shop)

    assert report.metadata()["access_denied_groups"] == "email,names"
    assert query(
        tmp_path,
        "select first_name, email_address, phone_number, tax_id"
        " from shopify_customers_raw.customers",
    ) == [(None, None, "+15550100", None)]
    statuses = {
        str(row[0]): str(row[1])
        for row in query(
            tmp_path,
            "select field_group, status from"
            " shopify_customers_raw.customer_field_access",
        )
    }
    assert set(statuses) == {*FIELD_GROUPS, "whatsapp_consent"}
    assert statuses["names"] == "access_denied"
    assert statuses["tax_id"] == "unsupported"
    assert statuses["external_identities"] == "excluded"
    assert statuses["tags"] == "available"


def test_metafield_inventory_excludes_secret_namespaces(tmp_path: Path) -> None:
    shop = FakeCustomerShop()
    shop.add(
        customer(
            1,
            metafields=[metafield(1), metafield(2, namespace="Secret_vault", key="x")],
        )
    )

    run(tmp_path, shop)

    assert query(
        tmp_path,
        "select namespace from shopify_customers_raw.customers__metafields",
    ) == [("custom",)]
    assert query(
        tmp_path,
        "select namespace, collected from"
        " shopify_customers_raw.customer_metafield_inventory order by namespace",
    ) == [("Secret_vault", False), ("custom", True)]


def test_customer_scope_is_enough_and_required(tmp_path: Path) -> None:
    shop = FakeCustomerShop(scopes=["read_orders"])
    with pytest.raises(SourceError, match="read_customers"):
        run(tmp_path, shop)

    ok = FakeCustomerShop(scopes=["read_customers"])
    ok.add(customer(1))
    assert run(tmp_path, ok).customers == 1


def test_contract_break_is_rejected_without_blocking_others(tmp_path: Path) -> None:
    shop = FakeCustomerShop()
    shop.add(customer(1))
    bad = customer(2)
    bad["numberOfOrders"] = "many"
    shop.add(bad)

    report = run(tmp_path, shop)

    assert (report.customers, report.rejects) == (1, 1)
    [(error,)] = query(
        tmp_path, "select error from shopify_customers_raw.shopify_customer_rejects"
    )
    assert "numberOfOrders" in str(error)
    assert "ada2@example.invalid" not in str(error)

    # The next refresh re-reads the open reject; once fixed it loads and closes.
    shop.add(customer(2))
    again = run(tmp_path, shop, "refresh", "run-2")
    assert again.rereads == 1
    assert again.open_rejects == 0


def test_errors_never_carry_customer_values(tmp_path: Path) -> None:
    shop = FakeCustomerShop()
    broken = customer(1)
    broken["defaultAddress"] = {"id": "gid://shopify/MailingAddress/999"}
    shop.add(broken)

    report = run(tmp_path, shop)

    assert report.rejects == 1
    [(error,)] = query(
        tmp_path, "select error from shopify_customers_raw.shopify_customer_rejects"
    )
    assert "Lovelace" not in str(error) and "Main St" not in str(error)


def test_sweep_reads_all_and_marks_full_coverage(tmp_path: Path) -> None:
    shop = FakeCustomerShop()
    shop.add(customer(1, updated="2020-01-01T00:00:00Z"))
    shop.add(customer(2, updated="2026-10-01T00:20:00Z"))

    report = run(tmp_path, shop, "sweep", "sweep-1")

    assert report.customers == 2
    [(sweep, start)] = query(
        tmp_path,
        "select is_full_sweep, window_start from"
        " shopify_customers_raw.customer_batch_evidence",
    )
    assert sweep is True
    assert isinstance(start, datetime) and start.year == 1970


def test_declared_children_match_physical_tables(tmp_path: Path) -> None:
    shop = FakeCustomerShop()
    shop.add(
        customer(
            1,
            metafields=[metafield(1)],
            contacts=[contact(2, [])],
        )
    )
    run(tmp_path, shop)
    for path, columns in NESTED_TABLES.items():
        table = "customers__" + "__".join(path)
        names = {
            str(r[0])
            for r in query(
                tmp_path,
                "select column_name from information_schema.columns where"
                " table_schema = 'shopify_customers_raw' and table_name = %s",
                table,
            )
        }
        assert {c.name for c in columns} <= names, table
    assert query(
        tmp_path,
        "select company_location_id from"
        " shopify_customers_raw.customers__company_contacts",
    ) == [("",)]


def test_child_tables_exist_for_a_store_without_children_and_fill_later(
    tmp_path: Path,
) -> None:
    shop = FakeCustomerShop()
    shop.add(customer(1, addresses=[], tags=[]))
    run(tmp_path, shop)

    for table in NESTED_TABLES:
        name = "customers__" + "__".join(table)
        assert query(
            tmp_path, f"select count(*) from shopify_customers_raw.{name}"
        ) == [(1 if name == "customers__tax_exemptions" else 0,)]

    shop.add(customer(1, updated="2026-10-01T00:40:00Z", tags=["x"], addresses=[]))
    run(tmp_path, shop, run_id="run-2")
    shop.add(customer(1, updated="2026-10-01T00:50:00Z", tags=["y", "z"], addresses=[]))
    run(tmp_path, shop, run_id="run-3")

    assert query(
        tmp_path,
        "select tag from shopify_customers_raw.customers__tags order by tag",
    ) == [("y",), ("z",)]


def test_tax_id_requires_an_explicit_collection_decision(tmp_path: Path) -> None:
    shop = FakeCustomerShop()
    shop.add(customer(1))
    _ = load_customers(
        customer_settings(tmp_path),
        CustomersRun("default-policy", "reextract", START, END),
        transport=shop,
        sleeper=lambda _: None,
    )
    assert query(tmp_path, "select tax_id from shopify_customers_raw.customers") == [
        (None,)
    ]
    assert query(
        tmp_path,
        "select status from shopify_customers_raw.customer_field_access "
        "where field_group='tax_id'",
    ) == [("excluded",)]


def test_repeated_rejects_cannot_become_complete_zero_reject_evidence(
    tmp_path: Path,
) -> None:
    shop = FakeCustomerShop()
    shop.add(customer(1))
    bad = customer(2)
    bad["numberOfOrders"] = "many"
    shop.add(bad)
    _ = run(tmp_path, shop, "refresh", "invalid-first")
    report = run(tmp_path, shop, "refresh", "invalid-again")
    assert report.open_rejects == 1
    assert query(
        tmp_path,
        "select rejects from shopify_customers_raw.customer_batch_evidence "
        "where batch_id='invalid-again'",
    ) == [(1,)]


def test_customer_that_moves_during_child_reads_blocks_complete_coverage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from analytics_ingestion.shopify.customer_source import CustomerReader
    from analytics_ingestion.shopify.jsonvalue import JsonObject

    def unavailable(
        self: CustomerReader,
        parent_id: str,
        name: str,
        root: JsonObject,
        document: str,
    ) -> list[JsonObject] | None:
        return None

    monkeypatch.setattr(CustomerReader, "_follow", unavailable)
    shop = FakeCustomerShop()
    shop.add(customer(1))
    report = run(tmp_path, shop, "sweep")
    assert report.moved == 1
    assert query(
        tmp_path,
        "select completeness from shopify_customers_raw.customer_batch_evidence",
    ) == [("partial",)]
