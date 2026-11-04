"""Customer profiles refresh independently and preserve safe order joins."""

import json
import subprocess
from pathlib import Path

import duckdb
import pytest

from tests.project_build import NAMESPACE, REMOVED_CUSTOMER, ProjectBuild
from tests.shopify_order_fixtures import ROOT

# Real dbt project nodes these tests read or execute (ADR 0026).
pytestmark = pytest.mark.dbt(
    "+path:models/marts/shopify_customers",
    "+path:models/marts/shopify_orders",
    "path:models/staging/shopify_customers",
)
MARTS = f"{NAMESPACE}_marts"


def _failed_tests(target: Path) -> set[str]:
    results = json.loads((target / "run_results.json").read_text())  # pyright: ignore[reportAny] # dbt artifact read in a test.
    return {
        str(result["unique_id"]).split(".")[2]  # pyright: ignore[reportAny]
        for result in results["results"]  # pyright: ignore[reportAny]
        if result["status"] in ("fail", "error")
    }


def test_reviewed_customer_contracts_match_the_build(
    project_build: ProjectBuild,
) -> None:
    from analytics_transformations.mart_contracts import (
        contracts,
        read_json,
        snapshot,
    )

    manifest = read_json(project_build.target / "manifest.json")
    assert (
        snapshot(contracts(manifest))
        == (ROOT / "dbt/customer-mart-contracts.json").read_text()
    )


def test_profiles_without_orders_and_cross_shop_ids_keep_their_grain(
    project_build: ProjectBuild,
) -> None:
    with duckdb.connect(str(project_build.database), read_only=True) as db:
        assert db.execute(
            "select shopify_shop_id, shopify_customer_id, shopify_order_count "
            f"from {MARTS}.dim_customers order by 1, 2"
        ).fetchall() == [
            ("shop-1", "customer-1", 2),
            ("shop-1", "customer-2", 0),
            ("shop-2", "customer-1", 1),
        ]
        # Joining customers never repeats or drops an order.
        assert (
            db.execute(
                f"select count(*) from {MARTS}.fct_orders o "
                f"left join {MARTS}.dim_customers c "
                "on o.shopify_shop_id = c.shopify_shop_id "
                "and o.shopify_customer_id = c.shopify_customer_id"
            ).fetchone()
            == db.execute(f"select count(*) from {MARTS}.fct_orders").fetchone()
        )


def test_order_contacts_never_read_customer_profiles(
    project_build: ProjectBuild,
) -> None:
    """A profile update cannot change an order's contact details."""
    manifest = json.loads((project_build.target / "manifest.json").read_text())  # pyright: ignore[reportAny] # dbt artifact read in a test.
    parents: dict[str, list[str]] = manifest["parent_map"]  # pyright: ignore[reportAny]
    ancestors: set[str] = set()
    pending = ["model.analytics.dim_order_contacts"]
    while pending:
        for parent in parents.get(pending.pop(), []):
            if parent not in ancestors:
                ancestors.add(parent)
                pending.append(parent)
    assert ancestors
    assert not {node for node in ancestors if "shopify_customers" in node}


def test_customer_access_denial_is_distinct_from_a_legitimate_null(
    project_build: ProjectBuild,
) -> None:
    with duckdb.connect(str(project_build.database), read_only=True) as db:
        assert db.execute(
            "select shopify_shop_id, email_address, email_access_status "
            f"from {MARTS}.dim_customer_details "
            "where shopify_customer_id='customer-1' order by 1"
        ).fetchall() == [
            ("shop-1", None, "available"),
            ("shop-2", None, "access_denied"),
        ]
        columns = {
            row[0]
            for row in db.execute(
                "select column_name from information_schema.columns "
                f"where table_schema='{MARTS}' and table_name='dim_customers'"
            ).fetchall()
        }
    assert columns.isdisjoint(
        {
            "first_name",
            "last_name",
            "display_name",
            "email_address",
            "phone_number",
            "customer_note",
            "tax_id",
            "latitude",
            "longitude",
            "address_line_1",
            "address_line_2",
        }
    )


@pytest.mark.parametrize(
    ("statement", "failing_test"),
    [
        (
            "insert into raw_customers.customers select * from "
            "raw_customers.customers where shop_id='shop-1' and "
            "customer_id='customer-1'",
            "shopify_customers_customers_unique_key",
        ),
        (
            "insert into raw_customers.customers__addresses select * from "
            "raw_customers.customers__addresses where shop_id='shop-1' "
            "and customer_id='customer-1'",
            "shopify_customers_addresses_unique_key",
        ),
        (
            "insert into raw_customers.customer_field_access select * from "
            "raw_customers.customer_field_access where shop_id='shop-1' "
            "and field_group='email'",
            "shopify_customers_field_access_unique_key",
        ),
    ],
)
def test_duplicate_sources_fail_before_publication(
    project_build: ProjectBuild, tmp_path: Path, statement: str, failing_test: str
) -> None:
    # Staging models are views, so the copy's checks read the duplicate.
    path = project_build.copy(tmp_path)
    with duckdb.connect(str(path)) as db:
        db.execute(statement)
    result = subprocess.run(
        [
            "dbt",
            "test",
            "--project-dir",
            str(ROOT / "dbt"),
            "--profiles-dir",
            str(ROOT / "dbt"),
            "--target-path",
            str(tmp_path / "target"),
            "--select",
            "path:models/staging/shopify_customers",
            "--indirect-selection",
            "cautious",
        ],
        env=project_build.env(path),
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert result.returncode != 0
    assert _failed_tests(tmp_path / "target") == {failing_test}, result.stdout


def test_removed_customer_keeps_orders_and_removes_customer_details(
    project_build: ProjectBuild,
) -> None:
    """Ingestion purges an unavailable profile; its orders stay published."""
    with duckdb.connect(str(project_build.database), read_only=True) as db:
        assert db.execute(
            f"select shopify_order_id from {MARTS}.fct_orders "
            "where shopify_customer_id = ?",
            [REMOVED_CUSTOMER],
        ).fetchall() == [("1004",)]
        for relation in ("dim_customers", "dim_customer_details"):
            assert db.execute(
                f"select count(*) from {MARTS}.{relation} "
                "where shopify_customer_id = ?",
                [REMOVED_CUSTOMER],
            ).fetchone() == (0,)
