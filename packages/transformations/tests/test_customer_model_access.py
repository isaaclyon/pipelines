"""Check analytical column separation in fixtures, not warehouse permissions."""

import duckdb
import pytest

from tests.project_build import GEOGRAPHY_PROBES, NAMESPACE, ProjectBuild

# Real dbt project nodes these tests read (ADR 0026).
pytestmark = pytest.mark.dbt(
    "+path:models/marts/shopify_customers",
    "+dim_order_geography",
    "+fct_order_lines",
)
MARTS = f"{NAMESPACE}_marts"


def test_customer_models_preserve_geography_and_separate_contact_columns(
    project_build: ProjectBuild,
) -> None:
    with duckdb.connect(str(project_build.database), read_only=True) as db:
        assert db.execute(
            "select shopify_shop_id, shopify_customer_id, default_address_city, "
            f"default_address_postal_code from {MARTS}.dim_customers order by 1, 2"
        ).fetchall() == [
            ("shop-1", "customer-1", "Salt Lake City", "84101"),
            ("shop-1", "customer-2", None, None),
            ("shop-2", "customer-1", "Berlin", "10115"),
        ]
        for relation in ("dim_order_geography", "fct_order_lines"):
            columns = {
                row[0]
                for row in db.execute(
                    "select column_name from information_schema.columns "
                    "where table_schema = ? and table_name = ?",
                    [MARTS, relation],
                ).fetchall()
            }
            assert not columns & {"order_email", "shipping_address_address1"}
            # Only order 1002 carries the probes, so each column reads its own.
            for column, value in GEOGRAPHY_PROBES.items():
                assert db.execute(
                    f"select distinct shopify_order_id from {MARTS}.{relation} "
                    f"where {column} = ?",
                    [value],
                ).fetchall() == [("1002",)]
        assert db.execute(
            f"select is_email_format_valid from {MARTS}.dim_customer_details "
            "where shopify_customer_id = 'customer-2'"
        ).fetchone() == (True,)
