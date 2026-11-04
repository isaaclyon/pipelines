"""Build payment evidence from raw fixtures without applying eligibility rules."""

import os
import subprocess
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

# Real dbt project nodes these tests execute (ADR 0026).
pytestmark = pytest.mark.dbt("+path:models/staging/shopify_orders")

ROOT = Path(__file__).parents[1]
MODELS = [
    "stg_shopify_orders__transactions",
    "stg_shopify_orders__refunds",
    "stg_shopify_orders__refund_transactions",
]


def _raw(path: Path) -> None:
    with duckdb.connect(str(path)) as db:
        _ = db.execute("create schema raw")
        _ = db.execute("""
            create table raw.orders__transactions as
            select 'shop-1'::varchar shop_id, 'order-1'::varchar order_id,
                'tx-1'::varchar transaction_id, 'auth-1'::varchar parent_transaction_id,
                '2026-07-01 00:30:00-07'::timestamptz created_at,
                null::timestamptz processed_at, ' capture '::varchar kind,
                ' success '::varchar status, 'manual'::varchar gateway,
                true::boolean test, null::varchar error_code,
                0.123456789::decimal(38,9) amount_shop_amount,
                'USD'::varchar amount_shop_currency,
                0.987654321::decimal(38,9) amount_presentment_amount,
                'CAD'::varchar amount_presentment_currency
        """)
        _ = db.execute("""
            insert into raw.orders__transactions
            select shop_id, order_id, 'tx-' || v.n, null, created_at, created_at,
                v.kind, v.status, gateway, false, 'DECLINED',
                amount_shop_amount, amount_shop_currency,
                amount_presentment_amount, amount_presentment_currency
            from raw.orders__transactions cross join (values
                ('2', 'authorization', 'pending'), ('3', 'sale', 'failure'),
                ('4', 'refund', 'success'), ('5', 'future_kind', 'future_status')
            ) v(n, kind, status)
        """)
        _ = db.execute("""
            create table raw.orders__refunds as
            select 'shop-1'::varchar shop_id, 'order-1'::varchar order_id,
                'refund-1'::varchar refund_id, null::varchar return_id,
                null::timestamptz created_at,
                '2026-07-01 00:30:00-07'::timestamptz updated_at,
                null::timestamptz processed_at,
                0.123456789::decimal(38,9) total_refunded_shop_amount,
                'USD'::varchar total_refunded_shop_currency,
                null::decimal(38,9) total_refunded_presentment_amount,
                null::varchar total_refunded_presentment_currency,
                'Private note'::varchar note
        """)
        _ = db.execute("""
            create table raw.orders__refunds__transactions as
            select 'shop-1'::varchar shop_id, 'order-1'::varchar order_id,
                'refund-1'::varchar refund_id, 'tx-4'::varchar transaction_id
        """)


def _build(path: Path, target: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "dbt",
            "build",
            "--select",
            *MODELS,
            "--exclude",
            "tag:shopify_commercial_cross_model",
            "--quiet",
            "--warn-error-options",
            '{"error": ["NoNodesForSelectionCriteria"]}',
            "--project-dir",
            str(ROOT / "dbt"),
            "--profiles-dir",
            str(ROOT / "dbt"),
            "--target-path",
            str(target),
            "--log-path",
            str(target / "logs"),
        ],
        env=os.environ
        | {
            "DBT_TARGET": "local",
            "DBT_NAMESPACE": "payment",
            "DUCKDB_PATH": str(path),
            "SHOPIFY_ORDERS_DATASET": "raw",
        },
        timeout=90,
        capture_output=True,
        text=True,
        check=False,
    )


def test_staging_preserves_payment_evidence(tmp_path: Path) -> None:
    raw = tmp_path / "warehouse.duckdb"
    _raw(raw)
    result = _build(raw, tmp_path / "target")
    assert result.returncode == 0, result.stdout + result.stderr
    with duckdb.connect(str(raw), read_only=True) as db:
        rows = db.execute("""
            select transaction_id, kind, status, is_test
            from payment_staging.stg_shopify_orders__transactions
            order by transaction_id
        """).fetchall()
        assert rows == [
            ("tx-1", "CAPTURE", "SUCCESS", True),
            ("tx-2", "AUTHORIZATION", "PENDING", False),
            ("tx-3", "SALE", "FAILURE", False),
            ("tx-4", "REFUND", "SUCCESS", False),
            ("tx-5", "FUTURE_KIND", "FUTURE_STATUS", False),
        ]
        assert db.execute("""
            select parent_transaction_id, created_at, processed_at, gateway,
                transaction_shop_amount, shop_currency_code,
                transaction_presentment_amount, presentment_currency_code
            from payment_staging.stg_shopify_orders__transactions
            where transaction_id = 'tx-1'
        """).fetchone() == (
            "auth-1",
            datetime(2026, 7, 1, 7, 30),
            None,
            "manual",
            Decimal("0.123456789"),
            "USD",
            Decimal("0.987654321"),
            "CAD",
        )
        assert db.execute("""
            select refund_id, return_id, created_at, updated_at, processed_at,
                total_refunded_shop_amount, total_refunded_presentment_amount,
                shop_currency_code, presentment_currency_code
            from payment_staging.stg_shopify_orders__refunds
        """).fetchone() == (
            "refund-1",
            None,
            None,
            datetime(2026, 7, 1, 7, 30),
            None,
            Decimal("0.123456789"),
            None,
            "USD",
            None,
        )
        assert db.execute("""
            select * from payment_staging.stg_shopify_orders__refund_transactions
        """).fetchall() == [("shop-1", "order-1", "refund-1", "tx-4")]


@pytest.mark.parametrize(
    "table",
    [
        "orders__transactions",
        "orders__refunds",
        "orders__refunds__transactions",
    ],
)
@pytest.mark.parametrize("defect", ["duplicate", "null_key"])
def test_staging_rejects_invalid_keys(tmp_path: Path, table: str, defect: str) -> None:
    raw = tmp_path / "warehouse.duckdb"
    _raw(raw)
    with duckdb.connect(str(raw)) as db:
        if defect == "duplicate":
            _ = db.execute(f"insert into raw.{table} select * from raw.{table} limit 1")
        else:
            _ = db.execute(f"update raw.{table} set shop_id = null")
    result = _build(raw, tmp_path / "target")
    assert result.returncode != 0
    expected = "unique" if defect == "duplicate" else "not_null"
    assert expected in result.stdout, result.stdout + result.stderr


@pytest.mark.parametrize("empty", [False, True])
def test_staging_accepts_scoped_keys_and_empty_sources(
    tmp_path: Path, empty: bool
) -> None:
    raw = tmp_path / "warehouse.duckdb"
    _raw(raw)
    with duckdb.connect(str(raw)) as db:
        for table in (
            "orders__transactions",
            "orders__refunds",
            "orders__refunds__transactions",
        ):
            if empty:
                _ = db.execute(f"delete from raw.{table}")
            else:
                _ = db.execute(
                    f"insert into raw.{table} "
                    f"select * replace ('shop-2' as shop_id) from raw.{table}"
                )
    result = _build(raw, tmp_path / "target")
    assert result.returncode == 0, result.stdout + result.stderr
    with duckdb.connect(str(raw), read_only=True) as db:
        for model, count in zip(MODELS, (10, 2, 2), strict=True):
            assert db.execute(
                f"select count(*) from payment_staging.{model}"
            ).fetchone() == (0 if empty else count,)
