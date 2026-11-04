"""Raw GoodDay inventory shaped like the loader's output: three captures, one
of them from a failed load."""

from decimal import Decimal
from pathlib import Path

import duckdb

QUANTITIES = (
    "on_hand_total",
    "on_hand_sellable",
    "on_hand_unsellable",
    "on_hand_reserved",
    "on_hand_committed",
    "on_hand_available",
    "available",
    "ats",
    "inbound_total",
    "inbound_available",
    "fulfilled_total",
    "transfer_in_transit",
    "return_total",
)


def write_inventory(path: Path) -> None:
    with duckdb.connect(str(path)) as db:
        db.execute("create schema goodday_raw")
        db.execute(
            "create table goodday_raw._dlt_loads (load_id varchar, status bigint)"
        )
        db.execute(
            "insert into goodday_raw._dlt_loads values ('old',0),('new',0),('failed',1)"
        )
        db.execute("""create table goodday_raw.inventory_captures (
            capture_id varchar, _dlt_load_id varchar, captured_at timestamptz,
            observed_at timestamptz, source_relation varchar,
            row_count bigint)""")
        db.execute("""insert into goodday_raw.inventory_captures values
            ('old','old','2026-10-26 10:00:01+00','2026-10-26 10:00:00+00',
             'vendor.inventory',1),
            ('new','new','2026-10-26 11:00:01+00','2026-10-26 11:00:00+00',
             'vendor.inventory',1),
            ('failed','failed','2026-10-26 12:00:01+00','2026-10-26 12:00:00+00',
             'vendor.inventory',1)
        """)
        columns = ", ".join(f"{name} decimal(38,9)" for name in QUANTITIES)
        db.execute(f"""create table goodday_raw.inventory_by_item (
            _capture_id varchar, _dlt_load_id varchar, _row_number bigint,
            merchant_id bigint, item_id bigint, channel_id bigint, location_id bigint,
            parent_item_id bigint, merchant_name varchar, item_name varchar,
            item_status varchar, sku varchar, channel_name varchar,
            location_name varchar,
            updated_at timestamptz, {columns})""")
        db.execute("""insert into goodday_raw.inventory_by_item
            (_capture_id, _dlt_load_id, _row_number, merchant_id, item_id, channel_id,
             location_id, sku, updated_at, on_hand_total, ats, available)
            values ('old','old',1,58,1,1,1,'OLD','2026-10-25 00:00:00+00',100,100,100),
                   ('new','new',1,58,2,null,null,' AbC ','2026-10-25 00:00:00+00',
                    -1.123456789,0,null),
                   ('failed','failed',1,58,3,1,1,'FAILED','2026-10-25 00:00:00+00',
                    999,999,999)
        """)
        for index, name in enumerate(QUANTITIES):
            if name not in {"on_hand_total", "ats", "available"}:
                db.execute(
                    f"update goodday_raw.inventory_by_item set {name}=? "
                    "where _capture_id='new'",
                    [Decimal(index) + Decimal("0.000000001")],
                )
