"""GoodDay inventory through dlt's ``sql_database`` source over BigQuery.

dlt reflects the source view's column types and streams the query result in
chunks. The engine uses only the supplied service account and billing project,
never generic Google credentials. Query cost and duration are not capped here;
the Dagster run time limit bounds each run.
"""

import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime

import sqlalchemy as sa
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.sources.sql_database import sql_table
from dlt.sources.sql_database.schema_types import TypeEngineAny
from google.cloud import bigquery
from google.oauth2 import service_account

TABLE = "inventory_by_item"
_PROJECT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*")
DESCRIPTIONS = {
    "merchant_id": "GoodDay merchant identity owning the inventory balance.",
    "item_id": "GoodDay item identity, not an analytical product key.",
    "location_id": "GoodDay location identity; source nulls are retained.",
    "channel_id": "GoodDay channel identity; source nulls are retained.",
    "sku": "Item SKU as supplied by GoodDay, without normalization.",
    "item_name": "GoodDay item display name at capture time.",
    "item_status": "Source-reported item lifecycle status.",
    "parent_item_id": "GoodDay parent item identity for this variant.",
    "location_name": "Source-reported inventory location label.",
    "channel_name": "Source-reported inventory channel label.",
    "on_hand_total": "Total on-hand units reported by GoodDay.",
    "on_hand_sellable": "On-hand units classified as sellable by GoodDay.",
    "on_hand_unsellable": "On-hand units classified as unsellable by GoodDay.",
    "on_hand_reserved": "On-hand units classified as reserved by GoodDay.",
    "on_hand_committed": "On-hand units classified as committed by GoodDay.",
    "on_hand_available": "On-hand units classified as available by GoodDay.",
    "available": "Source availability, not equated with on-hand availability.",
    "ats": "GoodDay available-to-sell quantity, retained without recalculation.",
    "inbound_total": "Total inbound units reported by GoodDay.",
    "inbound_available": "Inbound units classified as available by GoodDay.",
    "fulfilled_total": "Fulfilled quantity reported by GoodDay.",
    "transfer_in_transit": "Transfer quantity reported in transit by GoodDay.",
    "return_total": "Return quantity reported by GoodDay.",
    "avg_cost_per_unit": "Source average unit cost; currency and basis are unverified.",
    "updated_at": "Source update instant; not evidence of complete view refresh.",
}


@dataclass(frozen=True)
class SourceSettings:
    project: str
    dataset: str
    billing_project: str
    location: str

    def __post_init__(self) -> None:
        for name, value in (
            ("project", self.project),
            ("billing_project", self.billing_project),
        ):
            if not _PROJECT.fullmatch(value):
                raise ValueError(f"GoodDay {name} is invalid")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.dataset):
            raise ValueError("GoodDay dataset is invalid")
        if not re.fullmatch(r"[A-Za-z0-9-]+", self.location):
            raise ValueError("GoodDay location is invalid")

    @property
    def schema(self) -> str:
        return f"{self.project}.{self.dataset}"

    @property
    def relation(self) -> str:
        return f"{self.schema}.{TABLE}"


@dataclass(frozen=True)
class Snapshot:
    source_relation: str
    observed_at: datetime
    columns: TTableSchemaColumns
    _rows: Iterator[dict[str, object]]

    def rows(self) -> Iterator[dict[str, object]]:
        return self._rows


def _instant(sql_type: TypeEngineAny) -> TypeEngineAny:
    """BigQuery TIMESTAMP is an absolute instant; the driver reflects it naive."""
    if isinstance(sql_type, sa.TIMESTAMP):
        return sa.TIMESTAMP(timezone=True)
    return sql_type


def read_inventory(engine: sa.Engine, schema: str, relation: str) -> Snapshot:
    """Reflect the view now; the query runs as rows are read."""
    resource = sql_table(
        credentials=engine,
        table=TABLE,
        schema=schema,
        reflection_level="full_with_precision",
        chunk_size=1000,
        type_adapter_callback=_instant,
    )
    columns: TTableSchemaColumns = {}
    for name, reflected in resource.compute_table_schema().get("columns", {}).items():
        column = reflected.copy()
        if column.get("timezone") is True:
            # dlt's default; existing captures were stored without the hint.
            del column["timezone"]
        column["description"] = DESCRIPTIONS.get(
            name, "Uninterpreted source field; business meaning is not governed."
        )
        columns[name] = column
    return Snapshot(relation, datetime.now(UTC), columns, iter(resource))


def engine_from_info(settings: SourceSettings, info: dict[str, object]) -> sa.Engine:
    """Never use generic Google credentials or an implicit billing project."""
    try:
        credentials = service_account.Credentials.from_service_account_info(  # pyright: ignore[reportUnknownMemberType]  # SDK factory lacks typed kwargs.
            info, scopes=["https://www.googleapis.com/auth/bigquery"]
        )
    except Exception:
        raise ValueError("GoodDay service-account credential is invalid") from None
    client = bigquery.Client(
        project=settings.billing_project,
        credentials=credentials,
        location=settings.location,
        default_query_job_config=bigquery.QueryJobConfig(use_query_cache=False),
    )
    return sa.create_engine(
        "bigquery://?user_supplied_client=True", connect_args={"client": client}
    )
