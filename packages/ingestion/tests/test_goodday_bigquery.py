"""BigQuery boundaries are checked without credentials or paid queries."""

from decimal import Decimal
from typing import cast

import pytest
import sqlalchemy as sa

from analytics_ingestion.goodday.bigquery import (
    SourceSettings,
    engine_from_info,
    read_inventory,
)

SETTINGS = SourceSettings("source-project", "merchant", "billing-project", "US")


def test_source_requires_valid_identifiers() -> None:
    with pytest.raises(ValueError, match="project"):
        SourceSettings("bad`project", "merchant", "billing", "US")
    with pytest.raises(ValueError, match="dataset"):
        SourceSettings("source", "bad.dataset", "billing", "US")


def test_engine_uses_only_the_supplied_account_and_billing_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import bigquery
    from google.oauth2 import service_account

    seen: list[object] = []

    def credentials(info: object, **_: object) -> AnonymousCredentials:
        seen.append(info)
        return AnonymousCredentials()

    monkeypatch.setattr(
        service_account.Credentials, "from_service_account_info", credentials
    )
    info: dict[str, object] = {"type": "service_account"}
    engine = engine_from_info(SETTINGS, info)
    with engine.connect() as connection:
        raw = connection.connection.dbapi_connection
        client = cast(object, raw._client)  # pyright: ignore[reportOptionalMemberAccess]  # DB-API holds the supplied client.
    assert seen == [info]
    assert isinstance(client, bigquery.Client)
    assert client.project == "billing-project"
    assert client.location == "US"
    config = client.default_query_job_config
    assert isinstance(config, bigquery.QueryJobConfig)
    assert config.use_query_cache is False  # pyright: ignore[reportUnknownMemberType]  # SDK property is untyped.
    table = sa.Table("inventory_by_item", sa.MetaData(), schema=SETTINGS.schema)
    assert "`source-project.merchant`.`inventory_by_item`" in str(
        sa.select(sa.literal(1)).select_from(table).compile(dialect=engine.dialect)
    )


def test_invalid_credential_error_hides_its_contents() -> None:
    with pytest.raises(ValueError, match="credential is invalid") as error:
        engine_from_info(SETTINGS, {"private_key": "secret-marker"})
    assert "secret-marker" not in str(error.value)
    assert error.value.__cause__ is None


def test_reflected_columns_keep_types_and_get_descriptions() -> None:
    engine = sa.create_engine("sqlite://")
    with engine.begin() as connection:
        _ = connection.exec_driver_sql(
            "create table inventory_by_item (item_id integer not null, "
            "avg_cost_per_unit numeric(38, 9), new_vendor_field text)"
        )
        _ = connection.exec_driver_sql(
            "insert into inventory_by_item values (1, 12.5, 'x'), (2, null, null)"
        )

    snapshot = read_inventory(engine, "main", "fixture.merchant.inventory_by_item")

    assert snapshot.source_relation == "fixture.merchant.inventory_by_item"
    assert snapshot.columns["item_id"].get("nullable") is False
    assert snapshot.columns["avg_cost_per_unit"].get("scale") == 9
    assert "currency" in str(snapshot.columns["avg_cost_per_unit"].get("description"))
    assert snapshot.columns["new_vendor_field"].get("description")
    assert list(snapshot.rows()) == [
        {"item_id": 1, "avg_cost_per_unit": Decimal("12.5"), "new_vendor_field": "x"},
        {"item_id": 2, "avg_cost_per_unit": None, "new_vendor_field": None},
    ]


def test_timestamps_are_instants_and_datetimes_stay_naive() -> None:
    # BigQuery TIMESTAMP is an absolute instant; DATETIME has no time zone.
    # The SQLite stand-in reflects both names to the same SQLAlchemy types.
    engine = sa.create_engine("sqlite://")
    with engine.begin() as connection:
        _ = connection.exec_driver_sql(
            "create table inventory_by_item (updated_at timestamp, local_at datetime)"
        )

    columns = read_inventory(engine, "main", "fixture").columns

    # Aware is dlt's default; an explicit hint would break the frozen schema
    # of captures stored before this reader.
    assert "timezone" not in columns["updated_at"]
    assert columns["local_at"].get("timezone") is False
