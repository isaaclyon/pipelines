import base64
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import dagster as dg
import pytest

from analytics_transformations.defs import dbt_docs as module
from analytics_transformations.mart_contracts import mapping, sequence, text

# Docs and lineage cover the whole real dbt project (ADR 0026). dbt reports no
# executed nodes for docs, so the suite cannot check this declaration.
pytestmark = pytest.mark.dbt("fqn:*", "source:*")

DBT = Path(__file__).parents[1] / "dbt"


def _env(tmp_path: Path, **overrides: str) -> dict[str, str]:
    return {
        "ANALYTICS_ENVIRONMENT": "local",
        "DBT_TARGET": "local",
        "DUCKDB_PATH": str(tmp_path / "warehouse.duckdb"),
        "DBT_PROJECT_DIR": str(DBT),
        "COLIBRI_EXECUTABLE": os.environ.get(
            "COLIBRI_EXECUTABLE", str(DBT.parent / "lineage/.venv/bin/colibri")
        ),
        "DBT_PROFILES_DIR": str(DBT),
        **overrides,
    }


def _lineage(page: Path) -> set[tuple[str, str, str, str]]:
    """Column edges in the data a colibri page embeds."""
    encoded = re.search(r'colibriData = JSON\.parse\(atob\("([^"]+)"', page.read_text())
    assert encoded
    data = mapping(json.loads(base64.b64decode(encoded[1])))  # pyright: ignore[reportAny]
    edges = (mapping(edge) for edge in sequence(mapping(data["lineage"])["edges"]))
    # Table-level edges, such as joins, carry empty column names.
    return {
        (
            text(edge["source"]),
            str(edge["sourceColumn"]),
            text(edge["target"]),
            str(edge["targetColumn"]),
        )
        for edge in edges
    }


def test_build_writes_docs_and_column_lineage_pages(tmp_path: Path) -> None:
    site = tmp_path / "docs"
    site.mkdir()
    pages = module.build_docs(_env(tmp_path), site)
    assert pages == (site / "index.html", site / "lineage" / "index.html")
    html = pages[0].read_text()
    # The static page embeds the manifest, so documented models are inside it.
    assert "fct_orders" in html
    assert "stg_shopify_orders__orders" in html
    assert sorted(path.name for path in site.iterdir()) == ["index.html", "lineage"]
    assert [path.name for path in (site / "lineage").iterdir()] == ["index.html"]
    for page in pages:
        assert page.stat().st_mode & 0o777 == 0o644
    # The warehouse is empty, as staging and intermediate relations are in
    # deployments, yet column lineage still traces through them.
    # Both pages are built with usage tracking off.
    assert '"send_anonymous_usage_stats":false' in html.replace(" ", "")
    assert "window.colibriTelemetry = false" in pages[1].read_text()
    traced = _lineage(pages[1])
    assert (
        "model.analytics.stg_shopify_orders__orders",
        "source_name",
        "model.analytics.int_shopify_orders__classifications",
        "is_retail",
    ) in traced
    assert (
        "model.analytics.int_shopify_orders__classifications",
        "is_retail",
        "model.analytics.fct_orders",
        "is_retail",
    ) in traced


def test_lineage_catalog_declares_relations_the_warehouse_lacks() -> None:
    manifest = {
        "nodes": {
            "model.p.built": {"resource_type": "model"},
            "model.p.staged": {
                "resource_type": "model",
                "database": "db",
                "schema": "s",
                "name": "staged",
                "alias": "staged",
                "columns": {
                    "id": {"name": "id", "data_type": "varchar"},
                    "amount": {"name": "amount", "data_type": "number(12,2)"},
                },
            },
            "test.p.check": {"resource_type": "test"},
        }
    }
    built = {"metadata": {"name": "built"}, "columns": {}}
    catalog = {"metadata": {}, "nodes": {"model.p.built": built}, "sources": {}}
    nodes = mapping(module.lineage_catalog(manifest, catalog)["nodes"])
    assert nodes["model.p.built"] is built
    assert set(nodes) == {"model.p.built", "model.p.staged"}
    assert mapping(nodes["model.p.staged"])["columns"] == {
        "id": {"name": "id", "type": "varchar", "index": 1, "comment": None},
        "amount": {
            "name": "amount",
            "type": "number(12,2)",
            "index": 2,
            "comment": None,
        },
    }


@pytest.mark.parametrize("environment", ["", "staging"], ids=["unset", "unknown"])
def test_build_requires_an_explicit_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, environment: str
) -> None:
    calls: list[list[str]] = []

    def record(command: list[str], **_: object) -> None:
        calls.append(command)

    monkeypatch.setattr(module.subprocess, "run", record)
    with pytest.raises(ValueError, match="environment"):
        _ = module.build_docs(
            _env(tmp_path, ANALYTICS_ENVIRONMENT=environment), tmp_path
        )
    assert calls == []


def test_failed_build_keeps_the_previous_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    site = tmp_path / "docs"
    site.mkdir()
    (site / "index.html").write_text("previous")

    def fail(command: list[str], **_: object) -> None:
        raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(module.subprocess, "run", fail)
    with pytest.raises(RuntimeError, match="dbt docs generate failed"):
        _ = module.build_docs(_env(tmp_path), site)
    assert [path.name for path in site.iterdir()] == ["index.html"]
    assert (site / "index.html").read_text() == "previous"


def test_failed_lineage_keeps_both_previous_pages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    site = tmp_path / "docs"
    (site / "lineage").mkdir(parents=True)
    (site / "index.html").write_text("previous docs")
    (site / "lineage" / "index.html").write_text("previous lineage")
    run = subprocess.run

    def fail_lineage(command: list[str], **kwargs: object) -> None:
        if Path(command[0]).name == "colibri":
            raise subprocess.CalledProcessError(1, command)
        _ = run(command, **kwargs)  # pyright: ignore[reportCallIssue, reportArgumentType, reportUnknownVariableType]

    monkeypatch.setattr(module.subprocess, "run", fail_lineage)
    with pytest.raises(RuntimeError, match="colibri generate failed"):
        _ = module.build_docs(_env(tmp_path), site)
    assert (site / "index.html").read_text() == "previous docs"
    assert (site / "lineage" / "index.html").read_text() == "previous lineage"
    assert sorted(path.name for path in site.rglob("*")) == [
        "index.html",
        "index.html",
        "lineage",
    ]


def test_namespace_follows_the_published_marts_schema() -> None:
    env = {"ANALYTICS_ENVIRONMENT": "development"}
    assert module.docs_namespace(env) == "development"
    marts = {**env, "ANALYTICS_MARTS_SCHEMA": "Development_Marts"}
    assert module.docs_namespace(marts) == "development"
    with pytest.raises(ValueError, match="_marts"):
        _ = module.docs_namespace({**env, "ANALYTICS_MARTS_SCHEMA": "development"})


def test_sensor_requests_one_build_per_release_image() -> None:
    image = "ghcr.io/example/transformations@sha256:" + "a" * 64
    request = module.release_request({"DAGSTER_CURRENT_IMAGE": image})
    assert isinstance(request, dg.RunRequest)
    assert request.run_key == f"{module.JOB_NAME}:{image}"
    assert isinstance(module.release_request({}), dg.SkipReason)


def test_sensor_starts_stopped() -> None:
    sensor = module.defs.resolve_sensor_def(module.SENSOR_NAME)
    assert sensor.default_status == dg.DefaultSensorStatus.STOPPED


def test_docs_describe_each_groups_published_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observed: dict[str, object] = {}
    lineage = module.lineage_catalog

    def capture(manifest: object, catalog: object) -> dict[str, object]:
        observed.update(mapping(mapping(manifest)["nodes"]))
        return lineage(manifest, catalog)

    monkeypatch.setattr(module, "lineage_catalog", capture)
    site = tmp_path / "docs"
    site.mkdir()
    module.build_docs(
        _env(
            tmp_path,
            ANALYTICS_MARTS_SCHEMA="local_orders_marts",
            ANALYTICS_CUSTOMERS_MARTS_SCHEMA="local_customers_marts",
            ANALYTICS_INVENTORY_MARTS_SCHEMA="local_inventory_marts",
            SHOPIFY_CUSTOMERS_ENABLED="true",
            GOODDAY_INVENTORY_ENABLED="true",
        ),
        site,
    )
    for model, schema in {
        "fct_orders": "local_orders_marts",
        "dim_order_contacts": "local_orders_marts",
        "dim_customers": "local_customers_marts",
        "dim_customer_details": "local_customers_marts",
        "fct_inventory__current": "local_inventory_marts",
    }.items():
        assert mapping(observed[f"model.analytics.{model}"])["schema"] == schema


@pytest.mark.parametrize("documentation", [True, False])
@pytest.mark.parametrize("independent", [True, False])
def test_snowflake_docs_route_without_changing_candidate_builds(
    tmp_path: Path, documentation: bool, independent: bool
) -> None:
    target = tmp_path / "target"
    subprocess.run(
        [
            str(Path(sys.executable).with_name("dbt")),
            "parse",
            "--project-dir",
            str(DBT),
            "--profiles-dir",
            str(DBT),
            "--target-path",
            str(target),
            "--no-partial-parse",
            "--vars",
            json.dumps({"documentation_publications": documentation}),
        ],
        env=_env(
            tmp_path,
            DBT_TARGET="snowflake-development",
            DBT_NAMESPACE="candidate",
            SNOWFLAKE_ACCOUNT="fixture",
            SNOWFLAKE_USER="fixture",
            SNOWFLAKE_DATABASE="fixture",
            ANALYTICS_MARTS_SCHEMA="dev_orders_marts",
            ANALYTICS_CUSTOMERS_MARTS_SCHEMA="dev_customers_marts"
            if independent
            else "",
            ANALYTICS_INVENTORY_MARTS_SCHEMA="dev_inventory_marts"
            if independent
            else "",
            SHOPIFY_CUSTOMERS_ENABLED="true",
            GOODDAY_INVENTORY_ENABLED="true",
        ),
        check=True,
        timeout=60,
    )
    nodes = mapping(mapping(module.read_json(target / "manifest.json"))["nodes"])
    for model, schema in {
        "fct_orders": "dev_orders_marts",
        "dim_order_contacts": "dev_orders_marts",
        "dim_customers": "dev_customers_marts",
        "dim_customer_details": "dev_customers_marts",
        "fct_inventory__current": "dev_inventory_marts",
    }.items():
        expected = "candidate_marts"
        if documentation:
            expected = (
                schema
                if independent or schema == "dev_orders_marts"
                else "candidate_marts"
            )
        assert mapping(nodes[f"model.analytics.{model}"])["schema"] == expected
