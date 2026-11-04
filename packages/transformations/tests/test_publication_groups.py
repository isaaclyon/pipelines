"""A failed business question cannot prevent an unrelated publication."""

import json
import shutil
from pathlib import Path

import duckdb
import pytest

from analytics_transformations.mart_contracts import mapping
from analytics_transformations.publication import validate_results
from tests.test_mart_contracts import manifest

pytestmark = pytest.mark.dbt(files=("dbt/macros",))

ROOT = Path(__file__).parents[1]


def grouped_manifest() -> dict[str, object]:
    value = manifest()
    nodes = mapping(value["nodes"])
    model = mapping(nodes["model.analytics.fct_examples"])
    config = mapping(model["config"])
    config["meta"] = mapping(config["meta"]) | {"publication_group": "orders"}
    model["config"] = config
    nodes["model.analytics.fct_examples"] = model
    # A second, independent set of contracts and checks.
    inventory = mapping(json.loads(json.dumps(nodes).replace("examples", "inventory")))  # pyright: ignore[reportAny] # JSON is parsed immediately.
    other = mapping(inventory["model.analytics.fct_inventory"])
    other_config = mapping(other["config"])
    other_config["meta"] = mapping(other_config["meta"]) | {
        "publication_group": "inventory"
    }
    other["config"] = other_config
    inventory["model.analytics.fct_inventory"] = other
    value["nodes"] = nodes | inventory
    value["metadata"] = {"invocation_id": "run"}
    return value


def test_results_require_only_the_selected_group() -> None:
    value = grouped_manifest()
    results = {
        "metadata": {"invocation_id": "run"},
        "results": [
            {"unique_id": node_id, "status": "pass"}
            for node_id in mapping(value["nodes"])
            if "examples" in node_id
        ],
    }
    plan = validate_results(value, results, publication_group="orders")
    assert [mart.relation for mart in plan] == ["fct_examples"]
    with pytest.raises(ValueError, match="lacks required"):
        validate_results(value, results, publication_group="inventory")
    with pytest.raises(ValueError, match="publication group"):
        validate_results(value, results, publication_group="missing")
    with pytest.raises(ValueError, match="publication group"):
        validate_results(value, results)


@pytest.fixture
def grouped_project(tmp_path: Path) -> Path:
    project = tmp_path / "project"
    shutil.copytree(ROOT / "tests/fixtures/publication", project)
    shutil.copytree(ROOT / "dbt/macros", project / "macros")
    mart = project / "models/marts/fct_observations.sql"
    mart.write_text(
        "{{ config(meta={'publication_group': 'orders'}) }}\n" + mart.read_text()
    )
    # Inventory shares an ancestor but not a publication boundary. Its bad check
    # must not be pulled into an order build through eager test selection.
    inventory = project / "models/marts/fct_inventory.sql"
    inventory.write_text(
        "{{ config(meta={'publication_group': 'inventory'}) }}\n"
        + mart.read_text().split("\n", 1)[1]
    )
    schema = project / "models/marts/schema.yml"
    schema.write_text(
        schema.read_text()
        + schema.read_text()
        .partition("models:\n")[2]
        .replace("fct_observations", "fct_inventory")
        .replace("observation_reconciliation", "inventory_reconciliation")
    )
    reconcile = project / "tests/observation_reconciliation.sql"
    (project / "tests/inventory_reconciliation.sql").write_text(
        reconcile.read_text().replace("fct_observations", "fct_inventory")
    )
    return project


def test_independent_candidates_ignore_another_groups_failed_check(
    tmp_path: Path, grouped_project: Path
) -> None:
    from tests.model_build import build_models

    raw = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(raw)):
        pass
    inventory = grouped_project / "models/marts/fct_inventory.sql"
    inventory.write_text(
        inventory.read_text().replace(
            "observed_amount", "-observed_amount as observed_amount"
        )
    )
    with pytest.raises(RuntimeError):
        build_models(
            raw,
            namespace="ci_inventory",
            project=grouped_project,
            publication_group="inventory",
        )
    build_models(
        raw, namespace="ci_orders", project=grouped_project, publication_group="orders"
    )
    with duckdb.connect(str(raw), read_only=True) as db:
        assert db.execute(
            "select observed_amount from ci_orders_marts.fct_observations"
        ).fetchone() == (100,)
        assert db.execute(
            "select table_name from information_schema.tables "
            "where table_schema='ci_orders_marts'"
        ).fetchall() == [("fct_observations",)]


def test_cross_group_dependencies_and_checks_fail_closed() -> None:
    from analytics_transformations.publication import publication_manifest

    for node_id in (
        "model.analytics.fct_examples",
        "test.analytics.reconcile_examples",
    ):
        value = grouped_manifest()
        nodes = mapping(value["nodes"])
        node = mapping(nodes[node_id])
        node["depends_on"] = {
            "nodes": ["model.analytics.fct_examples", "model.analytics.fct_inventory"]
        }
        nodes[node_id] = node
        value["nodes"] = nodes
        with pytest.raises(ValueError, match="publication group"):
            publication_manifest(value, "orders")


@pytest.mark.dbt("+path:models/marts/shopify_orders", "source:shopify_orders")
def test_orders_and_availability_publish_despite_bad_inventory_and_missing_customers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.shopify_order_fixtures import GOOD_LINES, GOOD_ORDERS, write_raw
    from tests.test_goodday_inventory import write_inventory

    raw = tmp_path / "warehouse.duckdb"
    write_raw(raw, GOOD_ORDERS, GOOD_LINES)
    write_inventory(raw)
    monkeypatch.setenv("SHOPIFY_ORDERS_DATASET", "raw")
    monkeypatch.setenv("SHOPIFY_CUSTOMERS_ENABLED", "true")
    monkeypatch.setenv("GOODDAY_INVENTORY_ENABLED", "true")
    monkeypatch.setenv("GOODDAY_DATASET", "goodday_raw")
    with duckdb.connect(str(raw)) as db:
        db.execute("update goodday_raw.inventory_captures set row_count=99")
    from tests.model_build import build_models

    result = build_models(raw, namespace="ci_orders", publication_group="orders")
    with duckdb.connect(str(raw), read_only=True) as db:
        for relation in ("fct_orders", "fct_data_availability"):
            assert db.execute(
                f"select distinct publication_id from ci_orders_marts.{relation}"
            ).fetchall() == [(result,)]
        assert (
            db.execute(
                "select table_name from information_schema.tables "
                "where table_schema='ci_orders_marts' "
                "and table_name in ('dim_customers', 'fct_inventory__current')"
            ).fetchall()
            == []
        )


def test_snowflake_uncertain_group_does_not_hold_another_groups_lease(
    tmp_path: Path,
) -> None:
    from collections.abc import Mapping, Sequence

    from analytics_transformations.publication import publish_snowflake_marts

    leases: set[str] = set()
    promoted: dict[str, object] = {}

    def runner(command: Sequence[str], env: Mapping[str, str], timeout: int) -> None:
        if command[1] == "run-operation":
            operation = command[2]
            if operation == "validate_model_outputs":
                return
            args = mapping(json.loads(command[command.index("--args") + 1]))  # pyright: ignore[reportAny] # Parse JSON at the CLI boundary.
            namespace = str(args["namespace"])
            assert str(args["candidate_namespace"]).startswith(
                namespace + "_candidate_"
            )
            if operation == "prepare_mart_candidate":
                if namespace in leases:
                    raise RuntimeError("occupied lease")
                leases.add(namespace)
            elif operation in ("publish_mart_candidate", "confirm_mart_publication"):
                if namespace == "ci_inventory":
                    raise RuntimeError("uncertain inventory swap")
                promoted[namespace] = args["relations"]
            elif operation == "clean_mart_candidate":
                leases.remove(namespace)
            return
        value = grouped_manifest()
        value["metadata"] = {
            "project_name": "publication_fixture",
            "invocation_id": "run",
        }
        nodes = mapping(value["nodes"])
        for key, item in nodes.items():
            node = mapping(item)
            if node.get("resource_type") == "model":
                node["schema"] = env["DBT_NAMESPACE"] + "_marts"
                node["package_name"] = "publication_fixture"
                node["relation_name"] = str(node["schema"]) + "." + str(node["alias"])
                columns = mapping(node["columns"])
                column = mapping(columns["observation_id"])
                column["config"] = {
                    "meta": {
                        "pii": False,
                        "identity": {
                            "kind": "analytical",
                            "meaning": "Fixture observation",
                        },
                    }
                }
                node["columns"] = columns | {"observation_id": column}
                nodes[key] = node
        value["nodes"] = nodes
        target = Path(command[command.index("--target-path") + 1])
        target.mkdir(parents=True, exist_ok=True)
        (target / "manifest.json").write_text(json.dumps(value))
        selected = (
            "inventory"
            if env["DBT_NAMESPACE"].startswith("ci_inventory_")
            else "examples"
        )
        (target / "run_results.json").write_text(
            json.dumps(
                {
                    "metadata": {"invocation_id": "run"},
                    "results": [
                        {"unique_id": key, "status": "pass"}
                        for key in nodes
                        if selected in key
                    ],
                }
            )
        )

    def publish(group: str) -> None:
        publish_snowflake_marts(
            environment="development",
            namespace="ci_" + group,
            publication_group=group,
            project_dir=tmp_path,
            profiles_dir=tmp_path,
            reader_role="reader",
            runner=runner,
        )

    with pytest.raises(RuntimeError, match="uncertain inventory"):
        publish("inventory")
    assert leases == {"ci_inventory"}
    publish("orders")
    assert leases == {"ci_inventory"}
    assert promoted == {"ci_orders": ["fct_examples"]}
    with pytest.raises(RuntimeError, match="occupied lease"):
        publish("inventory")


@pytest.mark.parametrize(
    "lookup",
    ["model.analytics.stg_reference__subjects", "model.analytics.fct_inventory"],
)
def test_ancestor_check_cannot_disappear_when_its_reference_is_outside_scope(
    lookup: str,
) -> None:
    value = grouped_manifest()
    nodes = mapping(value["nodes"])
    mart = mapping(nodes["model.analytics.fct_examples"])
    ancestor = "model.analytics.stg_fixture__subjects"
    mart["depends_on"] = {"nodes": [ancestor]}
    nodes["model.analytics.fct_examples"] = mart
    nodes[ancestor] = {
        "resource_type": "model",
        "config": {"materialized": "view"},
        "depends_on": {"nodes": []},
    }
    nodes.setdefault(lookup, nodes[ancestor])
    nodes["test.analytics.subject_lookup"] = {
        "resource_type": "test",
        "attached_node": ancestor,
        "depends_on": {"nodes": [ancestor, lookup]},
        "config": {"severity": "error", "meta": {"publication_blocking": True}},
    }
    value["nodes"] = nodes
    results = {
        "metadata": {"invocation_id": "run"},
        "results": [
            {"unique_id": node_id, "status": "pass"}
            for node_id in nodes
            if node_id != "test.analytics.subject_lookup"
        ],
    }
    with pytest.raises(ValueError, match="outside.*publication group"):
        validate_results(value, results, publication_group="orders")


def test_grouped_jobs_load_and_emit_events_for_shared_ancestors(
    tmp_path: Path, grouped_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os
    from datetime import UTC, datetime

    import dagster as dg

    from analytics_transformations.dbt_execution import CandidateRunner
    from analytics_transformations.defs import development_publication as module
    from analytics_transformations.publication import PublicationResult, build_candidate

    # Both groups own a candidate copy of this model and its attached check.
    schema = grouped_project / "models/staging/schema.yml"
    schema.write_text(
        schema.read_text().replace(
            "      - name: observation_id\n",
            "      - name: observation_id\n        data_tests: [not_null]\n",
        )
    )
    for key, value in {
        "DBT_PROJECT_DIR": str(grouped_project),
        "DBT_PROFILES_DIR": str(ROOT / "dbt"),
        "GOODDAY_INVENTORY_ENABLED": "true",
        "ANALYTICS_MARTS_SCHEMA": "ci_orders_marts",
        "ANALYTICS_INVENTORY_MARTS_SCHEMA": "ci_inventory_marts",
        "ANALYTICS_MARTS_READER_ROLE": "reader",
    }.items():
        monkeypatch.setenv(key, value)
    definitions = module.build_definitions(os.environ)
    dg.Definitions.validate_loadable(definitions)

    def publish(**arguments: object) -> PublicationResult:
        group = str(arguments["publication_group"])
        runner = arguments["runner"]
        assert isinstance(runner, CandidateRunner)
        env = dict(os.environ) | {
            "DBT_TARGET": "local",
            "DBT_NAMESPACE": "ci_" + group,
            "DUCKDB_PATH": str(tmp_path / (group + ".duckdb")),
        }
        build_candidate(
            grouped_project,
            ROOT / "dbt",
            tmp_path / group,
            env,
            240,
            runner,
            publication_group=group,
        )
        return PublicationResult("ci_" + group, group, datetime.now(UTC))

    monkeypatch.setattr(module, "publish_snowflake_marts", publish)
    for group in ("orders", "inventory"):
        job_name, publication, _, _ = module.GROUP_SETTINGS[group]
        ancestor = dg.AssetKey([group, "stg_fixture__observations"])
        assert ancestor in definitions.resolve_assets_def(publication).keys
        result = definitions.resolve_job_def(job_name).execute_in_process()
        assert result.success
        assert ancestor in {
            event.asset_key for event in result.get_asset_materialization_events()
        }
        assert any(
            check.asset_key == ancestor and check.passed
            for check in result.get_asset_check_evaluations()
        )
