"""Published relations retain their physical shape and require meaningful gates."""

import json
from pathlib import Path
from typing import cast

import pytest

from analytics_transformations.mart_contracts import compatible, contracts, identifier


def manifest() -> dict[str, object]:
    return {
        "nodes": {
            "model.analytics.fct_examples": {
                "resource_type": "model",
                "name": "fct_examples",
                "fqn": ["analytics", "fct_examples"],
                "original_file_path": "models/marts/fct_examples.sql",
                "alias": "fct_examples",
                "description": "Synthetic observations.",
                "config": {
                    "materialized": "table",
                    "access": "public",
                    "contract": {"enforced": True},
                    "meta": {
                        "owner": "Analytics Platform",
                        "schema_description": "Synthetic published tables.",
                        "publication": {
                            "access": "analytical",
                            "grain": "One row per observation",
                            "key": ["observation_id"],
                            "checks": [
                                "unique_examples",
                                "not_null_examples",
                                "reconcile_examples",
                            ],
                        },
                    },
                },
                "columns": {
                    "observation_id": {
                        "data_type": "bigint",
                        "description": "Stable observation identity.",
                        "config": {"meta": {"pii": False}},
                    }
                },
                "depends_on": {"nodes": []},
            },
            **{
                f"test.analytics.{name}": {
                    "resource_type": "test",
                    "name": name,
                    "fqn": ["analytics", name],
                    "depends_on": {"nodes": ["model.analytics.fct_examples"]},
                    "config": {
                        "severity": "ERROR",
                    },
                    "test_metadata": {
                        "name": kind,
                        "kwargs": {"column_name": "observation_id"},
                    },
                }
                for name, kind in [
                    ("unique_examples", "unique"),
                    ("not_null_examples", "not_null"),
                    ("reconcile_examples", "reconciliation"),
                ]
            },
        }
    }


def test_portable_identifier_limit_matches_snowflake() -> None:
    assert identifier("a" * 255) == "a" * 255
    with pytest.raises(ValueError):
        identifier("a" * 256)


def test_published_shape_requires_documented_contract() -> None:
    plan = contracts(manifest())
    assert len(plan) == 1
    assert plan[0].key == ("observation_id",)
    assert len(plan[0].checks) == 3


def test_contract_removal_is_breaking() -> None:
    with pytest.raises(ValueError, match="removed"):
        compatible(contracts(manifest()), ())


def test_undocumented_mart_fails_closed() -> None:
    value = manifest()
    value["nodes"] = {
        "model.analytics.bad": {
            "resource_type": "model",
            "original_file_path": "models/marts/bad.sql",
        }
    }
    with pytest.raises(ValueError):
        contracts(value)


def test_incompatible_shapes_and_compatible_expansion() -> None:
    from dataclasses import replace

    from analytics_transformations.mart_contracts import Column

    old = contracts(manifest())
    mart = old[0]
    compatible(
        old,
        (
            replace(
                mart,
                columns=(
                    *mart.columns,
                    Column("new_detail", "varchar", "Optional useful detail.", False),
                ),
            ),
        ),
    )
    for changed in (
        replace(mart, grain="One row per day"),
        replace(mart, key=("changed_id",)),
        replace(mart, columns=(replace(mart.columns[0], data_type="varchar"),)),
        replace(mart, columns=(replace(mart.columns[0], nullable=False),)),
        replace(
            mart,
            columns=(
                *mart.columns,
                Column("required_detail", "varchar", "Required detail", False, False),
            ),
        ),
    ):
        with pytest.raises(ValueError):
            compatible(old, (changed,))


def test_composite_key_requires_unique_check_over_the_key() -> None:
    data = manifest()
    nodes = cast(dict[str, dict[str, object]], data["nodes"])
    model = nodes["model.analytics.fct_examples"]
    config = cast(dict[str, dict[str, dict[str, object]]], model["config"])
    publication = config["meta"]["publication"]
    publication["key"] = ["shop_id", "observation_id"]
    cast(dict[str, object], model["columns"])["shop_id"] = {
        "data_type": "varchar",
        "description": "Shop identity.",
        "config": {"meta": {"pii": False}},
    }
    nodes["test.analytics.not_null_shop"] = {
        **nodes["test.analytics.not_null_examples"],
        "name": "not_null_shop",
        "test_metadata": {"name": "not_null", "kwargs": {"column_name": "shop_id"}},
    }
    unique = nodes["test.analytics.unique_examples"]
    for kwargs in ({"column_name": "observation_id"}, {"columns": ["shop_id"]}):
        unique["test_metadata"] = {"name": "unique_combination", "kwargs": kwargs}
        with pytest.raises(ValueError, match="unique"):
            contracts(data)
    unique["test_metadata"] = {
        "name": "unique_combination",
        "kwargs": {"columns": ["observation_id", "shop_id"]},
    }
    assert contracts(data)[0].key == ("shop_id", "observation_id")


def _publication(data: dict[str, object]) -> dict[str, object]:
    nodes = cast(dict[str, dict[str, object]], data["nodes"])
    config = cast(
        dict[str, dict[str, dict[str, object]]],
        nodes["model.analytics.fct_examples"]["config"],
    )
    return config["meta"]["publication"]


@pytest.mark.parametrize("value", [None, "", "public", "Restricted"])
def test_access_must_be_declared_and_known(value: object) -> None:
    data = manifest()
    if value is None:
        del _publication(data)["access"]
    else:
        _publication(data)["access"] = value
    with pytest.raises(ValueError, match="access"):
        contracts(data)


def test_access_survives_the_reviewed_snapshot() -> None:
    from analytics_transformations.mart_contracts import (
        analytical,
        read_snapshot,
        restricted,
        snapshot,
    )

    data = manifest()
    _publication(data)["access"] = "restricted"
    plan = contracts(data)
    assert restricted(plan) == plan and analytical(plan) == ()
    assert read_snapshot(json.loads(snapshot(plan))) == plan  # pyright: ignore[reportAny]
    unclassified = cast(
        list[dict[str, object]], json.loads(snapshot(contracts(manifest())))
    )
    for entry in unclassified:
        del entry["access"]
    with pytest.raises(ValueError, match="access"):
        _ = read_snapshot(unclassified)


def test_columns_carry_their_reviewed_pii_classification() -> None:
    """Cube reads column PII flags from the reviewed snapshot (ADR 0014)."""
    from analytics_transformations.mart_contracts import read_snapshot, snapshot

    data = manifest()
    nodes = cast(dict[str, dict[str, object]], data["nodes"])
    columns = cast(
        dict[str, dict[str, object]], nodes["model.analytics.fct_examples"]["columns"]
    )
    columns["observation_id"]["config"] = {"meta": {"pii": True}}
    plan = contracts(data)
    assert plan[0].columns[0].pii is True
    assert read_snapshot(json.loads(snapshot(plan))) == plan  # pyright: ignore[reportAny]
    for meta in ({}, {"pii": "false"}):
        columns["observation_id"]["config"] = {"meta": meta}
        with pytest.raises(ValueError, match="observation_id.*pii"):
            contracts(data)


def test_access_reclassification_is_breaking_both_ways() -> None:
    from dataclasses import replace

    old = contracts(manifest())
    for before, after in (("analytical", "restricted"), ("restricted", "analytical")):
        with pytest.raises(ValueError, match="access changed"):
            compatible(
                (replace(old[0], access=before),), (replace(old[0], access=after),)
            )


def test_native_severity_is_the_only_blocking_declaration() -> None:
    from analytics_transformations.mart_contracts import check_blocking

    assert check_blocking("test.error", {"config": {"severity": "ERROR"}})
    assert not check_blocking("test.warn", {"config": {"severity": "WARN"}})
    # Obsolete metadata cannot downgrade an error.
    assert check_blocking(
        "test.error",
        {"config": {"severity": "error", "meta": {"publication_blocking": False}}},
    )
    for severity in (None, "", "ignore"):
        with pytest.raises(ValueError, match="severity"):
            check_blocking("test.invalid", {"config": {"severity": severity}})


def test_snapshot_command_prints_proposal_without_editing_baseline(
    tmp_path: "Path",
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from analytics_transformations.mart_contracts import main, snapshot

    path = tmp_path / "manifest.json"
    _ = path.write_text(json.dumps(manifest()))
    baseline = tmp_path / "reviewed.json"
    _ = baseline.write_text("reviewed baseline")
    monkeypatch.setattr("sys.argv", ["mart_contracts", str(path), "--snapshot"])
    main()
    assert capsys.readouterr().out == snapshot(contracts(manifest()))
    assert baseline.read_text() == "reviewed baseline"
