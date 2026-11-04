"""Snowflake uncertain acknowledgements never trigger a blind second swap."""

import json
from collections.abc import Mapping, Sequence
from pathlib import Path

import pytest

from analytics_transformations.publication import (
    PublicationResult,
    publish_snowflake_marts,
)
from tests.test_mart_contracts import manifest


@pytest.mark.parametrize(
    "failure,expected",
    [
        ("prepare", ["prepare_mart_candidate"]),
        ("build", ["prepare_mart_candidate", "clean_mart_candidate"]),
        ("cleanup", ["prepare_mart_candidate", "clean_mart_candidate"]),
        (
            "confirmed",
            [
                "prepare_mart_candidate",
                "validate_model_outputs",
                "publish_mart_candidate",
                "confirm_mart_publication",
                "clean_mart_candidate",
            ],
        ),
        (
            "uncertain",
            [
                "prepare_mart_candidate",
                "validate_model_outputs",
                "publish_mart_candidate",
                "confirm_mart_publication",
            ],
        ),
    ],
)
def test_recovery_is_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str, expected: list[str]
) -> None:
    operations: list[str] = []
    diagnostic_root = tmp_path / "diagnostics"
    monkeypatch.setenv("DBT_TARGET_PATH", str(diagnostic_root))

    def runner(command: Sequence[str], env: Mapping[str, str], timeout: int) -> None:
        if command[1] == "run-operation":
            operation = command[2]
            operations.append(operation)
            if operation != "validate_model_outputs":
                # CLI JSON is a boundary; verify the build and swap use one generation.
                arguments = json.loads(command[command.index("--args") + 1])  # pyright: ignore[reportAny]
                assert arguments["publication_id"] == env["DBT_PUBLICATION_ID"]
            if operation == "clean_mart_candidate" and failure == "cleanup":
                raise RuntimeError("private cleanup error")
            if (
                (operation == "prepare_mart_candidate" and failure == "prepare")
                or (
                    operation == "publish_mart_candidate"
                    and failure in ("confirmed", "uncertain")
                )
                or (operation == "confirm_mart_publication" and failure == "uncertain")
            ):
                raise RuntimeError("injected operation failure")
            # dbt macros write their own artifacts. They must never replace the
            # successful build's evidence, even during publication recovery.
            target = Path(command[command.index("--target-path") + 1])
            target.mkdir(parents=True, exist_ok=True)
            for name in ("manifest.json", "run_results.json"):
                _ = (target / name).write_text(json.dumps({"operation": operation}))
            return
        if command[1] == "build" and failure in ("build", "cleanup"):
            raise RuntimeError("injected build failure")
        value = manifest()
        # Fixture JSON is mutable only at this test boundary.
        from analytics_transformations.mart_contracts import mapping

        nodes = mapping(value["nodes"])
        model = mapping(nodes["model.analytics.fct_examples"])
        model["schema"] = env["DBT_NAMESPACE"] + "_marts"
        model["package_name"] = "recovery_fixture"
        model["relation_name"] = model["schema"] + ".fct_examples"
        columns = mapping(model["columns"])
        observation = mapping(columns["observation_id"])
        observation["config"] = {
            "meta": {
                "pii": False,
                "identity": {
                    "kind": "analytical",
                    "meaning": "Synthetic observation identity",
                },
            }
        }
        columns["observation_id"] = observation
        model["columns"] = columns
        nodes["model.analytics.fct_examples"] = model
        value["nodes"] = nodes
        value["metadata"] = {
            "invocation_id": "run",
            "project_name": "recovery_fixture",
        }
        target = Path(command[command.index("--target-path") + 1])
        target.mkdir(parents=True, exist_ok=True)
        _ = (target / "manifest.json").write_text(json.dumps(value))
        _ = (target / "run_results.json").write_text(
            json.dumps(
                {
                    "metadata": {"invocation_id": "run"},
                    "results": [
                        {
                            "unique_id": node_id,
                            "status": "success"
                            if node_id.startswith("model.")
                            else "pass",
                        }
                        for node_id in nodes
                    ],
                }
            )
        )

    def publish() -> PublicationResult:
        return publish_snowflake_marts(
            environment="development",
            namespace="publication_test",
            project_dir=tmp_path,
            profiles_dir=tmp_path,
            reader_role="analytics_reader",
            runner=runner,
        )

    if failure == "confirmed":
        result = publish()
        artifacts = result.artifacts_directory
        assert artifacts is not None
        assert artifacts == diagnostic_root / "publications" / result.publication_id
        assert artifacts.stat().st_mode & 0o777 == 0o700
        for name in ("manifest.json", "run_results.json"):
            # Read the retained files after publish, confirmation and cleanup.
            build = (artifacts / "build" / name).read_text()
            assert '"invocation_id": "run"' in build
            assert '"operation"' not in build
            cleanup = (artifacts / "clean_mart_candidate" / name).read_text()
            assert '"operation": "clean_mart_candidate"' in cleanup
    else:
        with pytest.raises(RuntimeError, match="injected") as error:
            publish()
        if failure == "cleanup":
            notes = " ".join(error.value.__notes__)
            assert "cleanup failed" in notes
            assert "clean_mart_candidate" in notes
            assert "private cleanup error" not in notes
    assert operations == expected


def test_input_drift_cleans_candidate_before_build_or_swap(tmp_path: Path) -> None:
    commands: list[str] = []

    def runner(command: Sequence[str], env: Mapping[str, str], timeout: int) -> None:
        del env, timeout
        commands.append(command[2] if command[1] == "run-operation" else command[1])
        if command[1] == "parse":
            target = Path(command[command.index("--target-path") + 1])
            target.mkdir(parents=True)
            _ = (target / "manifest.json").write_text(json.dumps(manifest()))

    with pytest.raises(ValueError, match="Publication inputs differ"):
        _ = publish_snowflake_marts(
            environment="development",
            namespace="coordination_test",
            project_dir=tmp_path,
            profiles_dir=tmp_path,
            reader_role="coordination_reader",
            expected_sources=frozenset({"undeclared_input"}),
            runner=runner,
        )
    assert commands == ["prepare_mart_candidate", "parse", "clean_mart_candidate"]
