"""Validate isolated dbt candidates before replacing the consumer boundary.

Publication is independent of business models.
Each publication contains one explicitly declared consistency group.
"""

import json
import os
import shutil
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from analytics_transformations.layer_dependencies import root_project
from analytics_transformations.manifest_checks import check_manifest
from analytics_transformations.mart_contracts import (
    REVIEWED_ACCESS_TIGHTENINGS,
    MartContract,
    contracts,
    identifier,
    mapping,
    read_json,
    restricted,
    sequence,
    snapshot,
    text,
)

Runner = Callable[[Sequence[str], Mapping[str, str], int], None]


@dataclass(frozen=True)
class PublicationResult:
    namespace: str
    publication_id: str
    published_at: datetime
    # Restricted relations built and tested but granted to no consumer.
    restricted_relations_unpublished: tuple[str, ...] = ()
    artifacts_directory: Path | None = None


def run_dbt(command: Sequence[str], env: Mapping[str, str], timeout: int) -> None:
    """Keep raw diagnostics private; never copy warehouse errors into run logs."""
    target = Path(command[command.index("--target-path") + 1])
    target.mkdir(mode=0o700, parents=True, exist_ok=True)
    target.chmod(0o700)
    failed = False
    with (target / "console.log").open("w") as output:
        try:
            _ = subprocess.run(
                [str(Path(sys.executable).with_name("dbt")), *command[1:]],
                env={**env, "DBT_LOG_PATH": str(target)},
                timeout=timeout,
                check=True,
                stdout=output,
                stderr=subprocess.STDOUT,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            failed = True
    if failed:
        raise RuntimeError(
            f"dbt {command[1]} failed or timed out; private diagnostics: {target}"
        ) from None


def _artifacts(project: Path, generation: str) -> Path:
    root = Path(os.environ.get("DBT_TARGET_PATH", str(project / "target")))
    directory = root.resolve() / "publications" / generation
    directory.mkdir(mode=0o700, parents=True)
    return directory


def _validate(namespace: str, timeout: int) -> None:
    _ = identifier(namespace)
    if (
        len(namespace) > 40
        or namespace in {"production", "prod", "main", "public"}
        or namespace.startswith(("prod_", "production_"))
    ):
        raise ValueError("Publication requires an isolated namespace")
    if not 1 <= timeout <= 240:
        raise ValueError("Command timeout must be between 1 and 240 seconds")


def publication_manifest(
    manifest: object, publication_group: str | None
) -> dict[str, object]:
    """Select one complete consistency group, its ancestors and contained checks.

    Ungrouped projects retain a single ``default`` group for compatibility with
    standalone projects. A project with named groups must classify every mart.
    No caller may select only part of a group.
    """
    value = mapping(manifest)
    nodes = {key: mapping(node) for key, node in mapping(value.get("nodes")).items()}
    marts = {
        key: node
        for key, node in nodes.items()
        if node.get("resource_type") == "model"
        and str(node.get("original_file_path", "")).startswith("models/marts/")
    }
    groups = {
        key: mapping(mapping(node.get("config")).get("meta", {})).get(
            "publication_group"
        )
        for key, node in marts.items()
    }
    named = any(group is not None for group in groups.values())
    membership = {
        key: identifier(group if named else "default") for key, group in groups.items()
    }
    available = set(membership.values())
    if publication_group is None:
        if len(available) != 1:
            raise ValueError("Select exactly one publication group")
        publication_group = next(iter(available))
    if publication_group not in available:
        raise ValueError(f"Unknown or empty publication group: {publication_group}")
    selected = {key for key, group in membership.items() if group == publication_group}
    ancestors = set(selected)
    pending = list(selected)
    while pending:
        node = nodes.get(pending.pop(), {})
        for dependency in sequence(
            mapping(node.get("depends_on", {})).get("nodes", [])
        ):
            key = text(dependency)
            if key in membership and key not in selected:
                raise ValueError("Mart dependencies must share a publication group")
            if key not in ancestors:
                ancestors.add(key)
                pending.append(key)

    # Tests may inspect additional raw evidence without requiring another mart.
    available_inputs = ancestors | set(mapping(value.get("sources", {})))

    def included(key: str, node: dict[str, object]) -> bool:
        if key in ancestors:
            return True
        if node.get("resource_type") not in ("test", "unit_test"):
            return False
        dependencies = set(
            sequence(mapping(node.get("depends_on", {})).get("nodes", []))
        )
        if not dependencies & ancestors:
            return False
        if dependencies <= available_inputs:
            return True
        owner = node.get("attached_node")
        # An unrelated mart's reconciliation or an unrelated model's attached
        # test may share our ancestors. It does not belong to this publication.
        # But a required ancestor's own check must never silently disappear.
        if not dependencies & selected and owner not in ancestors:
            if dependencies & (marts.keys() - selected):
                return False
            if isinstance(owner, str) and owner not in ancestors:
                return False
        raise ValueError(f"{key}: check requires models outside its publication group")

    value["nodes"] = {key: node for key, node in nodes.items() if included(key, node)}
    value["unit_tests"] = {
        key: node
        for key, node in mapping(value.get("unit_tests", {})).items()
        if included(key, mapping(node))
    }
    # dagster-dbt builds its selection graph from these maps. Removed models
    # and checks must disappear from the graph as well as the node collections.
    members = (
        set(mapping(value["nodes"]))
        | set(mapping(value["unit_tests"]))
        | set(mapping(value.get("sources", {})))
    )
    for field in ("parent_map", "child_map"):
        if field in value:
            value[field] = {
                key: [item for item in sequence(edges) if item in members]
                for key, edges in mapping(value[field]).items()
                if key in members
            }
    return value


def validate_results(
    manifest: object, results: object, *, publication_group: str | None = None
) -> tuple[MartContract, ...]:
    """Fail closed on missing, stale, skipped or unsuccessful blocking results."""
    manifest = publication_manifest(manifest, publication_group)
    plan = contracts(manifest)
    if not plan:
        raise ValueError("Publication requires at least one contracted mart")
    manifest_map, result_map = mapping(manifest), mapping(results)
    invocation = mapping(manifest_map.get("metadata")).get("invocation_id")
    if not isinstance(invocation, str) or invocation != mapping(
        result_map.get("metadata")
    ).get("invocation_id"):
        raise ValueError("Manifest/results invocation mismatch")
    required = {mart.model_id for mart in plan} | {
        check for mart in plan for check in mart.checks
    }
    nodes = mapping(manifest_map.get("nodes"))
    ancestors = {mart.model_id for mart in plan}
    pending = list(ancestors)
    while pending:
        node_id = pending.pop()
        node = mapping(nodes.get(node_id))
        for dependency in sequence(
            mapping(node.get("depends_on", {"nodes": []})).get("nodes")
        ):
            dependency_id = text(dependency)
            if dependency_id in nodes and dependency_id not in ancestors:
                ancestors.add(dependency_id)
                pending.append(dependency_id)
    # Ephemeral ancestors compile into their consumers and have no run result.
    # Keep traversing them and requiring their tests and executable dependencies.
    required |= {
        node_id
        for node_id in ancestors
        if mapping(mapping(nodes[node_id]).get("config", {})).get("materialized")
        != "ephemeral"
    }
    for section in (nodes, mapping(manifest_map.get("unit_tests", {}))):
        for node_id, value in section.items():
            node = mapping(value)
            if node.get("resource_type") not in ("test", "unit_test"):
                continue
            required.add(node_id)
    seen: set[str] = set()
    for value in sequence(result_map.get("results")):
        item = mapping(value)
        node_id = text(item.get("unique_id"))
        if node_id in seen:
            raise ValueError("Duplicate dbt result")
        seen.add(node_id)
        if node_id not in required:
            continue
        status = item.get("status")
        if status in ("success", "pass"):
            continue
        node = mapping(nodes.get(node_id))
        config = mapping(node.get("config"))
        if status == "warn" and str(config.get("severity")).lower() == "warn":
            continue
        raise ValueError(f"Unsuccessful publication result: {node_id}")
    if not required <= seen:
        raise ValueError("Candidate lacks required model/check results")
    return plan


def _command(project: Path, profiles: Path, target: Path) -> list[str]:
    # Each invocation owns its artifacts: cleanup and publication macros must
    # never overwrite the build manifest or run results used for diagnostics.
    # dbt validates this cache against files, variables and environment changes.
    cache = target.parent / "parse" / "partial_parse.msgpack"
    if target.name != "parse" and cache.is_file():
        target.mkdir(mode=0o700, parents=True, exist_ok=True)
        _ = shutil.copyfile(cache, target / "partial_parse.msgpack")
    return [
        "--project-dir",
        str(project),
        "--profiles-dir",
        str(profiles),
        "--target-path",
        str(target),
    ]


def validate_publication_sources(manifest: object, expected: frozenset[str]) -> None:
    """Reject graph drift before building a candidate with uncoordinated inputs.

    Source declarations can exist without being read (notably optional Loop).
    Resolve actual dependency edges rather than treating declarations as reads.
    """
    document = mapping(manifest)
    sources = mapping(document.get("sources", {}))
    used: set[str] = set()
    for value in mapping(document.get("nodes")).values():
        node = mapping(value)
        for dependency in sequence(
            mapping(node.get("depends_on", {})).get("nodes", [])
        ):
            key = text(dependency)
            if key.startswith("source."):
                used.add(text(mapping(sources.get(key)).get("source_name")))
    if frozenset(used) != expected:
        raise ValueError(
            "Publication inputs differ from dbt source dependencies: "
            f"undeclared={sorted(used - expected)}, unused={sorted(expected - used)}"
        )


def build_candidate(
    project: Path,
    profiles: Path,
    artifacts: Path,
    env: Mapping[str, str],
    timeout: int,
    runner: Runner,
    expected_sources: frozenset[str] | None = None,
    publication_group: str | None = None,
) -> tuple[MartContract, ...]:
    """Build and validate isolated model outputs without publishing them."""
    parsed_path = artifacts / "parse"
    built_path = artifacts / "build"
    runner(["dbt", "parse", *_command(project, profiles, parsed_path)], env, timeout)
    parsed = read_json(parsed_path / "manifest.json")
    scoped = publication_manifest(parsed, publication_group)
    selected_plan = contracts(scoped)
    if expected_sources is not None:
        validate_publication_sources(scoped, expected_sources)
    _ = check_manifest(parsed, namespace=env["DBT_NAMESPACE"])
    for value in mapping(mapping(parsed).get("nodes")).values():
        node = mapping(value)
        if node.get("resource_type") != "model":
            continue
        config = mapping(node.get("config"))
        if config.get("grants"):
            raise ValueError("Only the publication step may grant candidate access")
        expected = {
            env["DBT_NAMESPACE"] + "_" + layer
            for layer in ("staging", "intermediate", "marts")
        }
        if text(node.get("schema")).lower() not in expected:
            raise ValueError(
                "All model references must route to isolated candidate schemas"
            )
    runner(
        [
            "dbt",
            "build",
            *_command(project, profiles, built_path),
            "--select",
            " ".join(
                ("+" if node.get("resource_type") == "model" else "")
                + "fqn:"
                + ".".join(text(part) for part in sequence(node.get("fqn")))
                for node_id, value in mapping(scoped["nodes"]).items()
                if (node := mapping(value)).get("resource_type") == "test"
                or node_id in {mart.model_id for mart in selected_plan}
            ),
            "--indirect-selection",
            "cautious",
            "--warn-error-options",
            '{"error":["NoNodesForSelectionCriteria"]}',
        ],
        env,
        timeout,
    )
    built = read_json(built_path / "manifest.json")
    results = read_json(built_path / "run_results.json")
    plan = validate_results(built, results, publication_group=publication_group)
    if plan != selected_plan:
        raise ValueError("Publication contracts changed during the build")
    # Use the compiled build artifacts before run-operation replaces the manifest.
    # Ephemeral SQL has dbt's injected CTEs; physical models are inspected directly.
    nodes = mapping(mapping(built).get("nodes"))
    executed = {
        text(mapping(item).get("unique_id"))
        for item in sequence(mapping(results).get("results"))
    }
    pending = list(executed)
    while pending:
        node_id = pending.pop()
        if node_id not in nodes:
            continue
        for dependency in sequence(
            mapping(mapping(nodes[node_id]).get("depends_on", {})).get("nodes", [])
        ):
            dependency_id = text(dependency)
            if dependency_id in nodes and dependency_id not in executed:
                executed.add(dependency_id)
                pending.append(dependency_id)
    outputs: list[dict[str, object]] = []
    for node_id in sorted(executed):
        node = mapping(nodes.get(node_id, {}))
        if node.get("resource_type") != "model" or node.get(
            "package_name"
        ) != root_project(built):
            continue
        config = mapping(node.get("config"))
        sql = (
            text(node.get("compiled_code"))
            if config.get("materialized") == "ephemeral"
            else "select * from " + text(node.get("relation_name"))
        )
        # Only type declarations belong in the CLI payload. Full column metadata
        # can exceed Linux's per-argument limit for a moderately sized project.
        columns = {
            name: {"name": name, "data_type": mapping(column).get("data_type")}
            for name, column in mapping(node.get("columns")).items()
        }
        outputs.append({"model_id": node_id, "sql": sql, "columns": columns})
    runner(
        [
            "dbt",
            "run-operation",
            "validate_model_outputs",
            *_command(project, profiles, artifacts / "validate-outputs"),
            "--args",
            json.dumps({"outputs": outputs}),
        ],
        env,
        timeout,
    )
    return plan


def publish_snowflake_marts(
    *,
    environment: str,
    namespace: str,
    project_dir: Path,
    profiles_dir: Path,
    reader_role: str,
    restricted_reader_role: str | None = None,
    expected_sources: frozenset[str] | None = None,
    publication_group: str | None = None,
    timeout_seconds: int = 240,
    runner: Runner = run_dbt,
) -> PublicationResult:
    """Clone/build/test/swap under a Snowflake DDL lock shared by all publishers.

    ``reader_role`` receives SELECT on analytical marts only; stale or inherited
    table grants for it are revoked first. ``restricted`` marts are granted only
    to ``restricted_reader_role``. Without it they are granted to no consumer.

    A crashed publisher retains the lock and fails closed. Never automatically
    steal it: inspect the marker, terminate its session, then recover explicitly.
    """
    _validate(namespace, timeout_seconds)
    privileged = {"public", "accountadmin", "sysadmin", "securityadmin"}
    for role in (reader_role, restricted_reader_role):
        if role is None:
            continue
        _ = identifier(role.lower())
        if role.lower() in privileged:
            raise ValueError("Use a restricted consumer role")
    if (
        restricted_reader_role is not None
        and restricted_reader_role.lower() == reader_role.lower()
    ):
        raise ValueError("The restricted reader role must differ from the reader role")
    if environment != "development":
        raise ValueError(
            "Snowflake publication requires explicit development environment"
        )
    generation = uuid4().hex
    candidate_namespace = namespace + "_candidate_" + generation[:12]
    env = dict(os.environ)
    env.update(
        ANALYTICS_ENVIRONMENT=environment,
        DBT_TARGET="snowflake-development",
        DBT_NAMESPACE=candidate_namespace,
        DBT_STATEMENT_TIMEOUT_SECONDS="60",
        DBT_PUBLICATION_ID=generation,
    )
    artifacts = _artifacts(project_dir, generation)
    arguments: dict[str, object] = {
        "namespace": namespace,
        "candidate_namespace": candidate_namespace,
        "publication_id": generation,
        "reader_role": reader_role,
        "restricted_reader_role": restricted_reader_role,
    }

    def operation(name: str) -> None:
        runner(
            [
                "dbt",
                "run-operation",
                name,
                *_command(project_dir, profiles_dir, artifacts / name),
                "--args",
                json.dumps(arguments),
            ],
            env,
            timeout_seconds,
        )

    operation("prepare_mart_candidate")
    try:
        plan = build_candidate(
            project_dir,
            profiles_dir,
            artifacts,
            env,
            timeout_seconds,
            runner,
            expected_sources,
            publication_group,
        )
        arguments["relations"] = [mart.relation for mart in plan]
        arguments["contracts_json"] = snapshot(plan)
        arguments["access_tightenings"] = list(REVIEWED_ACCESS_TIGHTENINGS)
    except Exception as build_error:
        try:
            operation("clean_mart_candidate")
        except Exception:
            build_error.add_note(
                "Candidate cleanup failed; the lock may remain. "
                f"Private diagnostics: {artifacts / 'clean_mart_candidate'}"
            )
        raise
    try:
        operation("publish_mart_candidate")
    except Exception:
        # A timed-out DDL may have committed. Confirm the generation before
        # cleanup, and never retry SWAP. If confirmation fails, leave the
        # cross-host lock and candidate for explicit operator recovery.
        operation("confirm_mart_publication")
    operation("clean_mart_candidate")
    return PublicationResult(
        namespace,
        generation,
        datetime.now(UTC),
        ()
        if restricted_reader_role is not None
        else tuple(item.relation for item in restricted(plan)),
        artifacts,
    )
