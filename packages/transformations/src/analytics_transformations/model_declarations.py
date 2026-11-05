"""Check model declarations and test blocking declarations in a dbt manifest.

Every governed model (``governed_projects``: the root project and the toolkit
package) declares its owner, row grain, and either a
key or the reason it has none. A declared key has blocking not-null tests and a
blocking uniqueness test: ``unique`` for one column, or a
``unique_combination`` test over exactly the key columns. Data tests use native
severity: error fails the build, warn does not.

Every governed model, source and source table is described, and each
model schema (a logical collection such as ``staging``) has one
``meta.schema_description``, set beside ``+schema`` in ``dbt_project.yml``.
"""

from dataclasses import dataclass

from analytics_transformations.layer_dependencies import governed_projects
from analytics_transformations.manifest_values import (
    blocking_tests,
    check_blocking,
    has_unique_check,
    identifier,
    mapping,
    sequence,
    text,
)


@dataclass(frozen=True)
class ModelDeclaration:
    model_id: str
    owner: str
    grain: str
    key: tuple[str, ...]
    no_unique_key_reason: str | None


def _declaration(
    model_id: str, node: dict[str, object], tests: dict[str, dict[str, object]]
) -> ModelDeclaration:
    meta = mapping(mapping(node.get("config")).get("meta", {}))
    try:
        owner = text(meta.get("owner"))
        grain = text(meta.get("grain"))
    except ValueError as error:
        raise ValueError(f"{model_id}: declare meta owner and grain") from error
    has_key = "key" in meta
    reason = meta.get("no_unique_key_reason")
    if has_key == (reason is not None):
        raise ValueError(
            f"{model_id}: declare exactly one of meta key or no_unique_key_reason"
        )
    if reason is not None:
        _ = blocking_tests(model_id, tests)
        return ModelDeclaration(model_id, owner, grain, (), text(reason))
    key = tuple(identifier(item) for item in sequence(meta.get("key")))
    columns = set(mapping(node.get("columns", {})))
    if not key or len(set(key)) != len(key) or not set(key) <= columns:
        raise ValueError(f"{model_id}: key must name distinct documented columns")
    found = blocking_tests(model_id, tests)
    if not set(key) <= found.not_null:
        raise ValueError(f"{model_id}: key columns need blocking not-null tests")
    if not has_unique_check(found, key):
        raise ValueError(
            f"{model_id}: key needs a blocking unique test, or for a composite "
            "key a blocking unique_combination test "
            "over the key"
        )
    return ModelDeclaration(model_id, owner, grain, key, None)


def declarations(data: object) -> tuple[ModelDeclaration, ...]:
    """Return governed model declarations or raise ValueError."""
    projects = governed_projects(data)
    nodes = {
        key: mapping(value)
        for key, value in mapping(mapping(data).get("nodes")).items()
    }
    tests = {
        key: node
        for key, node in nodes.items()
        if node.get("resource_type") == "test" and node.get("package_name") in projects
    }
    for test_id, test in tests.items():
        _ = check_blocking(test_id, test)
    return tuple(
        _declaration(model_id, node, tests)
        for model_id, node in sorted(nodes.items())
        if node.get("resource_type") == "model" and node.get("package_name") in projects
    )


def _described(value: object) -> bool:
    return bool(str(value or "").strip())


def check_documentation(data: object) -> None:
    """Raise ValueError unless governed models, schemas and sources are
    described. Column descriptions are checked with column declarations."""
    projects = governed_projects(data)
    schemas: dict[str, str] = {}
    for model_id, value in sorted(mapping(mapping(data).get("nodes")).items()):
        node = mapping(value)
        if (
            node.get("resource_type") != "model"
            or node.get("package_name") not in projects
        ):
            continue
        if not _described(node.get("description")):
            raise ValueError(f"{model_id}: add a model description")
        config = mapping(node.get("config"))
        schema = str(config.get("schema") or "default")
        described = mapping(config.get("meta", {})).get("schema_description")
        if not _described(described):
            raise ValueError(
                f"{model_id}: describe schema {schema} with meta schema_description"
            )
        if schemas.setdefault(schema, str(described)) != str(described):
            raise ValueError(f"schema {schema}: declare one schema_description")
    sources = mapping(mapping(data).get("sources", {}))
    for source_id, value in sorted(sources.items()):
        source = mapping(value)
        if source.get("package_name") not in projects:
            continue
        if not _described(source.get("source_description")):
            raise ValueError(f"{source_id}: add a source description")
        if not _described(source.get("description")):
            raise ValueError(f"{source_id}: add a table description")
