"""Check model declarations and test blocking declarations in a dbt manifest.

Every staging and intermediate model in the root project declares its purpose,
owner, row grain, and either a key or the reason it has none. A declared key has
blocking not-null tests and a blocking uniqueness test, as marts do: ``unique``
for one column, or a ``unique_combination`` test
over exactly the key columns. Data tests use native severity: error blocks
publication, warn does not.
Marts are checked by ``mart_contracts``, which reads ``meta.publication``.

Every root-project model, source and source table is described, and each
model schema (a logical collection such as ``staging``) has one
``meta.schema_description``, set beside ``+schema`` in ``dbt_project.yml``.
Every root-project exposure is described. An exposure's ``meta.cube_fields``
(generated from the Cube model) maps each Cube field to ``<model>.<column>``
references, which must name declared columns of models the exposure depends on.
"""

import sys
from dataclasses import dataclass
from pathlib import Path

from analytics_transformations.column_declarations import check_column_declarations
from analytics_transformations.layer_dependencies import root_project
from analytics_transformations.mart_contracts import (
    blocking_tests,
    check_blocking,
    has_unique_check,
    identifier,
    mapping,
    read_json,
    sequence,
    text,
)
from analytics_transformations.pii import check_pii

DECLARED_LAYERS = ("models/staging/", "models/intermediate/")


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
    if not str(node.get("description", "")).strip():
        raise ValueError(f"{model_id}: add a model description")
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
    """Return root-project staging/intermediate declarations or raise ValueError."""
    project = root_project(data)
    nodes = {
        key: mapping(value)
        for key, value in mapping(mapping(data).get("nodes")).items()
    }
    tests = {
        key: node
        for key, node in nodes.items()
        if node.get("resource_type") == "test" and node.get("package_name") == project
    }
    for test_id, test in tests.items():
        _ = check_blocking(test_id, test)
    return tuple(
        _declaration(model_id, node, tests)
        for model_id, node in sorted(nodes.items())
        if node.get("resource_type") == "model"
        and node.get("package_name") == project
        and str(node.get("original_file_path", "")).startswith(DECLARED_LAYERS)
    )


def _described(value: object) -> bool:
    return bool(str(value or "").strip())


def check_documentation(data: object) -> None:
    """Raise ValueError unless root-project models, schemas and sources are
    described. Column descriptions are checked with column declarations."""
    project = root_project(data)
    schemas: dict[str, str] = {}
    for model_id, value in sorted(mapping(mapping(data).get("nodes")).items()):
        node = mapping(value)
        if node.get("resource_type") != "model" or node.get("package_name") != project:
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
        if source.get("package_name") != project:
            continue
        if not _described(source.get("source_description")):
            raise ValueError(f"{source_id}: add a source description")
        if not _described(source.get("description")):
            raise ValueError(f"{source_id}: add a table description")
    check_exposures(data)


def check_exposures(data: object) -> None:
    """Raise ValueError unless exposures are described and their Cube field
    map names declared columns of the models they depend on."""
    project = root_project(data)
    nodes = mapping(mapping(data).get("nodes"))
    exposures = mapping(mapping(data).get("exposures", {}))
    for exposure_id, value in sorted(exposures.items()):
        exposure = mapping(value)
        if exposure.get("package_name") != project:
            continue
        if not _described(exposure.get("description")):
            raise ValueError(f"{exposure_id}: add an exposure description")
        depends_on = mapping(exposure.get("depends_on")).get("nodes")
        parents = [
            mapping(nodes.get(text(node_id), {})) for node_id in sequence(depends_on)
        ]
        models = {text(node["name"]): node for node in parents if node}
        meta = mapping(mapping(exposure.get("config", {})).get("meta", {}))
        for field, columns in mapping(meta.get("cube_fields", {})).items():
            for reference in sequence(columns):
                model, _, column = text(reference).partition(".")
                if not column:
                    raise ValueError(f"{field}: name a column as <model>.<column>")
                if model not in models:
                    raise ValueError(f"{field}: {model} is not in depends_on")
                if column not in mapping(models[model].get("columns")):
                    raise ValueError(f"{field}: {model} has no column {column}")


def main() -> None:
    data = read_json(Path(sys.argv[1]))
    _ = declarations(data)
    check_column_declarations(data)
    check_documentation(data)
    check_pii(data)


if __name__ == "__main__":
    main()
