"""Check ADR 0022's forward-only layer dependencies in a parsed dbt manifest.

``manifest_checks`` runs these rules after ``dbt parse``.

A model's folder under its model path sets its layer, and its name prefix must
name the same layer. Layers read only from the layers ADR 0022 allows:

- staging reads sources, seeds, snapshots, and staging models of the same source;
- intermediate reads staging and intermediate models;
- marts read staging, intermediate, and mart models.

Staging model names follow ``stg_<source>__<entity>``. Snapshots read sources
only. Exposures and semantic models read marts only. Data tests, unit tests,
analyses, seeds, and metrics are exempt. Only the root project is checked;
models from installed packages count as staging inputs, like sources.

Model and snapshot SQL must reach relations through ``ref()`` or ``source()``
(or the project's ``sparse_source()``, which calls ``source()``).
A FROM or JOIN target must be one of those calls, ``{{ this }}``, a CTE name,
a subquery, or a table function in ``ROW_FUNCTIONS``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

type Layer = Literal["staging", "intermediate", "marts"]

LAYER_PREFIXES: dict[Layer, tuple[str, ...]] = {
    "staging": ("stg_",),
    "intermediate": ("int_",),
    "marts": ("fct_", "dim_"),
}
MODEL_READS: dict[Layer, frozenset[Layer]] = {
    "staging": frozenset({"staging"}),
    "intermediate": frozenset({"staging", "intermediate"}),
    "marts": frozenset({"staging", "intermediate", "marts"}),
}
# Models from installed dbt packages sit outside these layers, like sources.
STAGING_INPUTS = frozenset({"source", "seed", "snapshot", "package model"})
# Function resources are not relations, so any layer may call them.
ALWAYS_ALLOWED = frozenset({"function"})
CONSUMERS = {"exposure": "exposure", "semantic_model": "semantic model"}


@dataclass(frozen=True)
class Resource:
    unique_id: str
    resource_type: str
    name: str
    package: str
    path: str
    dependencies: tuple[str, ...]
    language: str
    raw_code: str
    alias: str = ""
    database: str = ""
    schema: str = ""
    version: str | None = None
    materialized: str = ""
    generic_test: bool = False
    store_failures: bool = False


@dataclass(frozen=True, order=True)
class Violation:
    resource: str
    rule: str
    detail: str

    def __str__(self) -> str:
        return f"{self.resource}: {self.rule}: {self.detail}"


def _mapping(value: object, where: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"Expected an object at {where}")
    return cast(dict[str, object], value)


def _text(value: object, where: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"Expected a string at {where}")
    return value


def _dependencies(node: dict[str, object], where: str) -> tuple[str, ...]:
    depends_on = _mapping(node.get("depends_on", {}), f"{where}.depends_on")
    nodes = depends_on.get("nodes", [])
    if not isinstance(nodes, list):
        raise ValueError(f"Expected a list at {where}.depends_on.nodes")
    return tuple(
        _text(item, f"{where}.depends_on.nodes") for item in cast(list[object], nodes)
    )


def parse_manifest(data: object) -> list[Resource]:
    """Read the resources and dependencies the rules need from a dbt manifest."""
    manifest = _mapping(data, "manifest")
    resources: list[Resource] = []
    for section in ("nodes", "unit_tests", "exposures", "semantic_models", "metrics"):
        entries = _mapping(manifest.get(section, {}), section)
        for unique_id, value in entries.items():
            where = f"{section}.{unique_id}"
            node = _mapping(value, where)
            resource_type = _text(node.get("resource_type"), f"{where}.resource_type")
            config = _mapping(node.get("config", {}), f"{where}.config")
            version = node.get("version")
            if version is not None and (
                isinstance(version, bool) or not isinstance(version, (str, int, float))
            ):
                raise ValueError(f"Expected a string or number at {where}.version")
            metadata = node.get("test_metadata")
            generic_test = resource_type == "test" and metadata is not None
            if generic_test:
                _ = _mapping(metadata, f"{where}.test_metadata")
            resources.append(
                Resource(
                    unique_id=unique_id,
                    resource_type=resource_type,
                    name=_text(node.get("name"), f"{where}.name"),
                    package=_text(
                        node.get("package_name", ""), f"{where}.package_name"
                    ),
                    path=_text(node.get("original_file_path", ""), where),
                    dependencies=_dependencies(node, where),
                    language=_text(node.get("language", "sql") or "sql", where),
                    raw_code=_text(node.get("raw_code", "") or "", where),
                    alias=_text(node.get("alias", "") or "", f"{where}.alias"),
                    database=_text(node.get("database", "") or "", f"{where}.database"),
                    schema=_text(node.get("schema", "") or "", f"{where}.schema"),
                    version=str(version) if version is not None else None,
                    materialized=_text(config.get("materialized", "") or "", where),
                    generic_test=generic_test,
                    store_failures=config.get("store_failures") is True
                    or config.get("store_failures_as") in ("table", "view"),
                )
            )
    return resources


def root_project(data: object) -> str:
    """Return the manifest's root project, whose resources the rules govern."""
    metadata = _mapping(_mapping(data, "manifest").get("metadata", {}), "metadata")
    return _text(metadata.get("project_name"), "metadata.project_name")


def _folder_layer(path: str) -> Layer | None:
    parts = Path(path).parts
    folder = parts[1] if len(parts) > 2 else ""
    for layer in LAYER_PREFIXES:
        if folder == layer:
            return layer
    return None


def _prefix_layer(name: str) -> Layer | None:
    for layer, prefixes in LAYER_PREFIXES.items():
        if name.startswith(prefixes):
            return layer
    return None


_STAGING_NAME = re.compile(r"stg_([a-z0-9]+(?:_[a-z0-9]+)*)__[a-z0-9]+(?:_[a-z0-9]+)*")


def _staging_source(name: str) -> str:
    return name.removeprefix("stg_").partition("__")[0]


def _layer_violations(model: Resource) -> list[Violation]:
    folder = _folder_layer(model.path)
    prefix = _prefix_layer(model.name)
    if folder is None:
        expected = ", ".join(f"models/{layer}/" for layer in LAYER_PREFIXES)
        return [
            Violation(
                model.name, "unknown-layer", f"{model.path} is outside {expected}"
            )
        ]
    if prefix != folder:
        prefixes = " or ".join(LAYER_PREFIXES[folder])
        return [
            Violation(
                model.name,
                "prefix-mismatch",
                f"{folder} models must start with {prefixes}",
            )
        ]
    if folder == "staging" and not _STAGING_NAME.fullmatch(model.name):
        return [
            Violation(
                model.name,
                "staging-name",
                "staging models must be named stg_<source>__<entity>",
            )
        ]
    return []


# Snowflake's 255-character identifier limit is the portable ceiling. DuckDB
# compares even quoted identifiers case-insensitively, so quoting cannot rescue
# case collisions. Database names are deployment-owned and may be uppercase.
_IDENTIFIER = re.compile(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*")


def _persisted(resource: Resource) -> bool:
    return (
        resource.resource_type in {"seed", "snapshot"}
        or resource.resource_type == "model"
        and resource.materialized != "ephemeral"
        or resource.resource_type == "test"
        and resource.store_failures
    )


def _naming_violations(
    resources: list[Resource], namespace: str | None
) -> list[Violation]:
    violations: list[Violation] = []
    relations: dict[tuple[str, str, str], str] = {}
    if namespace is not None and (
        not _IDENTIFIER.fullmatch(namespace) or len(namespace) > 241
    ):
        violations.append(
            Violation(
                "namespace",
                "namespace-name",
                "namespace must be lowercase snake_case "
                "and leave room for _test_failures",
            )
        )
    for resource in resources:

        def report(rule: str, detail: str, name: str = resource.name) -> None:
            violations.append(Violation(name, rule, detail))

        layer = _folder_layer(resource.path)
        kind = resource.resource_type
        if (
            kind in {"model", "seed", "snapshot", "test", "unit_test"}
            and not resource.generic_test
            and len(resource.name) > 255
        ):
            report(
                "identifier", "authored resource name must be at most 255 characters"
            )
        if resource.version is not None and not resource.unique_id.endswith(
            ".v" + resource.version
        ):
            report(
                "model-version", "declared version must match the manifest unique_id"
            )
        if not _persisted(resource):
            continue
        alias = resource.alias or resource.name
        if len(alias) > 255 or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", alias):
            report(
                "identifier",
                f"persisted identifier {alias} must be portable "
                "and at most 255 characters",
            )
        if resource.schema and (
            not _IDENTIFIER.fullmatch(resource.schema) or len(resource.schema) > 255
        ):
            report(
                "schema-name",
                "schema must be lowercase snake_case and at most 255 characters",
            )
        suffix = (
            layer
            if kind == "model"
            else {
                "seed": "seeds",
                "snapshot": "snapshots",
                "test": "test_failures",
            }.get(kind)
        )
        if (
            namespace is not None
            and suffix
            and resource.schema != f"{namespace}_{suffix}"
        ):
            report(
                "resource-schema",
                f"expected schema {namespace}_{suffix}, got {resource.schema}",
            )
        if resource.schema:
            key = (
                resource.database.casefold(),
                resource.schema.casefold(),
                alias.casefold(),
            )
            previous = relations.get(key)
            if previous is not None:
                report(
                    "identifier-collision",
                    f"physical relation collides with {previous}",
                )
            relations[key] = resource.unique_id
    return violations


def _dependency_violations(
    resource: Resource,
    layers: dict[str, Layer],
    by_id: dict[str, Resource],
    project: str,
) -> list[Violation]:
    violations: list[Violation] = []
    own_layer = layers.get(resource.unique_id)
    for dependency_id in resource.dependencies:
        dependency_type = dependency_id.partition(".")[0]
        if dependency_type in ALWAYS_ALLOWED:
            continue
        dependency = by_id.get(dependency_id)
        dependency_layer = layers.get(dependency_id)
        if dependency_type == "model" and dependency and dependency.package != project:
            dependency_type = "package model"
        elif dependency_type == "model" and dependency_layer is None:
            continue  # The dependency reports its own layer violation.
        name = dependency.name if dependency else dependency_id
        target = (
            f"{dependency_layer} model {name}"
            if dependency_layer
            else f"{dependency_type} {name}"
        )

        if resource.resource_type == "model" and own_layer is not None:
            if own_layer == "staging" and dependency_type in STAGING_INPUTS:
                continue
            if dependency_layer in MODEL_READS[own_layer]:
                if (
                    own_layer == "staging"
                    and _STAGING_NAME.fullmatch(name)
                    and _STAGING_NAME.fullmatch(resource.name)
                    and _staging_source(name) != _staging_source(resource.name)
                ):
                    violations.append(
                        Violation(
                            resource.name,
                            "cross-source-staging",
                            f"reads {target}; staging reads its own source only",
                        )
                    )
                continue
            allowed = {
                "staging": (
                    "sources, seeds, snapshots, package models, "
                    "and same-source staging models"
                ),
                "intermediate": "staging and intermediate models",
                "marts": "staging, intermediate, and mart models",
            }[own_layer]
            violations.append(
                Violation(
                    resource.name,
                    "forbidden-dependency",
                    f"reads {target}; {own_layer} reads {allowed}",
                )
            )
        elif resource.resource_type == "snapshot" and dependency_type != "source":
            violations.append(
                Violation(
                    resource.name,
                    "forbidden-dependency",
                    f"reads {target}; snapshots read sources only",
                )
            )
        elif resource.resource_type in CONSUMERS and dependency_layer != "marts":
            kind = CONSUMERS[resource.resource_type]
            violations.append(
                Violation(
                    resource.name,
                    "forbidden-dependency",
                    f"reads {target}; {kind}s read marts only",
                )
            )
    return violations


_JINJA = re.compile(r"\{#.*?#\}|\{%.*?%\}|\{\{(.*?)\}\}", re.DOTALL)
# sparse_source (macros/raw_files.sql) calls source() for its relation.
_DECLARED = re.compile(r"\s*(?:(?:ref|source|sparse_source)\s*\(|this\s*$)")
_PLACEHOLDER = re.compile(r"__jinja_(\d+)__")
_NOISE = re.compile(r"--[^\n]*|/\*.*?\*/|('(?:[^']|'')*')", re.DOTALL)
_STRING = "__string_literal__"
# Functions whose arguments may contain FROM without reading a relation.
_VALUE_FUNCTIONS = re.compile(
    r"\b(?:extract|trim|substring|position|overlay)\s*\([^()]*\)", re.IGNORECASE
)
_CTE = re.compile(
    r'([\w"]+)\s*(?:\([^()]*\)\s*)?as\s+(?:not\s+)?(?:materialized\s+)?\(',
    re.IGNORECASE,
)
_KEYWORD = re.compile(r"\b(distinct\s+)?(?:from|join)\b", re.IGNORECASE)
_NAME = r'(?:"[^"]*"|[\w$]+)(?:\.(?:"[^"]*"|[\w$]+))*'
_TARGET = re.compile(
    rf"\s*(?:lateral\b\s*)?(?!lateral\b)(@|{_NAME})(?:\s*(\()\s*(\w*))?",
    re.IGNORECASE,
)
_NEXT_TARGET = re.compile(r'(?:\s+as)?(?:\s+[\w"]+)?(?:\s*\([^()]*\))?\s*,')
# Table functions that produce rows without naming stored data.
ROW_FUNCTIONS = frozenset(
    {"flatten", "generate_series", "generator", "range", "split_to_table", "unnest"}
)


def _unquote(name: str) -> str:
    return name.strip('"').lower()


def _after_parens(sql: str, start: int) -> int:
    depth = 0
    for index in range(start, len(sql)):
        depth += {"(": 1, ")": -1}.get(sql[index], 0)
        if depth == 0:
            return index + 1
    return len(sql)


def find_hard_coded_relations(sql: str) -> list[str]:
    """Return FROM and JOIN targets that bypass ``ref()`` and ``source()``.

    This is a pattern check, not a SQL parser: a CTE name counts as declared
    anywhere in the query, and the first relation inside a parenthesized join
    is not checked.
    """
    expressions: list[str] = []

    def jinja(match: re.Match[str]) -> str:
        if match.group(1) is None:
            return " "
        expressions.append(match.group(1).strip())
        return f"__jinja_{len(expressions) - 1}__"

    def display(name: str) -> str:
        return _PLACEHOLDER.sub(
            lambda match: f"{{{{ {expressions[int(match.group(1))]} }}}}", name
        )

    sql = _JINJA.sub(jinja, sql)
    sql = _NOISE.sub(lambda match: f" {_STRING} " if match.group(1) else " ", sql)
    sql = _VALUE_FUNCTIONS.sub(" ", sql)
    ctes = {_unquote(name) for name in cast(list[str], _CTE.findall(sql))}
    found: list[str] = []
    for keyword in _KEYWORD.finditer(sql):
        if keyword.group(1):
            continue  # IS [NOT] DISTINCT FROM compares values.
        position = keyword.end()
        while target := _TARGET.match(sql, position):
            name, call, inner = target.group(1, 2, 3)
            position = target.end()
            placeholder = _PLACEHOLDER.fullmatch(name)
            if name == "@":
                found.append("a stage")
            elif name == _STRING:
                found.append("a string literal")
            elif call:
                function = _unquote(name.rpartition(".")[2])
                position = _after_parens(sql, target.start(2))
                if function not in ROW_FUNCTIONS and not (
                    function == "table" and inner.lower() in ROW_FUNCTIONS
                ):
                    found.append(f"{function}(...)")
            elif placeholder:
                if not _DECLARED.match(expressions[int(placeholder.group(1))]):
                    found.append(display(name))
            elif "." in name or _unquote(name) not in ctes:
                found.append(display(name))
            following = _NEXT_TARGET.match(sql, position)
            if following is None:
                break
            position = following.end()
    return found


def check(
    resources: list[Resource], project: str, *, namespace: str | None = None
) -> list[Violation]:
    """Return every layer violation in ``project``'s own resources, sorted.

    Resources from installed dbt packages are not checked.
    """
    by_id = {resource.unique_id: resource for resource in resources}
    resources = [resource for resource in resources if resource.package == project]
    layers: dict[str, Layer] = {}
    violations = _naming_violations(resources, namespace)
    for resource in resources:
        if resource.resource_type != "model":
            continue
        model_violations = _layer_violations(resource)
        violations.extend(model_violations)
        folder = _folder_layer(resource.path)
        if folder is not None:
            layers[resource.unique_id] = folder
    for resource in resources:
        violations.extend(_dependency_violations(resource, layers, by_id, project))
        if (
            resource.resource_type in {"model", "snapshot"}
            and resource.language == "sql"
        ):
            violations.extend(
                Violation(
                    resource.name,
                    "hard-coded-relation",
                    f"reads {relation} without ref() or source()",
                )
                for relation in find_hard_coded_relations(resource.raw_code)
            )
    return sorted(violations)
