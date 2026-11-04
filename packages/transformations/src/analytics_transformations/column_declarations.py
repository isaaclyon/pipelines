"""Validate model output names, types and machine-readable column meanings.

This checks declarations, not SQL semantics. Currency correctness tests must
reference both sides of a parent association; review establishes their SQL
meaning. Temporal value tests establish conversion correctness separately.
"""

import re
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from analytics_transformations.layer_dependencies import root_project
from analytics_transformations.mart_contracts import (
    check_blocking,
    identifier,
    mapping,
    sequence,
    text,
)


def normalize_data_type(value: object) -> str:
    """Normalize supported adapter spellings without erasing decimal precision."""
    value = re.sub(r"\s+", " ", text(value).strip().lower())
    value = re.sub(r"\s*([(),])\s*", r"\1", value)
    aliases = {
        "text": "varchar",
        "string": "varchar",
        "bool": "boolean",
        "int": "integer",
        "int4": "integer",
        "int8": "bigint",
        "int2": "smallint",
        "double precision": "double",
        "float8": "double",
        "timestamp_ntz": "timestamp",
        "timestamp without time zone": "timestamp",
        "timestamp with time zone": "timestamptz",
        "timestamp_tz": "timestamptz",
    }
    value = aliases.get(value, value)
    value = re.sub(r"^(number|numeric)\b", "decimal", value)
    if value == "decimal":
        return "decimal(38,0)"
    return re.sub(r"^decimal\(([0-9]+)\)$", r"decimal(\1,0)", value)


def _name(value: object) -> str:
    return identifier(value)


def _columns(node: dict[str, object]) -> dict[str, dict[str, object]]:
    columns = {
        name: mapping(value) for name, value in mapping(node.get("columns")).items()
    }
    if not columns:
        raise ValueError("declare every output column")
    return columns


def _meta(column: dict[str, object]) -> dict[str, object]:
    return mapping(mapping(column.get("config", {})).get("meta", {}))


def _test(
    name: object, model_ids: set[str], nodes: dict[str, dict[str, object]]
) -> None:
    requested = text(name)
    matches = [
        (key, node)
        for key, node in nodes.items()
        if node.get("resource_type") == "test" and node.get("name") == requested
    ]
    if len(matches) != 1:
        raise ValueError("correctness test must identify exactly one test")
    test_id, test = matches[0]
    dependencies = {
        text(value) for value in sequence(mapping(test.get("depends_on")).get("nodes"))
    }
    generic = mapping(test.get("test_metadata", {}) or {}).get("name")
    if (
        not model_ids <= dependencies
        or not check_blocking(test_id, test)
        or generic
        in (
            "not_null",
            "unique",
            "unique_combination",
            "relationships",
            "accepted_values",
        )
    ):
        raise ValueError("correctness test must block and cover the declared models")


def _currency(
    association: object,
    model_id: str,
    node: dict[str, object],
    columns: dict[str, dict[str, object]],
    nodes: dict[str, dict[str, object]],
) -> None:
    currency = mapping(association)
    parent_name = currency.get("model")
    target_columns = columns
    if parent_name is not None:
        if str(node.get("original_file_path", "")).startswith("models/marts/"):
            raise ValueError("mart currency must be exposed locally")
        parent_name = text(parent_name)
        parents = [
            (key, item)
            for key, item in nodes.items()
            if item.get("resource_type") == "model"
            and (key == parent_name or item.get("name") == parent_name)
            and item.get("package_name") == node.get("package_name")
        ]
        if len(parents) != 1:
            raise ValueError("currency parent must identify exactly one project model")
        parent_id, parent = parents[0]
        if parent_id == model_id:
            raise ValueError("use a local currency association for this model")
        target_columns = _columns(parent)
        join = mapping(currency.get("join"))
        if not join:
            raise ValueError("currency parent requires join keys")
        for child_key, parent_key in join.items():
            if (
                _name(child_key) not in columns
                or _name(parent_key) not in target_columns
            ):
                raise ValueError(
                    "currency join must reference declared columns on both models"
                )
        _test(currency.get("test"), {model_id, parent_id}, nodes)
    elif "join" in currency or "test" in currency:
        raise ValueError("local currency association only declares its column")
    currency_name = _name(currency.get("column"))
    if currency_name not in target_columns or not normalize_data_type(
        target_columns[currency_name].get("data_type")
    ).startswith("varchar"):
        raise ValueError("currency must reference a declared text column")


def _temporal(
    data_type: str,
    meta: dict[str, object],
    columns: dict[str, dict[str, object]],
    model_id: str,
    nodes: dict[str, dict[str, object]],
) -> None:
    temporal = mapping(meta.get("temporal"))
    kind = temporal.get("kind")
    if data_type.startswith("timestamp"):
        if kind != "instant" or temporal.get("timezone") != "UTC":
            raise ValueError(
                "timestamps must declare temporal kind instant and timezone UTC"
            )
    elif kind == "derived_date":
        instant = _name(temporal.get("instant"))
        if instant not in columns or not normalize_data_type(
            columns[instant].get("data_type")
        ).startswith("timestamp"):
            raise ValueError("derived date must reference a declared timestamp column")
        try:
            _ = ZoneInfo(text(temporal.get("timezone")))
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise ValueError(
                "derived date requires a valid conversion timezone"
            ) from error
    elif kind != "source_date":
        raise ValueError("dates require source_date or derived_date temporal meaning")
    if "test" in temporal:
        _test(temporal["test"], {model_id}, nodes)


def _column(
    name: str,
    column: dict[str, object],
    model_id: str,
    node: dict[str, object],
    columns: dict[str, dict[str, object]],
    nodes: dict[str, dict[str, object]],
) -> None:
    _ = _name(name)
    if "name" in column and column["name"] != name:
        raise ValueError("column name must match its declaration key")
    if not str(column.get("description", "")).strip():
        raise ValueError("add a column description")
    meta = _meta(column)
    published = str(node.get("original_file_path", "")).startswith("models/marts/")
    # Keep types where a declared correctness guarantee needs them. Ordinary
    # internal fields need an inventory and PII decision, not a second schema.
    needs_type = (
        published
        or name.endswith(("_amount", "_count", "_quantity"))
        or any(key in meta for key in ("currency", "temporal"))
    )
    if column.get("data_type") is None and not needs_type:
        return
    data_type = normalize_data_type(column.get("data_type"))
    integer = data_type in {"smallint", "integer", "bigint", "hugeint"} or bool(
        re.fullmatch(r"decimal\([0-9]+,0\)", data_type)
    )
    if name.endswith("_count") and not integer:
        raise ValueError("counts require an exact integer type")
    if name.endswith("_quantity") and not (
        integer
        or data_type in {"float", "double", "real"}
        or re.fullmatch(r"decimal\([0-9]+,[0-9]+\)", data_type)
    ):
        raise ValueError("quantities require a numeric type")
    timestamp = data_type.startswith("timestamp")
    if timestamp or data_type == "date":
        if published or "temporal" in meta:
            _temporal(data_type, meta, columns, model_id, nodes)
    elif "temporal" in meta:
        raise ValueError("temporal meaning requires a timestamp or date type")
    if name.endswith("_amount") or "currency" in meta:
        decimal = re.fullmatch(r"decimal\(([0-9]+),([0-9]+)\)", data_type)
        if (
            decimal is None
            or not 0 <= int(decimal[2]) <= int(decimal[1]) <= 38
            or int(decimal[1]) == 0
        ):
            raise ValueError("money requires an exact decimal type")
        if published or "currency" in meta:
            _currency(meta.get("currency"), model_id, node, columns, nodes)
    if published and (name == "id" or name.endswith("_id") or "identity" in meta):
        identity = mapping(meta.get("identity", {}))
        if identity.get("kind") == "source":
            source = _name(identity.get("source"))
            if not name.startswith(source + "_") or name == source + "_id":
                raise ValueError(
                    "mart source identities require a source-qualified entity_id"
                )
        elif identity.get("kind") == "analytical":
            _ = text(identity.get("meaning"))
        else:
            raise ValueError(
                "mart identities require explicit source or analytical meaning"
            )


def check_column_declarations(data: object) -> None:
    """Validate this project's model columns in a parsed dbt manifest."""
    manifest = mapping(data)
    nodes = {
        key: mapping(value) for key, value in mapping(manifest.get("nodes")).items()
    }
    project = root_project(data)
    for model_id, node in nodes.items():
        if node.get("resource_type") != "model" or node.get("package_name") != project:
            continue
        try:
            columns = _columns(node)
            for name, column in columns.items():
                try:
                    _column(name, column, model_id, node, columns, nodes)
                except ValueError as error:
                    raise ValueError(f"column {name}: {error}") from error
        except ValueError as error:
            raise ValueError(f"{model_id}: {error}") from error
