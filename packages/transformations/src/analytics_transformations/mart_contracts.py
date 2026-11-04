"""Parse dbt publication contracts and reject incompatible physical relations."""

import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path


def mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("Expected a JSON object")
    result: dict[str, object] = {}
    for key, item in value.items():  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType] # JSON boundary; copied into typed values below.
        if not isinstance(key, str):
            raise ValueError("Expected string object keys")
        result[key] = item
    return result


def sequence(value: object) -> tuple[object, ...]:
    if not isinstance(value, list):
        raise ValueError("Expected a JSON array")
    return tuple(value)  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType] # Elements enter as object and are parsed by callers.


def text(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Expected nonempty text")
    return value


def identifier(value: object) -> str:
    result = text(value)
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,254}", result):
        raise ValueError("Expected a lowercase unquoted identifier")
    return result


def checks_unique_key(test: dict[str, object], key: tuple[str, ...]) -> bool:
    """Whether a test is a ``unique_combination`` over exactly ``key``."""
    metadata = mapping(test.get("test_metadata", {}))
    if metadata.get("name") != "unique_combination":
        return False
    kwargs = mapping(metadata.get("kwargs", {}))
    if not isinstance(kwargs.get("columns"), list):
        return False
    names = tuple(identifier(item) for item in sequence(kwargs.get("columns")))
    return len(names) == len(key) and set(names) == set(key)


def check_blocking(test_id: str, test: dict[str, object]) -> bool:
    """Derive publication behavior from dbt's native severity, failing closed."""
    severity = mapping(test.get("config")).get("severity")
    if not isinstance(severity, str) or severity.lower() not in ("error", "warn"):
        raise ValueError(f"{test_id}: severity must be error or warn")
    return severity.lower() == "error"


@dataclass(frozen=True)
class KeyTests:
    """Blocking tests attached to one model, by test id."""

    not_null: frozenset[str]
    unique: frozenset[str]
    tests: dict[str, dict[str, object]]


def blocking_tests(model_id: str, tests: dict[str, dict[str, object]]) -> KeyTests:
    """Collect blocking tests on ``model_id`` and the columns they key."""
    not_null: set[str] = set()
    unique: set[str] = set()
    found: dict[str, dict[str, object]] = {}
    for test_id, test in tests.items():
        depends = sequence(mapping(test.get("depends_on", {"nodes": []})).get("nodes"))
        if model_id not in depends or not check_blocking(test_id, test):
            continue
        found[test_id] = test
        metadata = mapping(test.get("test_metadata", {}))
        column = mapping(metadata.get("kwargs", {})).get("column_name")
        if isinstance(column, str):
            if metadata.get("name") == "not_null":
                not_null.add(column)
            if metadata.get("name") == "unique":
                unique.add(column)
    return KeyTests(frozenset(not_null), frozenset(unique), found)


def has_unique_check(found: KeyTests, key: tuple[str, ...]) -> bool:
    """Whether ``key`` has a blocking uniqueness test.

    One column needs ``unique``; a composite key needs a
    ``unique_combination`` test over exactly its columns.
    """
    if len(key) == 1:
        return key[0] in found.unique
    return any(checks_unique_key(test, key) for test in found.tests.values())


def read_json(path: Path) -> object:
    return json.loads(path.read_text())  # pyright: ignore[reportAny] # Parsed immediately at the boundary by callers.


@dataclass(frozen=True)
class Column:
    name: str
    data_type: str
    description: str
    # Reviewed config.meta.pii; consumers such as Cube read it from the snapshot.
    pii: bool
    nullable: bool = True


@dataclass(frozen=True)
class MartContract:
    model_id: str
    relation: str
    description: str
    grain: str
    key: tuple[str, ...]
    columns: tuple[Column, ...]
    checks: tuple[str, ...]
    owner: str
    access: str = "analytical"


ACCESS_LEVELS = ("analytical", "restricted")


def analytical(plan: tuple[MartContract, ...]) -> tuple[MartContract, ...]:
    """Marts the default Cube reader may read."""
    return tuple(item for item in plan if item.access == "analytical")


def restricted(plan: tuple[MartContract, ...]) -> tuple[MartContract, ...]:
    """Marts that need a separate access boundary, such as personal data."""
    return tuple(item for item in plan if item.access == "restricted")


# Reviewed access-tightening migrations: relations that left ``analytical`` for
# ``restricted``. Customer activation stops serving order-contact personal data to
# the default reader once customer marts are enabled. Approved city/postal
# geography stays analytical, including ``fct_order_lines``. Never add a loosening here.
REVIEWED_ACCESS_TIGHTENINGS = ("dim_order_contacts",)


def _access(value: object) -> str:
    if not isinstance(value, str) or value not in ACCESS_LEVELS:
        raise ValueError(f"publication.access must be one of {ACCESS_LEVELS}")
    return value


def _pii(model_id: str, name: str, column: dict[str, object]) -> bool:
    config = mapping(column.get("config", {}))
    pii = mapping(config.get("meta", {})).get("pii")
    if not isinstance(pii, bool):
        raise ValueError(f"{model_id}.{name}: declare boolean config.meta.pii")
    return pii


def contracts(data: object) -> tuple[MartContract, ...]:
    """Require public tables, documented types/grain/key and blocking tests.

    ``config.meta.publication.checks`` names the model's consequential rules and
    reconciliation tests. Every declared test must exist and depend on this mart.
    Key uniqueness and non-null tests are verified independently of the list.
    """
    nodes = {
        key: mapping(value)
        for key, value in mapping(mapping(data).get("nodes")).items()
    }
    result: list[MartContract] = []
    for model_id, node in nodes.items():
        if node.get("resource_type") != "model" or not str(
            node.get("original_file_path", "")
        ).startswith("models/marts/"):
            continue
        config = mapping(node.get("config"))
        if (
            config.get("materialized") not in ("table", "incremental")
            or config.get("access") != "public"
            or mapping(config.get("contract")).get("enforced") is not True
        ):
            raise ValueError(
                f"{model_id}: marts require a public enforced table contract"
            )
        if config.get("grants"):
            raise ValueError(
                f"{model_id}: candidate grants belong to publication, not models"
            )
        publication = mapping(mapping(config.get("meta")).get("publication"))
        if "access" not in publication:
            raise ValueError(
                f"{model_id}: declare meta.publication.access "
                f"({' or '.join(ACCESS_LEVELS)})"
            )
        access = _access(publication.get("access"))
        key = tuple(identifier(item) for item in sequence(publication.get("key")))
        if not key or len(set(key)) != len(key):
            raise ValueError(f"{model_id}: declare a nonempty distinct grain key")
        columns = tuple(
            Column(
                identifier(name),
                text(mapping(column).get("data_type")).lower(),
                text(mapping(column).get("description")),
                _pii(model_id, name, mapping(column)),
                not any(
                    mapping(item).get("type") == "not_null"
                    for item in sequence(mapping(column).get("constraints", []))
                ),
            )
            for name, column in mapping(node.get("columns")).items()
        )
        if not columns or not set(key) <= {column.name for column in columns}:
            raise ValueError(f"{model_id}: key must name contracted columns")
        requested = {text(item) for item in sequence(publication.get("checks"))}
        if not requested:
            raise ValueError(
                f"{model_id}: declare correctness and reconciliation checks"
            )
        found = blocking_tests(
            model_id,
            {
                test_id: test
                for test_id, test in nodes.items()
                if test.get("resource_type") == "test"
            },
        )
        requested -= {text(test.get("name")) for test in found.tests.values()}
        if requested or not set(key) <= found.not_null:
            raise ValueError(
                f"{model_id}: missing blocking checks or key not-null tests"
            )
        if not has_unique_check(found, key):
            raise ValueError(
                f"{model_id}: key needs a blocking unique test, or for a "
                "composite key a blocking "
                "unique_combination test over the key"
            )
        result.append(
            MartContract(
                model_id,
                identifier(node.get("alias")),
                text(node.get("description")),
                text(publication.get("grain")),
                key,
                columns,
                tuple(sorted(found.tests)),
                text(mapping(config.get("meta")).get("owner")),
                access,
            )
        )
    if len({item.relation for item in result}) != len(result):
        raise ValueError("Published aliases must be distinct")
    return tuple(sorted(result, key=lambda item: item.relation))


def compatible(
    previous: tuple[MartContract, ...], candidate: tuple[MartContract, ...]
) -> None:
    """Existing physical columns/types/grain/keys cannot disappear or change."""
    current = {item.relation: item for item in candidate}
    for old in previous:
        new = current.get(old.relation)
        if new is None:
            raise ValueError(f"Published relation removed: {old.relation}")
        if (old.grain, old.key) != (new.grain, new.key):
            raise ValueError(f"Published grain changed: {old.relation}")
        if old.access != new.access:
            raise ValueError(
                f"Published access changed: {old.relation}; "
                "reclassification needs an explicit migration"
            )
        new_columns = {column.name: column for column in new.columns}
        for column in old.columns:
            if (
                column.name not in new_columns
                or new_columns[column.name].data_type != column.data_type
                or (column.nullable and not new_columns[column.name].nullable)
            ):
                raise ValueError(
                    f"Published column removed or retyped: {old.relation}.{column.name}"
                )

        old_names = {column.name for column in old.columns}
        if any(
            not column.nullable and column.name not in old_names
            for column in new.columns
        ):
            raise ValueError(f"New published columns must be nullable: {old.relation}")


def snapshot(plan: tuple[MartContract, ...]) -> str:
    return json.dumps([asdict(item) for item in plan], indent=2) + "\n"


def _boolean(value: object) -> bool:
    if not isinstance(value, bool):
        raise ValueError("Expected a boolean")
    return value


def read_snapshot(data: object) -> tuple[MartContract, ...]:
    result: list[MartContract] = []
    for entry in sequence(data):
        item = mapping(entry)
        result.append(
            MartContract(
                text(item.get("model_id")),
                identifier(item.get("relation")),
                text(item.get("description")),
                text(item.get("grain")),
                tuple(identifier(key) for key in sequence(item.get("key"))),
                tuple(
                    Column(
                        identifier(column.get("name")),
                        text(column.get("data_type")),
                        text(column.get("description")),
                        _boolean(column.get("pii")),
                        _boolean(column.get("nullable")),
                    )
                    for column in (
                        mapping(value) for value in sequence(item.get("columns"))
                    )
                ),
                tuple(text(check) for check in sequence(item.get("checks"))),
                text(item.get("owner")),
                _access(item.get("access")),
            )
        )
    return tuple(result)


def main() -> None:
    plan = contracts(read_json(Path(sys.argv[1])))
    if sys.argv[2] == "--snapshot":
        print(snapshot(plan), end="")
        return
    previous = read_snapshot(read_json(Path(sys.argv[2])))
    compatible(previous, plan)
    if snapshot(plan) != snapshot(previous):
        raise ValueError(
            "Reviewed contract snapshot must match the manifest; "
            "update after compatibility review"
        )


if __name__ == "__main__":
    main()
