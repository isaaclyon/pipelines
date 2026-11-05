"""Parse dbt manifest JSON values and read data-test severities."""

import json
import re
from dataclasses import dataclass
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
    """Whether a test blocks the build, from dbt's native severity, failing closed."""
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
