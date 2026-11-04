"""Typed, native dlt source state for one unfinished candidate snapshot."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import cast
from uuid import UUID

from dlt.pipeline.pipeline import Pipeline

from analytics_ingestion.loop_returns.errors import GuardError

SOURCE = "loop_candidate"


@dataclass(frozen=True)
class Checkpoint:
    scope: str
    snapshot_id: str
    start: datetime
    end: datetime
    next_start: datetime
    windows: int = 0
    rows: int = 0
    requests: int = 0
    splits: int = 0
    published: bool = False

    def state(self) -> dict[str, object]:
        return {
            "scope": self.scope,
            "snapshot_id": self.snapshot_id,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "next_start": self.next_start.isoformat(),
            "windows": self.windows,
            "rows": self.rows,
            "requests": self.requests,
            "splits": self.splits,
            "published": self.published,
        }


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise GuardError("Loop checkpoint is not an object")
    return cast(dict[str, object], value)


def _text(values: dict[str, object], key: str) -> str:
    value = values.get(key)
    if not isinstance(value, str) or not value:
        raise GuardError(f"Loop checkpoint {key} is invalid")
    return value


def _instant(values: dict[str, object], key: str) -> datetime:
    try:
        value = datetime.fromisoformat(_text(values, key))
    except ValueError:
        raise GuardError(f"Loop checkpoint {key} is invalid") from None
    if value.tzinfo is None:
        raise GuardError(f"Loop checkpoint {key} has no timezone")
    return value


def _count(values: dict[str, object], key: str) -> int:
    value = values.get(key)
    if type(value) is not int or value < 0:
        raise GuardError(f"Loop checkpoint {key} is invalid")
    return value


def restore(pipe: Pipeline) -> Checkpoint | None:
    # dlt's state typing is permissive; parse it at this boundary.
    raw: object = pipe.state  # pyright: ignore[reportAny]
    sources = _mapping(_mapping(raw).get("sources", {}))
    source = _mapping(sources.get(SOURCE, {}))
    saved = source.get("checkpoint")
    if saved is None:
        return None
    values = _mapping(saved)
    snapshot_id = _text(values, "snapshot_id")
    try:
        valid_id = UUID(snapshot_id).hex == snapshot_id
    except ValueError:
        valid_id = False
    if not valid_id:
        raise GuardError("Loop checkpoint snapshot identity is invalid")
    published = values.get("published")
    if type(published) is not bool:
        raise GuardError("Loop checkpoint publication state is invalid")
    result = Checkpoint(
        scope=_text(values, "scope"),
        snapshot_id=snapshot_id,
        start=_instant(values, "start"),
        end=_instant(values, "end"),
        next_start=_instant(values, "next_start"),
        windows=_count(values, "windows"),
        rows=_count(values, "rows"),
        requests=_count(values, "requests"),
        splits=_count(values, "splits"),
        published=published,
    )
    if (
        not result.start <= result.next_start <= result.end
        or result.start >= result.end
    ):
        raise GuardError("Loop checkpoint window bounds are invalid")
    if result.published and result.next_start != result.end:
        raise GuardError("Incomplete Loop checkpoint cannot be published")
    return result
