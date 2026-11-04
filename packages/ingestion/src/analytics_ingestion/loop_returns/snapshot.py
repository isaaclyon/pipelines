"""Contiguous half-open windows that cover one snapshot's history."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True)
class Window:
    start: datetime
    end: datetime


def windows(start: datetime, end: datetime, *, hours: int) -> list[Window]:
    """Split ``[start, end)`` into ``hours``-long windows; the last one is clipped."""
    if hours <= 0:
        raise ValueError("Window hours must be positive")
    step = timedelta(hours=hours)
    result: list[Window] = []
    cursor = start
    while cursor < end:
        upper = min(cursor + step, end)
        result.append(Window(cursor, upper))
        cursor = upper
    return result
