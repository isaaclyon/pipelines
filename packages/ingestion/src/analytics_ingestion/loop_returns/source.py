"""One Loop Returns snapshot: fetch every window, validate, then load atomically.

Completed windows are staged with native dlt checkpoints. A failed or guarded
run leaves the published snapshot in place and resumes the unfinished candidate.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime

from analytics_ingestion.loop_returns.checkpoint import Checkpoint, restore
from analytics_ingestion.loop_returns.client import (
    HttpGet,
    LoopReturnsClient,
    urllib_get,
)
from analytics_ingestion.loop_returns.contract import to_row
from analytics_ingestion.loop_returns.dlt_load import (
    candidate_count,
    candidate_rows,
    pipeline,
    published_result,
    recover_pending,
    removed_count,
    run_snapshot,
    stage_window,
)
from analytics_ingestion.loop_returns.errors import GuardError
from analytics_ingestion.loop_returns.settings import DuckDbTarget, LoopSettings
from analytics_ingestion.loop_returns.snapshot import windows

Progress = Callable[[dict[str, object]], None]


@dataclass(frozen=True)
class LoadReport:
    snapshot_id: str
    shop_id: str
    rows: int
    removed: int
    windows: int
    requests: int
    splits: int
    snapshot_end: datetime
    completed_at: datetime

    def metadata(self) -> dict[str, object]:
        """Dagster-ready metadata; counts make a shrinking snapshot visible."""
        return {
            "snapshot_id": self.snapshot_id,
            "rows": self.rows,
            "removed": self.removed,
            "windows": self.windows,
            "requests": self.requests,
            "splits": self.splits,
            "snapshot_end": self.snapshot_end.isoformat(),
        }


def utc_now() -> datetime:
    return datetime.now(UTC)


def load_snapshot(
    settings: LoopSettings,
    *,
    transport: HttpGet = urllib_get,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], datetime] = utc_now,
    end: datetime | None = None,
    window_hours: int = 168,
    progress: Progress | None = None,
    restart_candidate: bool = False,
) -> LoadReport:
    settings.target_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (settings.target_root / "snapshot.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise GuardError("Loop snapshot is already running") from None
        return _load_snapshot(
            settings,
            transport=transport,
            sleep=sleep,
            clock=clock,
            end=end,
            window_hours=window_hours,
            progress=progress,
            restart_candidate=restart_candidate,
        )


def _scope(settings: LoopSettings, hours: int, end: datetime | None) -> str:
    target = settings.destination
    identity = (
        str(target.path.resolve())
        if isinstance(target, DuckDbTarget)
        else f"{target.account}/{target.database}"
    )
    values = [
        settings.environment,
        settings.shop_id,
        settings.dataset,
        identity,
        settings.history_start.isoformat(),
        str(hours),
        end.isoformat() if end else None,
    ]
    return hashlib.sha256(json.dumps(values).encode()).hexdigest()


def _restart_only_changes_bounds(
    saved: Checkpoint, settings: LoopSettings, hours: int
) -> bool:
    # Reconstruct the old scope: its cutoff was either explicit or clock-selected.
    # Matching the entire hash proves target/shop/window size are unchanged.
    previous = replace(settings, history_start=saved.start)
    return saved.scope in {
        _scope(previous, hours, None),
        _scope(previous, hours, saved.end),
    }


def _load_snapshot(
    settings: LoopSettings,
    *,
    transport: HttpGet,
    sleep: Callable[[float], None],
    clock: Callable[[], datetime],
    end: datetime | None,
    window_hours: int,
    progress: Progress | None,
    restart_candidate: bool,
) -> LoadReport:
    if type(window_hours) is not int or not 1 <= window_hours <= 168:
        raise GuardError("Loop window size must be between 1 and 168 hours")
    if end is not None and (end.tzinfo is None or end <= settings.history_start):
        raise GuardError(
            "Loop snapshot end must be timezone-aware and after history start"
        )
    started = time.monotonic()
    scope = _scope(settings, window_hours, end)
    candidate = pipeline(settings, candidate=True)
    pipe = pipeline(settings)
    saved = restore(candidate)
    if (
        saved is not None
        and saved.scope != scope
        and not (
            restart_candidate
            and _restart_only_changes_bounds(saved, settings, window_hours)
        )
        and (not saved.published or candidate.has_pending_data or pipe.has_pending_data)
    ):
        raise GuardError(
            "Loop candidate scope changed; unfinished work was not modified"
        )
    recovering_candidate = candidate.has_pending_data
    recover_pending(candidate)
    candidate.sync_destination()
    saved = restore(candidate)
    if (
        saved is not None
        and saved.scope != scope
        and not (
            restart_candidate
            and _restart_only_changes_bounds(saved, settings, window_hours)
        )
        and (not saved.published or candidate.has_pending_data or pipe.has_pending_data)
    ):
        raise GuardError("Restored Loop candidate scope does not match this request")
    recover_pending(pipe)
    if (
        saved is not None
        and saved.scope == scope
        and not restart_candidate
        and (not saved.published or recovering_candidate)
    ):
        result = published_result(pipe, saved)
        if result is not None:
            stage_window(candidate, [], replace(saved, published=True))
            removed, completed_at = result
            return LoadReport(
                saved.snapshot_id,
                settings.shop_id,
                saved.rows,
                removed,
                saved.windows,
                saved.requests,
                saved.splits,
                saved.end,
                completed_at,
            )
    if saved is None or saved.published or restart_candidate:
        upper = end or clock()
        if upper.tzinfo is None or upper <= settings.history_start:
            raise GuardError(
                "Loop snapshot needs a positive timezone-aware history range"
            )
        saved = Checkpoint(
            scope,
            uuid.uuid4().hex,
            settings.history_start,
            upper,
            settings.history_start,
        )
    planned = windows(saved.start, saved.end, hours=window_hours)
    if (
        saved.start != settings.history_start
        or saved.windows > len(planned)
        or saved.next_start
        != (planned[saved.windows - 1].end if saved.windows else saved.start)
    ):
        raise GuardError("Loop checkpoint does not match its window plan")
    if saved.windows and candidate_count(candidate, settings.shop_id) != saved.rows:
        raise GuardError("Loop candidate count does not match its checkpoint")

    def notify(phase: str, checkpoint: Checkpoint, **extra: object) -> None:
        if progress is not None:
            progress(
                {
                    "phase": phase,
                    "snapshot_id": checkpoint.snapshot_id,
                    "completed_windows": checkpoint.windows,
                    "total_windows": len(planned),
                    "rows": checkpoint.rows,
                    "elapsed_seconds": round(time.monotonic() - started, 3),
                    "snapshot_end": checkpoint.end.isoformat(),
                    **extra,
                }
            )

    notify("restarted" if restart_candidate else "resumed", saved)
    client = LoopReturnsClient(
        settings.api_key, settings.shop_id, http_get=transport, sleep=sleep
    )
    client.stats.requests = saved.requests
    client.stats.splits = saved.splits
    for window in planned[saved.windows :]:
        notify(
            "window_started",
            saved,
            start=window.start.isoformat(),
            end=window.end.isoformat(),
        )
        rows = [
            to_row(header) for header in client.fetch_window(window.start, window.end)
        ]
        next_checkpoint = replace(
            saved,
            next_start=window.end,
            windows=saved.windows + 1,
            rows=saved.rows + len(rows),
            requests=client.stats.requests,
            splits=client.stats.splits,
        )
        stage_window(candidate, rows, next_checkpoint, first=saved.windows == 0)
        if candidate_count(candidate, settings.shop_id) != next_checkpoint.rows:
            raise GuardError("Loop candidate has conflicting copies across windows")
        saved = next_checkpoint
        notify("window_committed", saved, window_rows=len(rows))

    removed = removed_count(pipe, candidate, settings.shop_id, saved.rows)
    if removed > settings.max_removed_rows:
        raise GuardError(
            f"snapshot would remove {removed} returns; the limit is "
            f"{settings.max_removed_rows}. Nothing was published."
        )
    completed_at = clock()
    evidence: dict[str, object] = {
        "snapshot_id": saved.snapshot_id,
        "shop_id": settings.shop_id,
        "row_count": saved.rows,
        "removed_count": removed,
        "window_count": saved.windows,
        "snapshot_end": saved.end,
        "completed_at": completed_at,
    }
    notify("publishing", saved)
    run_snapshot(pipe, candidate_rows(candidate), evidence)
    stage_window(candidate, [], replace(saved, published=True))
    notify("published", saved)
    return LoadReport(
        saved.snapshot_id,
        settings.shop_id,
        saved.rows,
        removed,
        saved.windows,
        saved.requests,
        saved.splits,
        saved.end,
        completed_at,
    )
