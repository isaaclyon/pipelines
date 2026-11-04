"""Loop Returns snapshot load into DuckDB: retraction, evidence and failure safety."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from analytics_ingestion.loop_returns.contract import RETURN_COLUMNS
from analytics_ingestion.loop_returns.errors import (
    ContractError,
    GuardError,
    SourceError,
)
from analytics_ingestion.loop_returns.settings import (
    DuckDbTarget,
    LoopSettings,
)
from analytics_ingestion.loop_returns.snapshot import windows
from analytics_ingestion.loop_returns.source import load_snapshot
from tests.loop_returns_fakes import (
    API_KEY,
    SHOP_ID,
    FakeLoopApi,
    api_return,
    ok,
    status,
)

RUN_END = datetime(2026, 7, 20, 12, 0, 0, tzinfo=UTC)
HISTORY_START = datetime(2026, 7, 1, tzinfo=UTC)
DATASET = "loop_return_headers_raw"
CREATED_A = "2026-07-02T10:00:00+00:00"
CREATED_B = "2026-07-09T10:00:00+00:00"
CREATED_C = "2026-07-15T10:00:00+00:00"


def settings(tmp_path: Path, max_removed_rows: int = 200) -> LoopSettings:
    return LoopSettings(
        environment="local",
        api_key=API_KEY,
        shop_id=SHOP_ID,
        state_root=tmp_path / "state",
        dataset=DATASET,
        history_start=HISTORY_START,
        max_removed_rows=max_removed_rows,
        destination=DuckDbTarget(tmp_path / "raw.duckdb"),
    )


def clock() -> datetime:
    return RUN_END


def run(
    tmp_path: Path,
    api: FakeLoopApi,
    max_removed_rows: int = 200,
) -> None:
    _ = load_snapshot(
        settings(tmp_path, max_removed_rows),
        transport=api,
        sleep=lambda _seconds: None,
        clock=clock,
    )


def query(tmp_path: Path, sql: str) -> list[tuple[object, ...]]:
    with duckdb.connect(str(tmp_path / "raw.duckdb"), read_only=True) as connection:
        return connection.sql(sql).fetchall()


def return_ids(tmp_path: Path) -> list[str]:
    rows = query(
        tmp_path, f"select return_id from {DATASET}.returns order by return_id"
    )
    return [str(row[0]) for row in rows]


def test_windows_are_contiguous_half_open_and_clip_the_last_one() -> None:
    windows_ = windows(HISTORY_START, RUN_END, hours=168)

    assert [(w.start, w.end) for w in windows_] == [
        (datetime(2026, 7, 1, tzinfo=UTC), datetime(2026, 7, 8, tzinfo=UTC)),
        (datetime(2026, 7, 8, tzinfo=UTC), datetime(2026, 7, 15, tzinfo=UTC)),
        (datetime(2026, 7, 15, tzinfo=UTC), RUN_END),
    ]


def test_first_load_writes_headers_and_one_evidence_row(tmp_path: Path) -> None:
    api = FakeLoopApi(
        records=[
            api_return("1", CREATED_A),
            api_return("2", CREATED_B, handling_fee="N/A"),
            api_return("3", CREATED_C),
        ]
    )

    run(tmp_path, api)

    assert return_ids(tmp_path) == ["1", "2", "3"]
    evidence = query(
        tmp_path,
        f"select row_count, removed_count, window_count, "
        f"snapshot_end at time zone 'UTC' from {DATASET}.return_snapshots",
    )
    assert evidence == [(3, 0, 3, RUN_END.replace(tzinfo=None))]


def test_loaded_columns_are_exactly_the_non_pii_contract(tmp_path: Path) -> None:
    run(tmp_path, FakeLoopApi(records=[api_return("1", CREATED_A)]))

    columns = query(
        tmp_path,
        f"select column_name from information_schema.columns "
        f"where table_schema = '{DATASET}' and table_name = 'returns' "
        f"and not starts_with(column_name, '_dlt') "
        f"order by ordinal_position",
    )
    assert [str(row[0]) for row in columns] == [c.name for c in RETURN_COLUMNS]
    assert {"email", "customer", "tracking_number"}.isdisjoint(
        str(row[0]) for row in columns
    )


def test_money_is_exact_decimal_and_absent_money_is_null(tmp_path: Path) -> None:
    api = FakeLoopApi(
        records=[api_return("1", CREATED_A, handling_fee="15", exchange_total="N/A")]
    )

    run(tmp_path, api)

    types = query(
        tmp_path,
        f"select data_type from information_schema.columns "
        f"where table_schema = '{DATASET}' and table_name = 'returns' "
        f"and column_name = 'handling_fee'",
    )
    assert types == [("DECIMAL(38,9)",)]
    row = query(
        tmp_path,
        f"select handling_fee, exchange_total, shopify_order_id, "
        f"created_at at time zone 'UTC' from {DATASET}.returns",
    )
    fee, exchange, order, created = row[0]
    assert fee == Decimal("15")
    assert exchange is None
    assert order == "12600441897332"
    assert created == datetime(2026, 7, 2, 10, 0, 0)


def test_boundary_rows_are_loaded_once(tmp_path: Path) -> None:
    api = FakeLoopApi(
        records=[
            api_return("1", "2026-07-08T00:00:00+00:00"),
            api_return("2", "2026-07-15T00:00:00+00:00"),
            api_return("3", "2026-07-01T00:00:00+00:00"),
        ],
        exclusive_bounds=True,
    )

    run(tmp_path, api)

    assert return_ids(tmp_path) == ["1", "2", "3"]


def test_reload_retracts_a_return_the_snapshot_no_longer_contains(
    tmp_path: Path,
) -> None:
    first = FakeLoopApi(
        records=[api_return("1", CREATED_A), api_return("2", CREATED_B)]
    )
    run(tmp_path, first)

    second = FakeLoopApi(records=[api_return("1", CREATED_A)])
    run(tmp_path, second)

    assert return_ids(tmp_path) == ["1"]
    removed = query(
        tmp_path,
        f"select removed_count from {DATASET}.return_snapshots order by removed_count",
    )
    assert removed == [(0,), (1,)]


def test_failed_collection_keeps_the_previous_snapshot(tmp_path: Path) -> None:
    run(
        tmp_path,
        FakeLoopApi(records=[api_return("1", CREATED_A), api_return("2", CREATED_C)]),
    )
    broken = FakeLoopApi(records=[api_return("1", CREATED_A)])
    broken.scripted = [
        ok([api_return("1", CREATED_A)]),
        status(500),
        status(500),
        status(500),
        status(500),
    ]

    with pytest.raises(SourceError):
        run(tmp_path, broken)

    assert return_ids(tmp_path) == ["1", "2"]
    evidence = query(tmp_path, f"select count(*) from {DATASET}.return_snapshots")
    assert evidence == [(1,)]


def test_retry_resumes_committed_windows_without_exposing_partial_rows(
    tmp_path: Path,
) -> None:
    run(tmp_path, FakeLoopApi(records=[api_return("old", CREATED_A)]))
    broken = FakeLoopApi()
    broken.scripted = [ok([api_return("1", CREATED_A)]), *([status(500)] * 4)]
    with pytest.raises(SourceError):
        run(tmp_path, broken)
    assert return_ids(tmp_path) == ["old"]

    retry = FakeLoopApi(
        records=[api_return("2", CREATED_B), api_return("3", CREATED_C)]
    )
    result = load_snapshot(
        settings(tmp_path),
        transport=retry,
        sleep=lambda _seconds: None,
        clock=lambda: datetime(2026, 7, 21, tzinfo=UTC),
    )
    assert return_ids(tmp_path) == ["1", "2", "3"]
    assert result.snapshot_end == RUN_END
    assert len(retry.calls) == 2
    assert retry.calls[0].start is not None
    assert retry.calls[0].start > HISTORY_START


def test_resume_refuses_a_changed_history_scope(tmp_path: Path) -> None:
    from dataclasses import replace

    broken = FakeLoopApi()
    broken.scripted = [ok([api_return("1", CREATED_A)]), *([status(500)] * 4)]
    with pytest.raises(SourceError):
        run(tmp_path, broken)
    api = FakeLoopApi()
    changed = replace(
        settings(tmp_path), history_start=datetime(2026, 7, 2, tzinfo=UTC)
    )
    with pytest.raises(GuardError, match="scope"):
        _ = load_snapshot(changed, transport=api, clock=clock)
    assert api.calls == []


def test_mass_removal_above_the_limit_is_refused(tmp_path: Path) -> None:
    run(
        tmp_path,
        FakeLoopApi(
            records=[
                api_return("1", CREATED_A),
                api_return("2", CREATED_B),
                api_return("3", CREATED_C),
            ]
        ),
    )

    with pytest.raises(GuardError, match="2"):
        run(
            tmp_path,
            FakeLoopApi(records=[api_return("1", CREATED_A)]),
            max_removed_rows=1,
        )

    assert return_ids(tmp_path) == ["1", "2", "3"]


def test_empty_history_is_refused_instead_of_erasing_the_table(tmp_path: Path) -> None:
    run(tmp_path, FakeLoopApi(records=[api_return("1", CREATED_A)]))

    with pytest.raises(GuardError, match="empty"):
        run(tmp_path, FakeLoopApi(records=[]))

    assert return_ids(tmp_path) == ["1"]


def test_conflicting_copies_of_one_return_fail_closed(tmp_path: Path) -> None:
    api = FakeLoopApi(
        records=[
            api_return("1", CREATED_A),
            api_return("1", CREATED_A, state="open"),
        ]
    )

    with pytest.raises(ContractError, match="conflicting"):
        run(tmp_path, api)

    assert query(
        tmp_path,
        f"select count(*) from information_schema.tables "
        f"where table_schema = '{DATASET}' and table_name = 'returns'",
    ) == [(0,)]


def test_another_shop_cannot_replace_the_snapshot(tmp_path: Path) -> None:
    from dataclasses import replace

    run(tmp_path, FakeLoopApi(records=[api_return("1", CREATED_A)]))
    other = replace(settings(tmp_path), shop_id="gid://shopify/Shop/2")
    with pytest.raises(GuardError, match="shop"):
        _ = load_snapshot(
            other,
            transport=FakeLoopApi(records=[api_return("2", CREATED_A)]),
            clock=clock,
        )
    assert return_ids(tmp_path) == ["1"]


def test_removal_guard_uses_destination_when_dlt_state_is_lost(tmp_path: Path) -> None:
    import shutil

    run(tmp_path, FakeLoopApi(records=[api_return("1", CREATED_A)]))
    shutil.rmtree(settings(tmp_path).target_root / "dlt_source")
    with duckdb.connect(str(tmp_path / "raw.duckdb")) as connection:
        _ = connection.execute(f"update {DATASET}._dlt_loads set status = 1")
    with pytest.raises(GuardError, match="empty"):
        run(tmp_path, FakeLoopApi(records=[]))
    assert return_ids(tmp_path) == ["1"]


def test_concurrent_local_snapshot_is_refused(tmp_path: Path) -> None:
    import fcntl

    root = settings(tmp_path).target_root
    root.mkdir(parents=True)
    with (root / "snapshot.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(GuardError, match="already running"):
            run(tmp_path, FakeLoopApi(records=[api_return("1", CREATED_A)]))
    assert not (tmp_path / "raw.duckdb").exists()


def test_empty_first_snapshot_creates_the_declared_table(tmp_path: Path) -> None:
    run(tmp_path, FakeLoopApi(records=[]))
    assert return_ids(tmp_path) == []


def test_committed_window_survives_a_killed_process(tmp_path: Path) -> None:
    import signal
    import subprocess
    import sys

    script = f"""
import os, signal
from pathlib import Path
from analytics_ingestion.loop_returns.source import load_snapshot
from tests.test_loop_returns_load import settings, clock, CREATED_A
from tests.loop_returns_fakes import FakeLoopApi, api_return

def interrupt(event):
    if event['phase'] == 'window_committed':
        os.kill(os.getpid(), signal.SIGKILL)

load_snapshot(settings(Path({str(tmp_path)!r})),
              transport=FakeLoopApi(records=[api_return('1', CREATED_A)]),
              clock=clock, progress=interrupt)
"""
    result = subprocess.run([sys.executable, "-c", script], timeout=30, check=False)
    assert result.returncode == -signal.SIGKILL
    retry = FakeLoopApi(records=[api_return("2", CREATED_B)])
    run(tmp_path, retry)
    assert return_ids(tmp_path) == ["1", "2"]
    assert len(retry.calls) == 2


def test_progress_reports_restored_and_committed_window_counts(tmp_path: Path) -> None:
    events: list[dict[str, object]] = []
    _ = load_snapshot(
        settings(tmp_path),
        clock=clock,
        transport=FakeLoopApi(records=[api_return("1", CREATED_A)]),
        progress=events.append,
    )
    committed = [event for event in events if event["phase"] == "window_committed"]
    assert [event["completed_windows"] for event in committed] == [1, 2, 3]
    assert all(event["total_windows"] == 3 for event in committed)
    assert [event["window_rows"] for event in committed] == [1, 0, 0]
    assert events[-1]["phase"] == "published"
    assert all("api_key" not in event and "records" not in event for event in events)


def test_pending_first_window_recovers_before_destination_sync(tmp_path: Path) -> None:
    import signal
    import subprocess
    import sys

    script = f"""
import os, signal
from pathlib import Path
from dlt.pipeline.pipeline import Pipeline
from analytics_ingestion.loop_returns.source import load_snapshot
from tests.test_loop_returns_load import settings, clock, CREATED_A
from tests.loop_returns_fakes import FakeLoopApi, api_return

def interrupted_load(*args, **kwargs):
    os.kill(os.getpid(), signal.SIGKILL)
Pipeline.load = interrupted_load
load_snapshot(settings(Path({str(tmp_path)!r})),
              transport=FakeLoopApi(records=[api_return('1', CREATED_A)]), clock=clock)
"""
    result = subprocess.run([sys.executable, "-c", script], timeout=30, check=False)
    assert result.returncode == -signal.SIGKILL
    from dataclasses import replace

    wrong_target = replace(
        settings(tmp_path), destination=DuckDbTarget(tmp_path / "other.duckdb")
    )
    refused = FakeLoopApi(records=[api_return("1", CREATED_A)])
    with pytest.raises(GuardError, match="scope"):
        _ = load_snapshot(
            wrong_target, transport=refused, clock=clock, restart_candidate=True
        )
    assert refused.calls == []
    assert not (tmp_path / "other.duckdb").exists()
    retry = FakeLoopApi(records=[api_return("2", CREATED_B)])
    run(tmp_path, retry)
    assert return_ids(tmp_path) == ["1", "2"]
    assert len(retry.calls) == 2


def test_retry_after_publication_finishes_ack_without_reextracting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dlt.pipeline.pipeline import Pipeline

    from analytics_ingestion.loop_returns import source
    from analytics_ingestion.loop_returns.checkpoint import Checkpoint

    real_stage = source.stage_window

    def interrupt_ack(
        pipe: Pipeline,
        rows: list[dict[str, object]],
        checkpoint: Checkpoint,
        *,
        first: bool = False,
    ) -> None:
        if checkpoint.published:
            raise SourceError("interrupted before publication acknowledgement")
        real_stage(pipe, rows, checkpoint, first=first)

    with monkeypatch.context() as patch:
        patch.setattr(source, "stage_window", interrupt_ack)
        with pytest.raises(SourceError, match="acknowledgement"):
            run(tmp_path, FakeLoopApi(records=[api_return("1", CREATED_A)]))
    assert return_ids(tmp_path) == ["1"]
    retry = FakeLoopApi()
    run(tmp_path, retry)
    assert retry.calls == []
    assert return_ids(tmp_path) == ["1"]
    assert query(tmp_path, f"select count(*) from {DATASET}.return_snapshots") == [(1,)]


@pytest.mark.parametrize("rejection", ["empty", "removal", "conflict"])
def test_explicit_restart_rebuilds_rejected_candidate_without_losing_publication(
    tmp_path: Path,
    rejection: str,
) -> None:
    records = [api_return("1", CREATED_A), api_return("2", CREATED_B)]
    run(tmp_path, FakeLoopApi(records=records), max_removed_rows=0)
    rejected = FakeLoopApi(records=[] if rejection == "empty" else records[:1])
    if rejection == "conflict":
        rejected.scripted = [
            ok([api_return("1", CREATED_A)]),
            ok([api_return("1", CREATED_B)]),
        ]
    with pytest.raises(GuardError):
        run(tmp_path, rejected, max_removed_rows=0)
    assert return_ids(tmp_path) == ["1", "2"]
    assert query(tmp_path, f"select count(*) from {DATASET}.return_snapshots") == [(1,)]

    healthy = FakeLoopApi(records=records)
    _ = load_snapshot(
        settings(tmp_path, max_removed_rows=0),
        transport=healthy,
        clock=clock,
        restart_candidate=True,
    )
    assert len(healthy.calls) == 3
    assert return_ids(tmp_path) == ["1", "2"]
    assert query(tmp_path, f"select count(*) from {DATASET}.return_snapshots") == [(2,)]


def test_acknowledged_snapshot_allows_a_new_history_scope(tmp_path: Path) -> None:
    from dataclasses import replace

    run(tmp_path, FakeLoopApi(records=[api_return("1", CREATED_A)]))
    changed = replace(
        settings(tmp_path), history_start=datetime(2026, 7, 2, tzinfo=UTC)
    )
    api = FakeLoopApi(records=[api_return("1", CREATED_A)])
    _ = load_snapshot(changed, transport=api, clock=clock)
    assert len(api.calls) == 3
    assert return_ids(tmp_path) == ["1"]


@pytest.mark.parametrize("bound", ["start", "end"])
def test_restart_can_correct_rejected_history_bounds(
    tmp_path: Path, bound: str
) -> None:
    from dataclasses import replace

    records = [api_return("1", CREATED_A), api_return("2", CREATED_B)]
    original = settings(tmp_path, max_removed_rows=0)
    _ = load_snapshot(original, transport=FakeLoopApi(records=records), clock=clock)
    changed = (
        replace(original, history_start=datetime(2026, 7, 8, tzinfo=UTC))
        if bound == "start"
        else original
    )
    cutoff = datetime(2026, 7, 8, tzinfo=UTC) if bound == "end" else None
    with pytest.raises(GuardError, match="remove"):
        _ = load_snapshot(
            changed, transport=FakeLoopApi(records=records), clock=clock, end=cutoff
        )
    assert return_ids(tmp_path) == ["1", "2"]
    healthy = FakeLoopApi(records=records)
    _ = load_snapshot(original, transport=healthy, clock=clock, restart_candidate=True)
    assert len(healthy.calls) == 3
    assert return_ids(tmp_path) == ["1", "2"]
