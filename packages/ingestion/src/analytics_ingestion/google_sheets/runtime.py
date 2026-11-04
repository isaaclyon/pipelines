"""Live local Google Sheets captures from the kit settings file ``google_sheets.json``.

Only the local target reads live sheets (ADR 0029); remote loads and schedules
need their own approval, and CI uses fixtures. Each configured tab loads
``<table>_rows`` and ``<table>_captures`` in the ``raw_google_sheets`` dataset
through its own dlt pipeline, so one tab's pending package never blocks another.
"""

from collections.abc import Callable, Mapping
from functools import partial
from time import monotonic

from google.oauth2 import service_account

from analytics_ingestion.google_sheets.client import READONLY_SCOPE, Read, google_reader
from analytics_ingestion.google_sheets.source import capture_sheet
from analytics_ingestion.google_sheets.spec import SOURCE, parse_sheets
from analytics_ingestion.kit.runtime import mapping, read_settings, target

DATASET = "raw_google_sheets"
WALL_SECONDS = 120

# Opens a reader for one spreadsheet ID; tests supply fixtures.
Reader = Callable[[str], Read]


def bounded(
    read: Read,
    *,
    seconds: float = WALL_SECONDS,
    clock: Callable[[], float] = monotonic,
) -> Read:
    """Fail the capture once its reads pass the wall limit, from the first read.

    Each response also has its own 40-second budget in ``google_reader``.
    """
    started: list[float] = []

    def call(params: dict[str, object]) -> object:
        if not started:
            started.append(clock())
        message = f"Google Sheets capture exceeded its {seconds:g}-second wall limit"
        if clock() - started[0] > seconds:
            raise ValueError(message)
        result = read(params)
        if clock() - started[0] > seconds:
            raise ValueError(message)
        return result

    return call


def credentials_from_info(info: dict[str, object]) -> service_account.Credentials:
    """A service-account credential scoped only to spreadsheets.readonly."""
    try:
        return service_account.Credentials.from_service_account_info(  # pyright: ignore[reportUnknownMemberType]  # SDK factory lacks typed kwargs.
            info, scopes=[READONLY_SCOPE]
        )
    except Exception:
        raise ValueError(
            "Google Sheets service-account credential is invalid"
        ) from None


def load_google_sheets(
    env: Mapping[str, str], *, reader: Reader | None = None
) -> dict[str, str]:
    """Manual orchestrated run: capture every configured tab, in settings order.

    Returns each table's capture ID. Tests supply ``reader`` in place of live sheets.
    """
    destination = target(env, SOURCE, local_only=True)
    settings = read_settings(env, SOURCE)
    specs = parse_sheets(settings)
    info = mapping(settings, SOURCE, "service_account")
    if reader is None:
        reader = partial(google_reader, credentials_from_info(info))
    return {
        spec.table: capture_sheet(
            destination.pipeline(f"google_sheets_{spec.table}_local", DATASET),
            bounded(reader(spec.spreadsheet_id)),
            spec,
        )
        for spec in specs
    }
