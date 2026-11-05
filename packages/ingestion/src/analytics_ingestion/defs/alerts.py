"""Slack messages for failed runs in every code location (ADR 0030).

``alerts.slack`` in client.yaml turns this on: one run failure sensor, running
from the start, watches all code locations and posts each failed run to Slack.
The webhook URL is a secret, so it stays in the mounted file that
``SLACK_WEBHOOK_PATH`` names and is read once, when definitions load. Errors
name that variable and file, never the URL. With Slack off nothing is read.
"""

import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

import dagster as dg
import requests

from analytics_ingestion.client import ClientConfig, client_config

PATH_VARIABLE = "SLACK_WEBHOOK_PATH"
WEBHOOK_PREFIX = "https://hooks.slack.com/"
SENSOR_NAME = "slack_run_failure_alert"
TIMEOUT_SECONDS = 10

# Delivers one message to the webhook URL; raises when delivery fails.
Post = Callable[[str, str], None]


@dataclass(frozen=True)
class RunFailure:
    job: str
    location: str
    run_id: str
    error: str


def message(failure: RunFailure, dagster_url: str | None) -> str:
    """The job, code location, run ID, first error line and a link to the run."""
    first = next(
        (line.strip() for line in failure.error.splitlines() if line.strip()),
        "No error message",
    )
    lines = [
        f"Dagster run failed: {failure.job}",
        f"Code location: {failure.location}",
        f"Run ID: {failure.run_id}",
        f"Error: {first}",
    ]
    if dagster_url is not None:
        lines.append(f"{dagster_url.rstrip('/')}/runs/{failure.run_id}")
    return "\n".join(lines)


def webhook_url(env: Mapping[str, str]) -> str:
    """Read the URL from the file ``SLACK_WEBHOOK_PATH`` names."""
    raw = env.get(PATH_VARIABLE, "").strip()
    if not raw:
        raise ValueError(f"Set {PATH_VARIABLE}; client.yaml turns on Slack alerts")
    path = Path(raw)
    try:
        url = path.read_text().strip()
    except (OSError, UnicodeDecodeError):
        raise ValueError(f"{PATH_VARIABLE}: {path} must be a readable file") from None
    if not url:
        raise ValueError(f"{PATH_VARIABLE}: {path} is empty")
    if not url.startswith(WEBHOOK_PREFIX) or any(char.isspace() for char in url):
        raise ValueError(
            f"{PATH_VARIABLE}: {path} must hold one {WEBHOOK_PREFIX} webhook URL"
        )
    return url


def post_to_slack(url: str, text: str) -> None:
    """Raise on failed delivery; the error never includes the secret URL."""
    try:
        response = requests.post(url, json={"text": text}, timeout=TIMEOUT_SECONDS)
    except requests.RequestException as error:
        raise RuntimeError(
            f"Slack webhook request failed: {type(error).__name__}"
        ) from None
    if not 200 <= response.status_code < 300:
        raise RuntimeError(f"Slack webhook returned HTTP {response.status_code}")


def run_failure(context: dg.RunFailureSensorContext) -> RunFailure:
    """Prefer the first failed step's error; a run can also fail outside steps.

    Dagster wraps an exception from user code; its cause is that exception."""
    run = context.dagster_run
    origin = run.remote_job_origin
    error = context.failure_event.message or ""
    for event in context.get_step_failure_events():
        step_error = event.step_failure_data.error
        if step_error is not None:
            error = (step_error.cause or step_error).message
            break
    return RunFailure(
        job=run.job_name,
        location=origin.location_name if origin is not None else "unknown",
        run_id=run.run_id,
        error=error,
    )


def build_definitions(
    config: ClientConfig, env: Mapping[str, str], post: Post = post_to_slack
) -> dg.Definitions:
    slack = config.alerts.slack
    if slack is None:
        return dg.Definitions()
    url = webhook_url(env)

    # The pinned Dagster decorator's metadata parameter is partially untyped.
    @dg.run_failure_sensor(  # pyright: ignore[reportUnknownMemberType]
        name=SENSOR_NAME,
        monitor_all_code_locations=True,
        default_status=dg.DefaultSensorStatus.RUNNING,
        description="Posts each failed run in any code location to Slack.",
    )
    def slack_run_failure_alert(context: dg.RunFailureSensorContext) -> None:
        post(url, message(run_failure(context), slack.dagster_url))

    return dg.Definitions(sensors=[slack_run_failure_alert])


@dg.definitions
def defs() -> dg.Definitions:
    return build_definitions(client_config(), os.environ)
