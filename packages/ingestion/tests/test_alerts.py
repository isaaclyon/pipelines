"""Slack alerts on failed runs: registration, the webhook file and the message."""

import json
import threading
from collections.abc import Generator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import dagster as dg
import pytest

# The remote origin types are internal; a run from another code location needs one.
from dagster._core.remote_origin import (  # pyright: ignore[reportPrivateUsage]
    GrpcServerCodeLocationOrigin,
    RemoteJobOrigin,
    RemoteRepositoryOrigin,
)

from analytics_ingestion.client import ClientConfig
from analytics_ingestion.defs.alerts import (
    SENSOR_NAME,
    RunFailure,
    build_definitions,
    message,
    post_to_slack,
)
from tests.client_files import client

URL = "https://hooks.slack.com/services/T000/B000/fixture-secret"
SLACK = """\
alerts:
  slack:
    dagster_url: https://dagster.example.com/
"""


def webhook(tmp_path: Path, content: str = URL) -> dict[str, str]:
    path = tmp_path / "slack-webhook"
    _ = path.write_text(content + "\n")
    return {"SLACK_WEBHOOK_PATH": str(path)}


@pytest.fixture
def config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> ClientConfig:
    return client(monkeypatch, tmp_path, SLACK)


def sensor_of(definitions: dg.Definitions) -> dg.SensorDefinition:
    [sensor] = list(definitions.sensors or [])
    return sensor


def test_message_names_the_run_and_links_it() -> None:
    failure = RunFailure(
        "shopify_orders_dlt_load",
        "ingestion",
        "abc-123",
        "\nFailure: Set ANALYTICS_ENVIRONMENT\n  File ...",
    )
    assert message(failure, "https://dagster.example.com/") == (
        "Dagster run failed: shopify_orders_dlt_load\n"
        "Code location: ingestion\n"
        "Run ID: abc-123\n"
        "Error: Failure: Set ANALYTICS_ENVIRONMENT\n"
        "https://dagster.example.com/runs/abc-123"
    )
    assert message(failure, None).endswith("Error: Failure: Set ANALYTICS_ENVIRONMENT")


def test_slack_off_registers_and_reads_nothing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    off = client(monkeypatch, tmp_path)
    missing = {"SLACK_WEBHOOK_PATH": str(tmp_path / "missing")}
    assert list(build_definitions(off, missing).sensors or []) == []


def test_slack_on_registers_one_running_sensor(
    config: ClientConfig, tmp_path: Path
) -> None:
    sensor = sensor_of(build_definitions(config, webhook(tmp_path)))
    assert sensor.name == SENSOR_NAME
    assert sensor.default_status == dg.DefaultSensorStatus.RUNNING


@pytest.mark.parametrize(
    ("content", "problem"),
    [
        (None, "Set SLACK_WEBHOOK_PATH"),
        ("", "is empty"),
        ("https://example.com/hook/fixture-secret", "hooks.slack.com"),
        (URL + " " + URL, "hooks.slack.com"),
    ],
)
def test_slack_on_needs_a_webhook_file(
    config: ClientConfig, tmp_path: Path, content: str | None, problem: str
) -> None:
    env = {} if content is None else webhook(tmp_path, content)
    with pytest.raises(ValueError, match=problem) as error:
        _ = build_definitions(config, env)
    assert "SLACK_WEBHOOK_PATH" in str(error.value)
    assert "fixture-secret" not in str(error.value)


def test_unreadable_webhook_file_is_named(config: ClientConfig, tmp_path: Path) -> None:
    env = {"SLACK_WEBHOOK_PATH": str(tmp_path / "missing")}
    with pytest.raises(ValueError, match="SLACK_WEBHOOK_PATH: .*missing"):
        _ = build_definitions(config, env)


# The pinned Dagster decorators expose partially untyped signatures.
@dg.op  # pyright: ignore[reportUnknownMemberType]
def broken() -> None:
    raise ValueError("Snowflake refused the load\nsecond line")


@dg.job  # pyright: ignore[reportUnknownMemberType]
def failing_job() -> None:
    broken()


def test_sensor_posts_the_first_failed_step_error(
    config: ClientConfig, tmp_path: Path
) -> None:
    posted: list[tuple[str, str]] = []
    sensor = sensor_of(
        build_definitions(
            config, webhook(tmp_path), lambda url, text: posted.append((url, text))
        )
    )
    with dg.instance_for_test() as instance:
        result = failing_job.execute_in_process(instance=instance, raise_on_error=False)
        context = dg.build_run_status_sensor_context(
            sensor_name=SENSOR_NAME,
            dagster_instance=instance,
            dagster_run=result.dagster_run,
            dagster_event=result.get_run_failure_event(),
        ).for_run_failure()
        _ = sensor(context)
    [(url, text)] = posted
    assert url == URL
    run_id = result.run_id
    assert text.splitlines() == [
        "Dagster run failed: failing_job",
        "Code location: unknown",
        f"Run ID: {run_id}",
        "Error: ValueError: Snowflake refused the load",
        f"https://dagster.example.com/runs/{run_id}",
    ]


def test_sensor_alerts_on_runs_in_other_code_locations(
    config: ClientConfig, tmp_path: Path
) -> None:
    posted: list[str] = []
    sensor = sensor_of(
        build_definitions(
            config, webhook(tmp_path), lambda _, text: posted.append(text)
        )
    )
    location = GrpcServerCodeLocationOrigin(
        host="transformations", port=4000, location_name="transformations"
    )
    origin = RemoteJobOrigin(
        RemoteRepositoryOrigin(location, "__repository__"), "dbt_build"
    )
    with dg.instance_for_test() as instance:
        first = dg.build_sensor_context(instance=instance)
        _ = sensor.evaluate_tick(first)  # pyright: ignore[reportAny]
        run = instance.add_run(
            dg.DagsterRun(
                job_name="dbt_build", run_id="dbt-run", remote_job_origin=origin
            )
        )
        instance.report_run_failed(run)
        later = dg.build_sensor_context(instance=instance, cursor=first.cursor)
        _ = sensor.evaluate_tick(later)  # pyright: ignore[reportAny]
    [text] = posted
    assert "Dagster run failed: dbt_build\nCode location: transformations\n" in text
    assert "Run ID: dbt-run" in text


class Webhook(BaseHTTPRequestHandler):
    status = 200
    received: list[object] = []

    def do_POST(self) -> None:
        length = int(self.headers["Content-Length"])
        body: object = json.loads(self.rfile.read(length))  # pyright: ignore[reportAny]
        Webhook.received.append(body)
        self.send_response(Webhook.status)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        pass


@contextmanager
def local_webhook(status: int) -> Generator[str]:
    Webhook.status, Webhook.received = status, []
    server = HTTPServer(("127.0.0.1", 0), Webhook)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/services/fixture-secret"
    finally:
        server.shutdown()
        server.server_close()


def test_post_sends_the_message_as_slack_json() -> None:
    with local_webhook(200) as url:
        post_to_slack(url, "Dagster run failed: x")
    assert Webhook.received == [{"text": "Dagster run failed: x"}]


def test_post_raises_on_a_rejected_message_without_the_url() -> None:
    with (
        local_webhook(404) as url,
        pytest.raises(RuntimeError, match="HTTP 404") as error,
    ):
        post_to_slack(url, "Dagster run failed: x")
    assert "fixture-secret" not in str(error.value)


def test_post_raises_when_slack_is_unreachable_without_the_url() -> None:
    with local_webhook(200) as url:
        pass
    with pytest.raises(RuntimeError, match="request failed") as error:
        post_to_slack(url, "Dagster run failed: x")
    assert "fixture-secret" not in str(error.value)
    assert error.value.__cause__ is None and error.value.__suppress_context__
