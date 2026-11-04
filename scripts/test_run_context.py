"""Run containers inherit the resolved code-location configuration."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import cast

from run_context import add_run_contexts, mapping, string


def fixture() -> dict[str, object]:
    return {
        "name": "analytics-test",
        "services": {
            "location": {
                "environment": {
                    "DATASET": "isolated",
                    "APPROVED_FIELDS": '["quoted", "a$b"]',
                    "CREDENTIALS_PATH": "/run/secrets/key",
                    "DAGSTER_CURRENT_IMAGE": "example:release",
                    "DAGSTER_POSTGRES_PASSWORD": "not-in-context",
                },
                "volumes": [
                    {
                        "type": "bind",
                        "source": "/host/key",
                        "target": "/run/secrets/key",
                        "read_only": True,
                    },
                    {"type": "volume", "source": "state", "target": "/state"},
                ],
            }
        },
        "volumes": {"state": {"name": "analytics-test_state"}},
    }


def render_command(config: dict[str, object]) -> tuple[int, dict[str, object]]:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        (root / "resolved.json").write_text(json.dumps(config).replace("$", "$$"))
        docker = root / "docker"
        docker.write_text(
            f"#!{sys.executable}\n"
            "import pathlib, sys\n"
            "root = pathlib.Path(__file__).parent\n"
            "if 'config' in sys.argv:\n"
            "    print((root / 'resolved.json').read_text())\n"
            "else:\n"
            "    if '--project-name' not in sys.argv: sys.exit(99)\n"
            "    project = sys.argv[sys.argv.index('--project-name') + 1]\n"
            "    assert project == 'analytics-test'\n"
            "    path = pathlib.Path(sys.argv[sys.argv.index('-f') + 1])\n"
            "    (root / 'launched.json').write_text(path.read_text())\n"
        )
        docker.chmod(0o700)
        result = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).with_name("run_context.py")),
                "--env-file",
                "fixture.env",
                "--",
                "up",
            ],
            env={**os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"]},
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        launched = root / "launched.json"
        return result.returncode, (
            mapping(cast(object, json.loads(launched.read_text().replace("$$", "$"))))
            if launched.exists()
            else {}
        )


class RunContextTests(unittest.TestCase):
    def test_resolved_settings_and_mounts_reach_runs(self) -> None:
        config = fixture()
        add_run_contexts(config)
        service = mapping(mapping(config["services"])["location"])
        context = cast(
            object,
            json.loads(
                string(mapping(service["environment"])["DAGSTER_CONTAINER_CONTEXT"])
            ),
        )
        self.assertEqual(
            context,
            {
                "docker": {
                    "env_vars": [
                        "DATASET=isolated",
                        'APPROVED_FIELDS=["quoted", "a$b"]',
                        "CREDENTIALS_PATH=/run/secrets/key",
                    ],
                    "container_kwargs": {
                        "labels": {"analytics/project": "analytics-test"},
                        "volumes": [
                            "/host/key:/run/secrets/key:ro",
                            "analytics-test_state:/state:rw",
                        ],
                    },
                }
            },
        )

    def test_compose_round_trip_preserves_literal_dollars(self) -> None:
        code, config = render_command(fixture())
        self.assertEqual(code, 0)
        env = mapping(mapping(mapping(config["services"])["location"])["environment"])
        self.assertEqual(env["APPROVED_FIELDS"], '["quoted", "a$b"]')
        context = mapping(
            cast(object, json.loads(string(env["DAGSTER_CONTAINER_CONTEXT"])))
        )
        self.assertEqual(
            mapping(context["docker"])["env_vars"],
            [
                "DATASET=isolated",
                'APPROVED_FIELDS=["quoted", "a$b"]',
                "CREDENTIALS_PATH=/run/secrets/key",
            ],
        )

    def test_invalid_configuration_never_invokes_up(self) -> None:
        config = fixture()
        config["volumes"] = {}
        code, launched = render_command(config)
        self.assertEqual(code, 2)
        self.assertEqual(launched, {})

    def test_missing_run_mount_fails_before_launch(self) -> None:
        config = fixture()
        config["volumes"] = {}
        with self.assertRaisesRegex(ValueError, "volume"):
            add_run_contexts(config)

    def test_unmounted_credential_path_fails_without_disclosing_values(self) -> None:
        config = fixture()
        config["services"] = {
            "location": {
                "environment": {
                    "DAGSTER_CURRENT_IMAGE": "image",
                    "CREDENTIALS_PATH": "/private/secret",
                },
            }
        }
        with self.assertRaisesRegex(ValueError, "CREDENTIALS_PATH") as error:
            add_run_contexts(config)
        self.assertNotIn("/private/secret", str(error.exception))

    def test_path_cannot_escape_its_mount(self) -> None:
        config = fixture()
        service = mapping(mapping(config["services"])["location"])
        mapping(service["environment"])["STATE_ROOT"] = "/state/../outside"
        with self.assertRaisesRegex(ValueError, "STATE_ROOT"):
            add_run_contexts(config)

    def test_manual_context_is_rejected(self) -> None:
        config: dict[str, object] = {
            "services": {
                "location": {
                    "environment": {
                        "DAGSTER_CURRENT_IMAGE": "image",
                        "DAGSTER_CONTAINER_CONTEXT": "{}",
                    },
                }
            }
        }
        with self.assertRaisesRegex(ValueError, "generated"):
            add_run_contexts(config)


if __name__ == "__main__":
    unittest.main()
