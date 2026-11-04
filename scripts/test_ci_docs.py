"""Offline regression checks for conservative CI routing."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from ci_docs import is_doc, select_dbt_checks, select_lanes


class RoutingTests(unittest.TestCase):
    def test_lane_selection(self) -> None:
        all_packages = ["ingestion", "transformations", "metrics"]
        cases: list[tuple[list[str], tuple[list[str], bool]]] = [
            (["README.md"], ([], False)),
            (["packages/ingestion/tests/test_source.py"], (["ingestion"], False)),
            (
                ["packages/metrics/tests/config.test.cjs", "README.md"],
                (["metrics"], False),
            ),
            (
                ["packages/transformations/dbt/models/orders.sql"],
                (["transformations"], True),
            ),
            (["packages/metrics/cube.js"], (["metrics"], True)),
            # Metrics checks that this generated file matches the Cube model.
            (
                ["packages/transformations/dbt/models/exposures/_cube__exposures.yml"],
                (["transformations", "metrics"], True),
            ),
            # The Cube model reads its column metadata from this snapshot.
            (
                ["packages/transformations/dbt/customer-mart-contracts.json"],
                (["transformations", "metrics"], True),
            ),
            (["packages/ingestion/uv.lock"], (["ingestion"], True)),
            (
                [
                    "packages/ingestion/tests/test_source.py",
                    "packages/metrics/tests/config.test.cjs",
                ],
                (["ingestion", "metrics"], False),
            ),
            (
                ["packages/ingestion/tests/test_source.py", "packages/metrics/cube.js"],
                (["ingestion", "metrics"], True),
            ),
            (["scripts/check-python.sh"], (all_packages, True)),
            (["deploy/compose/ci.yaml"], (all_packages, True)),
            (["unknown"], (all_packages, True)),
            ([], (all_packages, True)),
        ]
        for paths, expected in cases:
            self.assertEqual(select_lanes(paths), expected, paths)

    def test_dbt_check_selection(self) -> None:
        dbt = "packages/transformations/dbt"
        cases: list[tuple[list[str], str]] = [
            # Changes that cannot affect dbt results run no dbt project tests.
            (["README.md"], "none"),
            (["packages/transformations/README.md"], "none"),
            (["packages/ingestion/src/source.py"], "none"),
            (["packages/metrics/cube.js"], "none"),
            (["deploy/dagster/workspace.yaml"], "none"),
            (["deploy/compose/compose.yaml"], "none"),
            (["scripts/run_context.py"], "none"),
            (["ruff.toml"], "none"),
            (["deploy/dagster/dagster.yaml", "docs/adrs/example.md"], "none"),
            # dbt nodes and imported Python are compared with the base.
            ([f"{dbt}/models/marts/fct_orders.sql"], "changed"),
            ([f"{dbt}/macros/money.sql", f"{dbt}/tests/check.sql"], "changed"),
            ([f"{dbt}/seeds/s.csv", f"{dbt}/snapshots/s.sql"], "changed"),
            ([f"{dbt}/analyses/a.sql"], "changed"),
            (["packages/transformations/tests/test_orders.py"], "changed"),
            (["packages/transformations/tests/conftest.py"], "changed"),
            (
                ["packages/transformations/src/analytics_transformations/pii.py"],
                "changed",
            ),
            (
                ["packages/ingestion/src/source.py", f"{dbt}/models/orders.sql"],
                "changed",
            ),
            # Project settings, data files, and the check environment run all.
            ([f"{dbt}/dbt_project.yml"], "all"),
            ([f"{dbt}/profiles.yml"], "all"),
            ([f"{dbt}/packages.yml"], "all"),
            ([f"{dbt}/mart-contracts.json"], "all"),
            (["packages/transformations/tests/fixtures/rows.csv"], "all"),
            (
                [
                    "packages/transformations/src/analytics_transformations/"
                    "source_fields.json"
                ],
                "all",
            ),
            (["packages/transformations/pyproject.toml"], "all"),
            (["packages/transformations/uv.lock"], "all"),
            (["packages/transformations/lineage/uv.lock"], "all"),
            (["packages/transformations/.sqlfluff"], "all"),
            (["deploy/compose/Dockerfile.python"], "all"),
            (["deploy/compose/local.yaml"], "all"),
            (["deploy/compose/ci.yaml"], "all"),
            (["scripts/check-python.sh"], "all"),
            (["scripts/platform"], "all"),
            (["scripts/platform.d/check.sh"], "all"),
            (["scripts/ci_docs.py"], "all"),
            ([".github/workflows/check.yml"], "all"),
            ([f"{dbt}/models/orders.sql", "packages/transformations/uv.lock"], "all"),
            # An unknown diff runs everything.
            ([], "all"),
        ]
        for paths, expected in cases:
            self.assertEqual(select_dbt_checks(paths), expected, paths)

    def test_documentation_paths(self) -> None:
        for path in [
            "README.md",
            "REVIEW.md",
            "docs/adrs/example.md",
            "packages/metrics/README.md",
            ".agents/AGENTS.md",
            ".agents/instructions/scripts/AGENTS.md",
        ]:
            self.assertTrue(is_doc(path), path)

    def test_runtime_and_unknown_paths(self) -> None:
        for path in [
            "packages/metrics/model/schema.yml",
            ".agents/mcp.json",
            ".agents/skills/tool/run.py",
            "docs/tool.py",
            "unknown.md",
            ".github/workflows/check.yml",
        ]:
            self.assertFalse(is_doc(path), path)

    def test_git_routing_and_documentation_failures(self) -> None:
        script = Path(__file__).with_name("ci_docs.py").resolve()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def git(*args: str) -> str:
                return subprocess.run(
                    ["git", *args],
                    cwd=root,
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout.strip()

            def classify(base: str) -> subprocess.CompletedProcess[str]:
                return subprocess.run(
                    [sys.executable, str(script), base, "HEAD"],
                    cwd=root,
                    capture_output=True,
                    text=True,
                )

            git("init")
            git("config", "user.email", "ci@example.invalid")
            git("config", "user.name", "CI test")
            (root / "README.md").write_text("Initial\n")
            git("add", ".")
            git("commit", "-m", "initial")
            base = git("rev-parse", "HEAD")
            (root / "README.md").write_text("Updated\n")
            git("commit", "-am", "docs")
            self.assertEqual(classify(base).stdout.splitlines()[0], "docs_only=true")
            self.assertEqual(
                classify("missing-revision").stdout.splitlines()[0], "docs_only=false"
            )
            self.assertEqual(classify("HEAD").stdout.splitlines()[0], "docs_only=false")
            (root / "runtime.py").write_text("pass\n")
            git("add", ".")
            git("commit", "-m", "mixed")
            self.assertEqual(classify(base).stdout.splitlines()[0], "docs_only=false")
            before_rename = git("rev-parse", "HEAD")
            git("mv", "runtime.py", "AGENTS.md")
            git("commit", "-m", "rename")
            self.assertEqual(
                classify(before_rename).stdout.splitlines()[0], "docs_only=false"
            )
            (root / "README.md").write_text("[broken](missing.md)\n")
            git("commit", "-am", "broken link")
            self.assertNotEqual(classify(base).returncode, 0)
            (root / "README.md").write_text("[external](https://example.com)\n")
            (root / "AGENTS.md").unlink()
            (root / "AGENTS.md").symlink_to("missing.md")
            git("add", ".")
            git("commit", "-m", "broken symlink")
            self.assertNotEqual(classify(base).returncode, 0)
            (root / "AGENTS.md").unlink()
            (root / "AGENTS.md").write_text("Guide\n")
            git("add", ".")
            git("commit", "-m", "fix symlink")
            self.assertEqual(classify(base).returncode, 0)
            # is_doc controls lane selection, not link coverage. Non-README
            # runbooks outside docs/ must still reject broken local links.
            runbook = root / "deploy/dagster/runbook.md"
            runbook.parent.mkdir(parents=True)
            runbook.write_text("[broken](missing.md)\n")
            git("add", ".")
            git("commit", "-m", "broken runbook link")
            self.assertNotEqual(classify(base).returncode, 0)
            runbook.write_text("[root](../../README.md)\n")
            git("commit", "-am", "fix runbook link")
            self.assertEqual(classify(base).returncode, 0)
            # `.agents/` is the only shared agent directory (AGENTS.md).
            (root / ".agent").mkdir()
            (root / ".agent" / "notes.md").write_text("Notes\n")
            git("add", ".")
            git("commit", "-m", "singular agent directory")
            result = classify(base)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(".agent/", result.stderr)


if __name__ == "__main__":
    unittest.main()
