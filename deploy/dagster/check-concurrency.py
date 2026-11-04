"""Read-only smoke checks for the installed run coordinator and its limits.

Uses the pinned coordinator's admission counter without connecting to storage
or launching runs. The same checks exercise local and remote configuration.
"""

import os
import unittest
from unittest.mock import patch

from dagster import DagsterRun
from dagster._core.instance.ref import InstanceRef
from dagster._core.run_coordinator.queued_run_coordinator import QueuedRunCoordinator
from dagster._utils.tags import TagConcurrencyLimitsCounter


def coordinator() -> QueuedRunCoordinator:
    configured = InstanceRef.from_dir(os.environ["DAGSTER_HOME"]).run_coordinator
    if not isinstance(configured, QueuedRunCoordinator):
        raise AssertionError("Expected QueuedRunCoordinator")
    return configured


class ConcurrencyChecks(unittest.TestCase):
    def test_capacity_comes_from_explicit_target_configuration(self) -> None:
        for limit in (1, 2):
            with self.subTest(limit=limit):
                with patch.dict(
                    os.environ, {"DAGSTER_MAX_CONCURRENT_RUNS": str(limit)}
                ):
                    self.assertEqual(
                        coordinator().get_run_queue_config().max_concurrent_runs, limit
                    )

    def test_shared_resource_blocks_only_its_own_writers(self) -> None:
        limits = coordinator().get_run_queue_config().tag_concurrency_limits or []
        active = DagsterRun(
            job_name="writer", tags={"analytics/exclusive-resource": "dataset-a"}
        )
        counter = TagConcurrencyLimitsCounter(limits, [active])
        self.assertTrue(counter.is_blocked(active))
        self.assertFalse(counter.is_blocked(DagsterRun(job_name="validation")))
        independent = DagsterRun(
            job_name="writer", tags={"analytics/exclusive-resource": "dataset-b"}
        )
        self.assertFalse(counter.is_blocked(independent))
        counter.update_counters_with_launched_item(independent)
        self.assertTrue(counter.is_blocked(independent))

    def test_selected_capacity_is_bounded_and_local_is_serial(self) -> None:
        limit = coordinator().get_run_queue_config().max_concurrent_runs
        self.assertIn(limit, (1, 2))
        if os.environ["ANALYTICS_ENVIRONMENT"] in ("local", "ci"):
            self.assertEqual(limit, 1)


if __name__ == "__main__":
    unittest.main()
