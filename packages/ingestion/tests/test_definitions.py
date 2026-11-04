from pathlib import Path

from dagster import Definitions

import analytics_ingestion
from analytics_ingestion.definitions import defs


def test_installed_definitions_load() -> None:
    assert "site-packages" in Path(analytics_ingestion.__file__).parts
    Definitions.validate_loadable(defs())
