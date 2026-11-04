from pathlib import Path

from dagster import Definitions

import analytics_transformations
from analytics_transformations.definitions import defs


def test_installed_definitions_load() -> None:
    assert "site-packages" in Path(analytics_transformations.__file__).parts
    Definitions.validate_loadable(defs())
