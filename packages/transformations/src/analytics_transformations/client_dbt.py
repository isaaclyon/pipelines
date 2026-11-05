"""Run dbt with the client's settings from ``client.yaml``.

dbt cannot read ``client.yaml``, so ``analytics-dbt`` is a drop-in for ``dbt``:
it loads the client configuration (``ANALYTICS_CLIENT_CONFIG``, see
``analytics_client_config``), then replaces itself with dbt, passing every
argument through, with two generated variables:

- ``ANALYTICS_ENABLED_SOURCES``: the enabled client.yaml source names,
  comma-separated and sorted, possibly empty;
- ``ANALYTICS_REPORTING_TIME_ZONE``: the client's reporting time zone.

The dbt project reads both with no default, so plain dbt fails. A caller that
sets either is refused: the file is the only source of these settings. The exit
code is dbt's, so dagster-dbt can use this as its ``dbt_executable``.

``python -m analytics_transformations.client_dbt COMMAND...`` runs another
command that renders the dbt project, such as ``sqlfluff``, the same way.
"""

import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import NoReturn

from analytics_client_config import ConfigError, load

ENABLED_SOURCES = "ANALYTICS_ENABLED_SOURCES"
REPORTING_TIME_ZONE = "ANALYTICS_REPORTING_TIME_ZONE"
GENERATED = (ENABLED_SOURCES, REPORTING_TIME_ZONE)


def dbt_env(env: Mapping[str, str]) -> dict[str, str]:
    """``env`` plus the variables generated from its client configuration.

    Raises ``ConfigError`` if the configuration does not load, or if ``env``
    already sets a generated variable.
    """
    preset = [name for name in GENERATED if name in env]
    if preset:
        raise ConfigError(
            f"{', '.join(preset)} is generated from client.yaml; unset it and "
            "change client.yaml instead"
        )
    config = load(env)
    return {
        **env,
        ENABLED_SOURCES: ",".join(config.sources.enabled),
        REPORTING_TIME_ZONE: config.reporting_time_zone,
    }


def executable(name: str) -> str:
    """A console script installed beside this Python, such as ``dbt``."""
    return str(Path(sys.executable).with_name(name))


def exec_with_client_env(command: Sequence[str]) -> NoReturn:
    """Replace this process with ``command`` under the client settings."""
    if not command:
        raise SystemExit("Usage: python -m analytics_transformations.client_dbt CMD")
    try:
        env = dbt_env(os.environ)
    except ConfigError as error:
        print(f"analytics-dbt: {error}", file=sys.stderr)
        raise SystemExit(2) from None
    os.execvpe(command[0], list(command), env)


def main() -> NoReturn:
    """The ``analytics-dbt`` console script."""
    exec_with_client_env([executable("dbt"), *sys.argv[1:]])


if __name__ == "__main__":
    exec_with_client_env(sys.argv[1:])
