"""Client configurations for tests: dbt runs through analytics-dbt, which reads
the file ``ANALYTICS_CLIENT_CONFIG`` names."""

from pathlib import Path

ROOT = Path(__file__).parents[1]
# The package check's client (scripts/check-python.sh): Shopify orders only.
CHECK_CLIENT = ROOT / "tests" / "client.yaml"
# Every source the dbt project gates.
ALL_SOURCES = ROOT / "tests" / "client-all-sources.yaml"
# Sources whose client.yaml entry requires options.
_OPTIONS = {
    "loop_returns": (
        "    shop_id: gid://shopify/Shop/1\n    history_start: 2024-01-01T00:00:00Z\n"
    ),
}


def write_client(
    folder: Path, *sources: str, zone: str = "America/New_York"
) -> dict[str, str]:
    """Write a client.yaml enabling ``sources``; return the variable naming it."""
    path = folder / "client.yaml"
    folder.mkdir(parents=True, exist_ok=True)
    _ = path.write_text(
        f"version: 1\nclient: fixture\nreporting_time_zone: {zone}\n"
        "warehouse: motherduck\nsources:\n"
        + "".join(f"  {name}:\n{_OPTIONS.get(name, '')}" for name in sources)
    )
    return {"ANALYTICS_CLIENT_CONFIG": str(path)}
