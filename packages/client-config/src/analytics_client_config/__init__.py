"""The client configuration file: one per client, checked in, no secrets.

ADR 0030 gives each client a repository with one ``client.yaml``. Every toolkit
service reads it from ``ANALYTICS_CLIENT_CONFIG`` (default
``/etc/analytics/client.yaml``) and stops at startup if it does not parse.
Secrets never belong here; they stay in the environment file (ADR 0032).

``parse`` and ``load`` return a typed ``ClientConfig`` or raise ``ConfigError``
naming the file and the field. Unknown keys are errors, so a misspelled setting
never falls back to a default silently.
"""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml

CONFIG_VARIABLE = "ANALYTICS_CLIENT_CONFIG"
DEFAULT_PATH = Path("/etc/analytics/client.yaml")
VERSION = 1

Warehouse = Literal["motherduck", "snowflake"]
WAREHOUSES: tuple[Warehouse, ...] = ("motherduck", "snowflake")

_SLUG = re.compile(r"[a-z][a-z0-9-]{0,39}")
_CRON_FIELD = re.compile(r"[0-9A-Za-z*/,\-?]+")
_SHOP_ID = re.compile(r"gid://shopify/Shop/[0-9]+")
_URL = re.compile(r"https?://[^\s]+")


class ConfigError(ValueError):
    """The client configuration is missing or invalid."""


@dataclass(frozen=True)
class Schedule:
    """Turns on a source's schedules. ``refresh`` overrides the toolkit's
    refresh cron; None keeps the default."""

    start: datetime
    refresh: str | None


@dataclass(frozen=True)
class ShopifyOrders:
    schedule: Schedule | None
    refresh_workers: int


@dataclass(frozen=True)
class ShopifyCustomers:
    schedule: Schedule | None
    collect_tax_id: bool
    # Exact (namespace, key) pairs; empty collects no metafield values.
    approved_metafields: frozenset[tuple[str, str]]


@dataclass(frozen=True)
class LoopReturns:
    shop_id: str
    history_start: datetime
    max_removed_rows: int


@dataclass(frozen=True)
class Sources:
    """Listed sources are enabled. Sources without options hold no value."""

    shopify_orders: ShopifyOrders | None
    shopify_customers: ShopifyCustomers | None
    loop_returns: LoopReturns | None
    goodday: bool
    google_sheets: bool
    impact_radius: bool

    @property
    def enabled(self) -> tuple[str, ...]:
        """Names of the enabled sources, sorted."""
        values: dict[str, object] = {
            "goodday": self.goodday,
            "google_sheets": self.google_sheets,
            "impact_radius": self.impact_radius,
            "loop_returns": self.loop_returns,
            "shopify_customers": self.shopify_customers,
            "shopify_orders": self.shopify_orders,
        }
        return tuple(
            name for name, value in values.items() if value not in (None, False)
        )


@dataclass(frozen=True)
class SlackAlerts:
    """Failed runs post to the Slack webhook whose URL is in the file named by
    ``SLACK_WEBHOOK_PATH``."""

    # Links each message to the run when set.
    dagster_url: str | None


@dataclass(frozen=True)
class Alerts:
    slack: SlackAlerts | None


@dataclass(frozen=True)
class ClientConfig:
    client: str
    reporting_time_zone: str
    warehouse: Warehouse
    sources: Sources
    alerts: Alerts

    def enabled(self, source: str) -> bool:
        return source in self.sources.enabled


SOURCE_NAMES = (
    "goodday",
    "google_sheets",
    "impact_radius",
    "loop_returns",
    "shopify_customers",
    "shopify_orders",
)


class _Reader:
    """Reads one mapping and remembers its path for error messages."""

    def __init__(self, origin: str, path: str, value: object) -> None:
        self.origin = origin
        self.path = path
        if value is None:
            value = {}
        if not isinstance(value, dict):
            raise self.error("must be a mapping")
        self.values = cast(dict[object, object], value)
        for key in self.values:
            if not isinstance(key, str):
                raise self.error("keys must be text")

    def error(self, message: str, key: str | None = None) -> ConfigError:
        where = ".".join(part for part in (self.path, key) if part) or "file"
        return ConfigError(f"{self.origin}: {where} {message}")

    def only(self, *allowed: str) -> None:
        for key in self.values:
            if key not in allowed:
                raise self.error(
                    f"is not a setting; expected one of {', '.join(allowed)}",
                    cast(str, key),
                )

    def has(self, key: str) -> bool:
        return key in self.values

    def raw(self, key: str) -> object:
        return self.values.get(key)

    def child(self, key: str) -> "_Reader":
        return _Reader(
            self.origin, ".".join(filter(None, (self.path, key))), self.raw(key)
        )

    def text(self, key: str, pattern: re.Pattern[str] | None = None) -> str:
        value = self.raw(key)
        if not isinstance(value, str) or not value.strip():
            raise self.error("must be nonempty text", key)
        if pattern is not None and not pattern.fullmatch(value):
            raise self.error(f"must match {pattern.pattern}", key)
        return value

    def optional_text(self, key: str, pattern: re.Pattern[str]) -> str | None:
        return self.text(key, pattern) if self.has(key) else None

    def integer(self, key: str, default: int, minimum: int) -> int:
        value = self.raw(key) if self.has(key) else default
        if type(value) is not int or value < minimum:
            raise self.error(f"must be a whole number of at least {minimum}", key)
        return value

    def boolean(self, key: str, default: bool) -> bool:
        value = self.raw(key) if self.has(key) else default
        if not isinstance(value, bool):
            raise self.error("must be true or false", key)
        return value

    def instant(self, key: str) -> datetime:
        value = self.raw(key)
        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value)
            except ValueError:
                raise self.error("must be an ISO 8601 instant", key) from None
        if not isinstance(value, datetime) or value.tzinfo is None or value.microsecond:
            raise self.error(
                "must be an instant with a UTC offset and whole seconds, "
                "such as 2024-01-01T00:00:00Z",
                key,
            )
        return value

    def cron(self, key: str) -> str | None:
        if not self.has(key):
            return None
        value = self.text(key)
        fields = value.split()
        if len(fields) != 5 or not all(_CRON_FIELD.fullmatch(f) for f in fields):
            raise self.error("must be a five-field cron expression", key)
        return value


def _schedule(source: _Reader) -> Schedule | None:
    if not source.has("schedule"):
        return None
    reader = source.child("schedule")
    reader.only("start", "refresh")
    return Schedule(reader.instant("start"), reader.cron("refresh"))


def _metafields(reader: _Reader) -> frozenset[tuple[str, str]]:
    value: object = reader.raw("approved_metafields") or []
    if not isinstance(value, list):
        raise reader.error("must be a list", "approved_metafields")
    pairs: set[tuple[str, str]] = set()
    for index, item in enumerate(cast(list[object], value)):
        path = ".".join(filter(None, (reader.path, "approved_metafields")))
        entry = _Reader(reader.origin, f"{path}[{index}]", item)
        entry.only("namespace", "key")
        namespace, key = entry.text("namespace"), entry.text("key")
        if "*" in namespace or "*" in key:
            raise entry.error("must name one exact metafield, without wildcards")
        pairs.add((namespace, key))
    return frozenset(pairs)


def _sources(reader: _Reader) -> Sources:
    reader.only(*SOURCE_NAMES)

    def options(name: str) -> _Reader | None:
        return reader.child(name) if reader.has(name) else None

    orders = None
    if (source := options("shopify_orders")) is not None:
        source.only("schedule", "refresh_workers")
        orders = ShopifyOrders(
            _schedule(source), source.integer("refresh_workers", 4, minimum=1)
        )
    customers = None
    if (source := options("shopify_customers")) is not None:
        source.only("schedule", "collect_tax_id", "approved_metafields")
        customers = ShopifyCustomers(
            _schedule(source),
            source.boolean("collect_tax_id", False),
            _metafields(source),
        )
    loop = None
    if (source := options("loop_returns")) is not None:
        source.only("shop_id", "history_start", "max_removed_rows")
        loop = LoopReturns(
            source.text("shop_id", _SHOP_ID),
            source.instant("history_start"),
            source.integer("max_removed_rows", 0, minimum=0),
        )
    kit: dict[str, bool] = {}
    for name in ("goodday", "google_sheets", "impact_radius"):
        if (source := options(name)) is not None:
            source.only()
        kit[name] = source is not None
    return Sources(
        orders,
        customers,
        loop,
        goodday=kit["goodday"],
        google_sheets=kit["google_sheets"],
        impact_radius=kit["impact_radius"],
    )


def _alerts(reader: _Reader) -> Alerts:
    reader.only("slack")
    if not reader.has("slack"):
        return Alerts(None)
    slack = reader.child("slack")
    slack.only("dagster_url")
    return Alerts(SlackAlerts(slack.optional_text("dagster_url", _URL)))


def parse(text: str, origin: str = "client.yaml") -> ClientConfig:
    """Parse configuration text; ``origin`` names it in errors."""
    try:
        document = cast(object, yaml.safe_load(text))
    except yaml.YAMLError:
        raise ConfigError(f"{origin}: file is not valid YAML") from None
    root = _Reader(origin, "", document)
    root.only(
        "version", "client", "reporting_time_zone", "warehouse", "sources", "alerts"
    )
    if root.raw("version") != VERSION:
        raise root.error(f"must be {VERSION}", "version")
    zone = root.text("reporting_time_zone")
    try:
        _ = ZoneInfo(zone)
    except (ZoneInfoNotFoundError, ValueError):
        raise root.error("must be an IANA time zone", "reporting_time_zone") from None
    warehouse = root.text("warehouse")
    if warehouse not in WAREHOUSES:
        raise root.error(f"must be one of {', '.join(WAREHOUSES)}", "warehouse")
    return ClientConfig(
        client=root.text("client", _SLUG),
        reporting_time_zone=zone,
        warehouse=warehouse,
        sources=_sources(root.child("sources")),
        alerts=_alerts(root.child("alerts")),
    )


def config_path(env: Mapping[str, str]) -> Path:
    path = Path(env.get(CONFIG_VARIABLE, "").strip() or DEFAULT_PATH)
    if not path.is_absolute():
        raise ConfigError(f"{CONFIG_VARIABLE} must be an absolute path")
    return path


def load(env: Mapping[str, str]) -> ClientConfig:
    """Read the file ``ANALYTICS_CLIENT_CONFIG`` names, or the default path."""
    path = config_path(env)
    try:
        text = path.read_text()
    except OSError:
        raise ConfigError(
            f"{path}: client configuration is unreadable; mount client.yaml "
            f"there or set {CONFIG_VARIABLE}"
        ) from None
    return parse(text, str(path))


__all__ = [
    "CONFIG_VARIABLE",
    "DEFAULT_PATH",
    "SOURCE_NAMES",
    "WAREHOUSES",
    "Alerts",
    "ClientConfig",
    "ConfigError",
    "LoopReturns",
    "Schedule",
    "ShopifyCustomers",
    "ShopifyOrders",
    "SlackAlerts",
    "Sources",
    "Warehouse",
    "config_path",
    "load",
    "parse",
]
