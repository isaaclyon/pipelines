"""Sanitized Loop Returns errors: messages never contain keys, payloads or URLs."""

from __future__ import annotations


class LoopIngestionError(RuntimeError):
    """Base class for Loop Returns ingestion failures."""


class ConfigurationError(LoopIngestionError):
    """Invalid environment, target or credential configuration."""


class SourceError(LoopIngestionError):
    """The API failed, sent an unknown envelope, or sent an incomplete result."""


class ContractError(LoopIngestionError):
    """A returned value violates the header contract."""


class GuardError(LoopIngestionError):
    """A snapshot would erase or remove too much history, so nothing was loaded."""
