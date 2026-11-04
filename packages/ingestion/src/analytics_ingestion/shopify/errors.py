"""Sanitized ingestion errors: messages never contain payloads, tokens or URLs."""

from __future__ import annotations


class ShopifyIngestionError(RuntimeError):
    """Base class for Shopify ingestion failures."""


class SourceError(ShopifyIngestionError):
    """The source returned an error, malformed control data or an incomplete result."""


class RetryableSourceError(SourceError):
    """A bounded read can be attempted again within the shared budget."""

    def __init__(self, message: str, wait: float = 0.0) -> None:
        super().__init__(message)
        self.wait = wait


class AccessDenied(SourceError):
    """Shopify denied access to a selected field (scope or protected data)."""


class QueryRejected(SourceError):
    """Shopify rejected a selected field as invalid for the pinned API version."""


class ContractError(ShopifyIngestionError):
    """A value violates the committed raw field contract."""


class BudgetExhausted(ShopifyIngestionError):
    """A client's request, byte or active-time limit stopped a runaway read."""


class ConfigurationError(ShopifyIngestionError):
    """Invalid environment, target or credential configuration."""
