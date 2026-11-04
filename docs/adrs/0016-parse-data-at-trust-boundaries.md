# 0016: Parse data at trust boundaries

Date: 2026-10-07

## Status

Accepted.

## Context

Configuration, source responses, files, and persisted state enter the platform
with shapes and meanings the application cannot assume. Repeated boolean
validation checks leave callers holding the same uncertain values and make
those assumptions spread through pipeline code.

Strict type annotations cannot establish that an external identifier, cursor,
timestamp, or amount is valid at runtime. The platform also needs to preserve
source data as schemas evolve.

## Decision

Parse external inputs at their trust boundaries into usable typed values or
specific errors. Successful parsing establishes the guarantees that downstream
code needs, so callers can operate on the result without repeating those checks.
Represent meaningful states with dedicated types and immutable values where
practical, following the [engineering standards](../../engineering-standards.md).

Keep untyped third-party values within small boundary adapters. Do not treat a
cast or a declared dictionary shape as evidence that runtime data was parsed.
Report failures with useful source and field context while excluding secrets
and sensitive payloads.

Apply parsing according to ownership: parse configuration and connector control
fields before use, preserve source-shaped raw data for replay, and establish
business-facing field types and keys in staging. Unknown source fields must not
silently disappear or cause an otherwise usable raw record to be rejected.

Test parsers against accepted forms and meaningful failure cases, including
precision and time-zone boundaries where relevant. Keep source-contract errors
visible and distinct from transport or credential failures.

## Consequences

- Application code receives values whose usable shape and meaning have already
  been established, improving both type checking and failure diagnosis.
- Assumptions are concentrated at boundaries and can be tested directly.
- Parsers and source contracts need maintenance as upstream formats evolve.
- Preserving raw data avoids equating a typed business projection with complete
  source history; retention and replay guarantees still need source-level design.
- Typed parsing complements transformation tests and reconciliation. It does not
  prove that a source delivered every expected record.
