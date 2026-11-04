# 0009: Use Python for data pipelines

Date: 2026-10-07

## Status

Accepted.

## Context

TypeScript is a preferred language, but the selected orchestration and
transformation tools have established Python integrations. Ingestion also needs
access to Python connector libraries and data-loading tools.

Making the pipeline runtime primarily TypeScript would require extra process or
API bridges around these tools and add custom infrastructure to a new platform.

## Decision

Use Python for ingestion connectors, Dagster definitions, and pipeline execution
glue. Use SQL and dbt's native configuration for warehouse transformations.
Use Cube's native model formats for semantic definitions.

Use TypeScript for application and API clients, or other components where it
provides a concrete benefit. Keep language boundaries explicit through warehouse
relations, files, typed package interfaces, or service APIs.

Require strict typing for maintained Python code and tests, and strict compiler
settings for TypeScript packages. Confine untyped library interactions to parsed,
typed adapters as described in the
[engineering standards](../../engineering-standards.md).

## Consequences

- Pipeline code can use the selected tools' native integration and testing paths.
- The platform needs fewer custom language bridges to execute and observe jobs.
- Python typing requires deliberate configuration and adapters around dynamic
  third-party libraries.
- SQL, semantic models, and service boundaries need execution and contract tests;
  language type checking cannot establish their correctness alone.
- TypeScript remains useful for consumers without requiring the pipeline runtime
  to be rewritten in it.
