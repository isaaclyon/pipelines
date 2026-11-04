---
name: working-with-dbt-model-governance
description: Use when assessing breaking dbt model changes, introducing model versions and migration windows, enforcing model contracts, or configuring groups and access rules in a single dbt Core project.
user-invocable: false
metadata:
  author: dbt-labs
  adapted-for: analytics-platform
---

# Working with dbt Model Governance

Use contracts, versions, groups, and access rules to protect consumers of models
within this repository's single dbt Core project. Adopt each feature when it solves
a concrete ownership or compatibility problem.

## Before changing a model

1. Read `dbt_project.yml`, the model SQL, its property YAML, and existing group definitions.
2. Check the project's dbt Core and adapter versions before choosing syntax or features.
3. Identify downstream models, exposures, dashboards, applications, and direct table consumers.
4. Decide whether the change affects names, types, grain, constraints, or business meaning.
5. Preserve existing consumers while introducing any required migration path.

Use `ref('model_name')` for models in this project and `source()` for declared data
sources. Use the `running-dbt-commands` skill for local CLI commands.

## Reference guides

| Feature | Purpose | Guide |
|---------|---------|-------|
| Contracts | Enforce output column names and data types | [Model contracts](references/model-contracts.md) |
| Versions | Give consumers time to adopt breaking changes | [Model versions](references/model-versions.md) |
| Groups | Record ownership of related resources | [Groups and access](references/groups-and-access.md) |
| Access rules | Control which models can reference other models | [Groups and access](references/groups-and-access.md) |

## Configuration placement

In model property YAML, nest `group`, `access`, and `contract` under `config:`:

```yaml
models:
  - name: fct_orders
    config:
      group: finance
      access: protected
      contract:
        enforced: true
    columns:
      - name: order_id
        data_type: int
```

Declare the group separately with an owner. A contract must declare **every output
column and its type**; this example assumes the model only returns `order_id`.
Use the `+` prefix for directory-level configuration in `dbt_project.yml`.

## Contracts

Consider a contract when dashboards, applications, or downstream models depend on
a stable output schema. It catches a mismatch between declared columns/types and
the SQL output before the model materializes.

Avoid blanket contracts on rapidly changing internal models, dynamic pivot outputs,
or unsupported materializations. Check adapter support and incremental
`on_schema_change` requirements in the reference guide.

Contracts complement data tests. Warehouse support determines whether a declared
constraint is enforced. On ordinary Snowflake tables, `unique`, `primary_key`, and
`foreign_key` constraints are informational; use data tests to verify them.

## Versions and breaking changes

Use a new version when removing or renaming columns, changing types or meaning, or
otherwise breaking consumers that need time to migrate. An enforced contract is
useful but is **not required** for model versioning.

Additive changes, transparent performance changes, and compatible bug fixes usually
do not need a new version. Inspect consumers before deciding: positional queries or
`select *` can make an apparently additive change incompatible.

### Preserve the migration window

Introducing a version and promoting it to latest are separate deployments:

1. Add the new SQL file and version properties while keeping `latest_version` on
   the old version.
2. Preserve the physical relation name and old columns used by existing consumers.
3. Build the selected versions in development and test their respective behavior.
4. Verify that existing consumers still receive the old shape. Let consumers opt
   into `ref('model_name', v=2)` or the new version's physical relation.
5. Promote `latest_version` only after consumers have migrated.
6. Deprecate and eventually remove the old version after the migration window.

Unpinned `ref()` calls resolve to `latest_version`. External tools query physical
relation names, so adding YAML versions alone does not protect those tools.

If consumers require an unsuffixed relation name, choose one supported mechanism:

- Core 1.12+: confirm support for the beta `latest_version_pointer` feature in the
  installed version and adapter before relying on it.
- Earlier Core: use the documented `create_latest_version_view` post-hook pattern.
- A version-specific `config.alias` is a manual fallback, requiring explicit
  relation-name handling when promoting the new version.

Never give a version and its pointer the same physical name. A pointer follows
`latest_version`; promoting latest changes the schema seen by unsuffixed consumers.
Review existing relation types and dependencies before deploying a hook that
replaces an existing relation.

## Groups and access rules

Groups record domain or team ownership. Define owners and follow the project's
existing directory conventions. Use groups where an ownership boundary helps.

| Access | References within this project |
|--------|--------------------------------|
| `private` | Only models in the same group; a group is required |
| `protected` | Models in any group; this is the default |
| `public` | Models in any group; communicates an intentional public interface |

Use `private` for group-internal implementation details and `protected` for shared
models. Before narrowing access, inspect all callers so existing references keep
working. Consider contracts for stable interfaces based on their consumers.

**Access rules govern dbt references. Warehouse grants govern who can query the
actual tables and views.**

## Validate changes

- Run `dbt parse` with the project's Core executable to validate configuration and references.
- Preview affected selections with `dbt list`.
- For authorized model changes, use selected development builds and data tests.
- For version migrations, check both the new output and the old consumer relation.
- Report any consumer whose migration status cannot be verified before promotion.
