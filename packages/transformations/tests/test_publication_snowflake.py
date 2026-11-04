"""Opt-in disposable development acceptance; ordinary checks never connect."""

import os
import shutil
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from uuid import uuid4

import pytest
import snowflake.connector

from analytics_transformations.mart_contracts import identifier
from analytics_transformations.publication import publish_snowflake_marts

ROOT = Path(__file__).parents[1]


@pytest.mark.skipif(
    os.getenv("PUBLICATION_LIVE_ACCEPTANCE") != "development",
    reason="explicit development acceptance only",
)
def test_development_publication_and_access(tmp_path: Path) -> None:
    """Roles are provisioned separately; test owns only its generated schemas."""
    assert os.environ["ANALYTICS_ENVIRONMENT"] == "development"
    assert os.environ["SNOWFLAKE_DATABASE"]
    assert os.environ["SNOWFLAKE_WAREHOUSE"]
    reader_role = identifier(os.environ["PUBLICATION_READER_ROLE"].lower())
    diagnostic_role = identifier(os.environ["PUBLICATION_DIAGNOSTIC_ROLE"].lower())
    namespace = "publication_accept_" + uuid4().hex[:10]
    project = tmp_path / "project"
    _ = shutil.copytree(ROOT / "tests/fixtures/publication", project)
    _ = shutil.copytree(ROOT / "dbt/macros", project / "macros")
    database = os.environ["SNOWFLAKE_DATABASE"]
    # The connector's dynamic API is confined here; rows enter as objects.
    with snowflake.connector.connect(  # pyright: ignore[reportUnknownMemberType]
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=database,
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        login_timeout=20,
        network_timeout=60,
        session_parameters={"TIMEZONE": "UTC", "STATEMENT_TIMEOUT_IN_SECONDS": 60},
    ) as connection:

        def sql(statement: str, role: str | None = None) -> list[tuple[object, ...]]:
            with connection.cursor() as cursor:
                cursor.execute(f"use role {role or os.environ['SNOWFLAKE_ROLE']}")
                cursor.execute("use secondary roles none")
                cursor.execute(statement)
                return [tuple(row) for row in cursor.fetchall()]  # pyright: ignore[reportAny] # Connector rows checked by assertions.

        contender_checked = False

        def publish(amount: int) -> str:
            def runner(
                command: Sequence[str], env: Mapping[str, str], timeout: int
            ) -> None:
                nonlocal contender_checked
                if command[1] == "build":
                    if not contender_checked:
                        lease_sql = (
                            "select publication_id from "
                            f"{namespace}_publication_lock.lease"
                        )
                        lease = sql(lease_sql)
                        with pytest.raises(RuntimeError):
                            _ = publish_snowflake_marts(
                                environment="development",
                                namespace=namespace,
                                project_dir=project,
                                profiles_dir=ROOT / "dbt",
                                reader_role=reader_role,
                            )
                        assert sql(lease_sql) == lease
                        contender_checked = True
                    candidate = env["DBT_NAMESPACE"] + "_marts"
                    # The writer sees the real candidate; the restricted consumer
                    # cannot enter it before validation, including cloned releases.
                    _ = sql(f"use schema {candidate}")
                    with pytest.raises(snowflake.connector.errors.ProgrammingError):
                        sql(f"use schema {candidate}", reader_role)
                _ = subprocess.run(
                    [*command, "--vars", f"{{amount: {amount}}}"],
                    env=env,
                    check=True,
                    timeout=timeout,
                    stdout=subprocess.DEVNULL,
                )

            return publish_snowflake_marts(
                environment="development",
                namespace=namespace,
                project_dir=project,
                profiles_dir=ROOT / "dbt",
                reader_role=reader_role,
                runner=runner,
            ).publication_id

        try:
            first = publish(100)
            assert sql(
                f"select publication_id from {namespace}_marts._publication",
                reader_role,
            ) == [(first,)]
            assert sql(
                f"select observed_amount from {namespace}_marts.fct_observations",
                reader_role,
            ) == [(100,)]
            # Diagnostic access remains a separate explicit privilege set.
            for layer in ("raw", "staging", "intermediate"):
                schema = namespace + "_" + layer
                _ = sql(f"create transient schema {schema}")
                _ = sql(f"create table {schema}.private_evidence as select 1 as id")
                _ = sql(f"grant usage on schema {schema} to role {diagnostic_role}")
                _ = sql(
                    f"grant select on table {schema}.private_evidence "
                    f"to role {diagnostic_role}"
                )
                with pytest.raises(snowflake.connector.errors.ProgrammingError):
                    sql(f"select * from {schema}.private_evidence", reader_role)
                assert sql(
                    f"select * from {schema}.private_evidence", diagnostic_role
                ) == [(1,)]
            with pytest.raises(subprocess.CalledProcessError):
                publish(-1)
            assert sql(
                f"select publication_id from {namespace}_marts._publication",
                reader_role,
            ) == [(first,)]
            second = publish(115)
            assert second != first
            assert sql(
                f"select observed_amount from {namespace}_marts.fct_observations",
                reader_role,
            ) == [(115,)]
            assert sql(
                f"select publication_id from {namespace}_marts._publication",
                reader_role,
            ) == [(second,)]
        finally:
            # Test-only cleanup of exact generated prefix, including crash locks.
            # Production recovery must first terminate the abandoned publisher.
            schemas = sql(
                f"select schema_name from {database}.information_schema.schemata "
                f"where startswith(schema_name, '{namespace.upper()}_')"
            )
            for row in schemas:
                assert isinstance(row[0], str) and row[0].lower().startswith(
                    namespace + "_"
                )
                _ = sql(f'drop schema "{row[0]}" cascade')
