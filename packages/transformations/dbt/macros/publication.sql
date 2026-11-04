{% macro publication_context(namespace, candidate_namespace, publication_id, reader_role, restricted_reader_role=none) %}
    {% if target.type != 'snowflake' or target.name != 'snowflake-development' %}
        {{ exceptions.raise_compiler_error('Publication requires explicit development Snowflake') }}
    {% endif %}
    {% for value in [namespace, candidate_namespace, publication_id, reader_role] + ([restricted_reader_role] if restricted_reader_role else []) %}
        {% if not modules.re.fullmatch('[A-Za-z0-9_]+', value) %}
            {{ exceptions.raise_compiler_error('Invalid publication identifier') }}
        {% endif %}
    {% endfor %}
    {% if candidate_namespace != namespace ~ '_candidate_' ~ publication_id[:12] or env_var('DBT_NAMESPACE') != candidate_namespace %}
        {{ exceptions.raise_compiler_error('Candidate does not match publication identity') }}
    {% endif %}
    {% if restricted_reader_role and restricted_reader_role | upper == reader_role | upper %}
        {{ exceptions.raise_compiler_error('The restricted reader role must differ from the reader role') }}
    {% endif %}
    {{ return({'database': adapter.quote(target.database), 'published': adapter.quote(namespace | upper ~ '_MARTS'), 'candidate': adapter.quote(candidate_namespace | upper ~ '_MARTS'), 'lock': adapter.quote(namespace | upper ~ '_PUBLICATION_LOCK'), 'reader': adapter.quote(reader_role | upper), 'restricted_reader': adapter.quote(restricted_reader_role | upper) if restricted_reader_role else none}) }}
{% endmacro %}

{% macro prepare_mart_candidate(namespace, candidate_namespace, publication_id, reader_role, restricted_reader_role=none) %}
    {% set c = publication_context(namespace, candidate_namespace, publication_id, reader_role, restricted_reader_role) %}
    {% if execute %}
        {% set future_grants = run_query('show future grants in database ' ~ c.database) %}
        {% if future_grants.rows | length > 0 %}
            {{ exceptions.raise_compiler_error('Publication database must have no future grants; use explicit grants') }}
        {% endif %}
        {# CREATE without IF NOT EXISTS is the atomic cross-host publisher guard. #}
        {% do run_query('create transient schema ' ~ c.database ~ '.' ~ c.lock) %}
        {% do run_query('create table ' ~ c.database ~ '.' ~ c.lock ~ '.lease as select ' ~ "'" ~ publication_id ~ "' as publication_id") %}
        {% set existing = run_query('select count(*) from ' ~ c.database ~ ".information_schema.schemata where schema_name = '" ~ namespace | upper ~ "_MARTS'") %}
        {% if existing.rows[0][0] > 0 %}
            {% set old_tables = run_query('select table_name from ' ~ c.database ~ ".information_schema.tables where table_schema = '" ~ namespace | upper ~ "_MARTS'") %}
            {% set old_names = [] %}
            {% for row in old_tables.rows %}{% do old_names.append(row[0]) %}{% endfor %}
            {% if old_names and '_PUBLICATION' not in old_names %}
                {{ exceptions.raise_compiler_error('Existing consumer relations require a publication marker; migrate explicitly') }}
            {% endif %}
            {# Schema grants are not copied. Tables may retain grants but readers
               have no schema USAGE until after validation immediately before swap. #}
            {% do run_query('create transient schema ' ~ c.database ~ '.' ~ c.candidate ~ ' clone ' ~ c.database ~ '.' ~ c.published) %}
        {% else %}
            {% do run_query('create transient schema ' ~ c.database ~ '.' ~ c.candidate) %}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro assert_publication_lease(c, publication_id) %}
    {% set lease = run_query('select publication_id from ' ~ c.database ~ '.' ~ c.lock ~ '.lease') %}
    {% if lease.rows | length != 1 or lease.rows[0][0] != publication_id %}
        {{ exceptions.raise_compiler_error('Publication lease mismatch; operator recovery required') }}
    {% endif %}
{% endmacro %}

{% macro publish_mart_candidate(namespace, candidate_namespace, publication_id, reader_role, restricted_reader_role=none, relations=none, contracts_json=none, access_tightenings=none) %}
    {% set c = publication_context(namespace, candidate_namespace, publication_id, reader_role, restricted_reader_role) %}
    {% if execute %}
        {% do assert_publication_lease(c, publication_id) %}
        {% set declared = fromjson(contracts_json) %}
        {% set names = declared | map(attribute='relation') | list %}
        {% if names | sort != relations | sort or not relations %}
            {{ exceptions.raise_compiler_error('Publication relations must match validated contracts') }}
        {% endif %}
        {% set tables = run_query('select table_name, table_type from ' ~ c.database ~ ".information_schema.tables where table_schema = '" ~ candidate_namespace | upper ~ "_MARTS'") %}
        {% set present = [] %}
        {% for row in tables.rows %}
            {% if row[0] != '_PUBLICATION' %}
                {% if row[1] != 'BASE TABLE' %}{{ exceptions.raise_compiler_error('Marts must be physical tables') }}{% endif %}
                {% do present.append(row[0] | lower) %}
            {% endif %}
        {% endfor %}
        {% if present | sort != relations | sort %}
            {{ exceptions.raise_compiler_error('Candidate contains missing or undeclared relations') }}
        {% endif %}
        {% set markers = run_query('select count(*) from ' ~ c.database ~ ".information_schema.tables where table_schema = '" ~ candidate_namespace | upper ~ "_MARTS' and table_name = '_PUBLICATION'") %}
        {% if markers.rows[0][0] > 0 %}
            {% set old_rows = run_query('select contracts from ' ~ c.database ~ '.' ~ c.candidate ~ '._publication') %}
            {% if old_rows.rows | length != 1 %}{{ exceptions.raise_compiler_error('Invalid prior publication marker') }}{% endif %}
            {% for old in fromjson(old_rows.rows[0][0]) %}
                {% set matches = declared | selectattr('relation', 'equalto', old.relation) | list %}
                {% if matches | length != 1 %}{{ exceptions.raise_compiler_error('Published relation removed') }}{% endif %}
                {% set new = matches[0] %}
                {% if old.get('access', 'analytical') != new.access and not (old.get('access', 'analytical') == 'analytical' and new.access == 'restricted' and old.relation in (access_tightenings or [])) %}{{ exceptions.raise_compiler_error('Published access changed; reclassification needs an explicit migration') }}{% endif %}
                {% if old.grain != new.grain or old.key != new.key %}{{ exceptions.raise_compiler_error('Published grain changed') }}{% endif %}
                {% for column in old.columns %}
                    {% set columns = new.columns | selectattr('name', 'equalto', column.name) | list %}
                    {% if columns | length != 1 or columns[0].data_type != column.data_type or (column.nullable and not columns[0].nullable) %}{{ exceptions.raise_compiler_error('Published column removed or retyped') }}{% endif %}
                {% endfor %}
                {% set old_names = old.columns | map(attribute='name') | list %}
                {% for column in new.columns %}
                    {% if column.name not in old_names and not column.nullable %}{{ exceptions.raise_compiler_error('New columns must be nullable') }}{% endif %}
                {% endfor %}
            {% endfor %}
        {% endif %}
        {% do run_query('create or replace table ' ~ c.database ~ '.' ~ c.candidate ~ "._publication as select '" ~ publication_id ~ "'::varchar as publication_id, current_timestamp()::timestamp_tz as published_at, '" ~ contracts_json | replace("'", "''") ~ "'::varchar as contracts") %}
        {# The marker holds contract metadata (names, types, descriptions; no
           values) for every mart so compatibility can be checked after a swap. #}
        {% set analytical = declared | rejectattr('access', 'equalto', 'restricted') | map(attribute='relation') | list %}
        {% set restricted = declared | selectattr('access', 'equalto', 'restricted') | map(attribute='relation') | list %}
        {# Cloned tables keep earlier grants. Clear every table grant for the
           consumer roles, then grant explicitly. Nothing is inherited. #}
        {% do run_query('revoke select on all tables in schema ' ~ c.database ~ '.' ~ c.candidate ~ ' from role ' ~ c.reader) %}
        {% if c.restricted_reader %}
            {% do run_query('revoke select on all tables in schema ' ~ c.database ~ '.' ~ c.candidate ~ ' from role ' ~ c.restricted_reader) %}
        {% endif %}
        {% for relation in analytical + ['_publication'] %}
            {% if not modules.re.fullmatch('[a-z_][a-z0-9_]*', relation) %}{{ exceptions.raise_compiler_error('Invalid relation') }}{% endif %}
            {% do run_query('grant select on table ' ~ c.database ~ '.' ~ c.candidate ~ '.' ~ adapter.quote(relation | upper) ~ ' to role ' ~ c.reader) %}
        {% endfor %}
        {% if c.restricted_reader %}
            {% for relation in restricted + ['_publication'] %}
                {% if not modules.re.fullmatch('[a-z_][a-z0-9_]*', relation) %}{{ exceptions.raise_compiler_error('Invalid relation') }}{% endif %}
                {% do run_query('grant select on table ' ~ c.database ~ '.' ~ c.candidate ~ '.' ~ adapter.quote(relation | upper) ~ ' to role ' ~ c.restricted_reader) %}
            {% endfor %}
        {% endif %}
        {# Fail closed: before any USAGE is granted, the default reader and PUBLIC
           must hold no privilege on a restricted relation. #}
        {% if restricted %}
            {% set exposed = run_query('select table_name, grantee from ' ~ c.database ~ ".information_schema.table_privileges where table_schema = '" ~ candidate_namespace | upper ~ "_MARTS' and privilege_type = 'SELECT' and grantee in ('" ~ reader_role | upper ~ "', 'PUBLIC') and table_name in ('" ~ (restricted | map('upper') | join("', '")) ~ "')") %}
            {% if exposed.rows | length > 0 %}
                {{ exceptions.raise_compiler_error('Restricted relations are readable by the default reader; publication refused') }}
            {% endif %}
        {% endif %}
        {# Only validated immutable candidates receive USAGE. This short interval
           exposes the validated candidate by its temporary name before SWAP. #}
        {% do run_query('grant usage on schema ' ~ c.database ~ '.' ~ c.candidate ~ ' to role ' ~ c.reader) %}
        {% if c.restricted_reader %}
            {% do run_query('grant usage on schema ' ~ c.database ~ '.' ~ c.candidate ~ ' to role ' ~ c.restricted_reader) %}
        {% endif %}
        {% set existing = run_query('select count(*) from ' ~ c.database ~ ".information_schema.schemata where schema_name = '" ~ namespace | upper ~ "_MARTS'") %}
        {% if existing.rows[0][0] > 0 %}
            {% do run_query('alter schema ' ~ c.database ~ '.' ~ c.candidate ~ ' swap with ' ~ c.database ~ '.' ~ c.published) %}
        {% else %}
            {% do run_query('alter schema ' ~ c.database ~ '.' ~ c.candidate ~ ' rename to ' ~ c.published) %}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro clean_mart_candidate(namespace, candidate_namespace, publication_id, reader_role, restricted_reader_role=none, relations=none, contracts_json=none, access_tightenings=none) %}
    {% set c = publication_context(namespace, candidate_namespace, publication_id, reader_role, restricted_reader_role) %}
    {% if execute %}
        {% do assert_publication_lease(c, publication_id) %}
        {% for layer in ['marts', 'staging', 'intermediate', 'seeds', 'snapshots', 'test_failures'] %}
            {% do run_query('drop schema if exists ' ~ c.database ~ '.' ~ adapter.quote(candidate_namespace | upper ~ '_' ~ layer | upper) ~ ' cascade') %}
        {% endfor %}
        {% do run_query('drop schema ' ~ c.database ~ '.' ~ c.lock ~ ' cascade') %}
    {% endif %}
{% endmacro %}

{% macro confirm_mart_publication(namespace, candidate_namespace, publication_id, reader_role, restricted_reader_role=none, relations=none, contracts_json=none, access_tightenings=none) %}
    {% set c = publication_context(namespace, candidate_namespace, publication_id, reader_role, restricted_reader_role) %}
    {% if execute %}
        {% do assert_publication_lease(c, publication_id) %}
        {% set marker = run_query('select publication_id from ' ~ c.database ~ '.' ~ c.published ~ '._publication') %}
        {% if marker.rows | length != 1 or marker.rows[0][0] != publication_id %}
            {{ exceptions.raise_compiler_error('Publication outcome unconfirmed; lock retained for operator recovery') }}
        {% endif %}
    {% endif %}
{% endmacro %}
