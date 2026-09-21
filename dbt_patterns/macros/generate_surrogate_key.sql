-- generate_surrogate_key.sql
-- Hash-based surrogate key macro.
-- Usage: {{ generate_surrogate_key(['user_id', 'session_id', 'event_date']) }}
-- Returns a deterministic MD5 hex string from the concatenation of the provided columns.
-- NULL-safe: coalesces each column to an empty string before hashing to avoid
-- hash(NULL || 'x') ≠ hash('' || 'x') inconsistencies.
--
-- Example output: 'a1b2c3d4e5f6...' (32-char hex string)
-- This is equivalent to dbt_utils.generate_surrogate_key but written in-house
-- so there is no external package dependency in production Databricks workflows.

{% macro generate_surrogate_key(field_list) %}
    md5(
        concat_ws(
            '||',
            {% for field in field_list %}
                coalesce(cast({{ field }} as string), '')
                {%- if not loop.last %},{% endif %}
            {% endfor %}
        )
    )
{% endmacro %}


-- Alternative: SHA-256 surrogate key for higher collision resistance.
-- Use this for >1B row tables where MD5 birthday collision risk is non-trivial.
-- Usage: {{ generate_surrogate_key_sha256(['user_id', 'event_id']) }}

{% macro generate_surrogate_key_sha256(field_list) %}
    sha2(
        concat_ws(
            '||',
            {% for field in field_list %}
                coalesce(cast({{ field }} as string), '')
                {%- if not loop.last %},{% endif %}
            {% endfor %}
        ),
        256
    )
{% endmacro %}


-- Partition-aware surrogate key that includes the partition column.
-- Prevents cross-partition key collisions when session_id is only unique within a date.
-- Usage: {{ generate_partitioned_surrogate_key(['session_id'], 'event_date') }}

{% macro generate_partitioned_surrogate_key(field_list, partition_field) %}
    {{ generate_surrogate_key(field_list + [partition_field]) }}
{% endmacro %}
