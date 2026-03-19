-- stg_events.sql
-- Staging model: minimal transformations, source column renaming only.
-- No business logic at this layer — that belongs in intermediate.
-- Source freshness is enforced via schema.yml (loaded_at_field + warn/error thresholds).

{{ config(
    materialized='view',
    tags=['staging', 'events'],
    meta={
        'owner': 'data-engineering',
        'contains_pii': true
    }
) }}

with source as (
    select * from {{ source('raw', 'events') }}
),

renamed as (
    select
        -- Primary keys
        cast(event_id          as string)  as event_id,

        -- Foreign keys
        cast(user_id           as string)  as user_id,
        cast(session_id        as string)  as session_id,

        -- Timestamps — cast to UTC timestamp, never trust source timezone labels
        to_utc_timestamp(
            to_timestamp(event_ts, "yyyy-MM-dd'T'HH:mm:ss"),
            'UTC'
        )                                  as event_ts,

        -- Event attributes
        lower(trim(event_type))            as event_type,
        lower(trim(product_line))          as product_line,

        -- Financials — coerce nulls to 0.0 to avoid silent aggregation errors
        coalesce(cast(revenue_amount as double), 0.0)  as revenue_amount,

        -- Boolean flags — normalize to true/false
        case
            when lower(trim(is_paid)) in ('true', '1', 'yes') then true
            when lower(trim(is_paid)) in ('false', '0', 'no') then false
            else null
        end                                as is_paid,

        -- Metadata (keep raw for debugging)
        metadata                           as raw_metadata,

        -- Source audit columns (from bronze/auto-loader)
        _ingested_at,
        _source_path,
        _batch_id,
        _record_hash

    from source
    where
        -- Hard filters: rows failing these are genuinely bad data
        event_id is not null
        and user_id is not null
        and event_ts is not null
        -- Reject future-dated events (clock skew > 1 hour = data quality issue)
        and event_ts <= timestampadd(hour, 1, current_timestamp())
        -- Reject events older than 1 year (retention boundary)
        and event_ts >= timestampadd(year, -1, current_timestamp())
)

select * from renamed
