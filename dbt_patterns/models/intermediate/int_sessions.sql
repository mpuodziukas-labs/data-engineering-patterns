-- int_sessions.sql
-- Intermediate model: session-level aggregation built from staged events.
-- Uses is_incremental() for efficient incremental runs on large event tables.
-- Full refresh triggered via: dbt run --full-refresh --select int_sessions

{{ config(
    materialized='incremental',
    unique_key='session_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    partition_by={
        'field': 'session_date',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by=['user_id'],
    tags=['intermediate', 'sessions'],
    meta={'owner': 'data-engineering'}
) }}

with events as (
    select * from {{ ref('stg_events') }}

    {% if is_incremental() %}
        -- On incremental runs: only process events from the last 3 days
        -- (3-day lookback handles late-arriving events + restatements)
        where event_ts >= timestampadd(day, -3, current_timestamp())
    {% endif %}
),

session_bounds as (
    select
        session_id,
        user_id,
        to_date(min(event_ts))       as session_date,
        min(event_ts)                as session_start_ts,
        max(event_ts)                as session_end_ts,
        count(event_id)              as total_events,
        count(distinct event_type)   as unique_event_types
    from events
    group by session_id, user_id
),

session_financials as (
    select
        session_id,
        sum(revenue_amount)          as session_revenue,
        max(cast(is_paid as int))    as had_payment,
        collect_set(product_line)    as product_lines_touched
    from events
    group by session_id
),

session_engagement as (
    -- Session duration in seconds
    select
        b.session_id,
        b.user_id,
        b.session_date,
        b.session_start_ts,
        b.session_end_ts,
        unix_timestamp(b.session_end_ts) - unix_timestamp(b.session_start_ts) as duration_seconds,
        b.total_events,
        b.unique_event_types,
        f.session_revenue,
        case when f.had_payment = 1 then true else false end as had_payment,
        f.product_lines_touched,
        size(f.product_lines_touched)                        as product_lines_count,
        -- Engagement tier: drives personalization downstream
        case
            when b.total_events >= 50  then 'power_user'
            when b.total_events >= 20  then 'engaged'
            when b.total_events >= 5   then 'casual'
            else                            'bounced'
        end                                                  as engagement_tier,
        -- Revenue tier
        case
            when f.session_revenue >= 1000  then 'enterprise'
            when f.session_revenue >= 100   then 'mid_market'
            when f.session_revenue > 0      then 'smb'
            else                                 'free'
        end                                                  as revenue_tier,
        current_timestamp()                                  as _int_processed_at
    from session_bounds b
    left join session_financials f on b.session_id = f.session_id
)

select * from session_engagement
