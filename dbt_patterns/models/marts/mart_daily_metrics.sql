-- mart_daily_metrics.sql
-- Final mart: daily business metrics consumed by BI tools + executive dashboards.
-- Append-only partitioned table — no updates, full day partitions rebuilt on rerun.
-- SLA: must complete within 15 minutes of daily trigger.

{{ config(
    materialized='incremental',
    unique_key=['metric_date', 'event_type', 'revenue_tier'],
    incremental_strategy='insert_overwrite',
    partition_by={
        'field': 'metric_date',
        'data_type': 'date',
        'granularity': 'day'
    },
    tags=['marts', 'daily', 'bi'],
    meta={
        'owner': 'data-engineering',
        'sla_minutes': 15,
        'consumers': ['tableau', 'looker', 'exec_dashboard']
    }
) }}

with sessions as (
    select * from {{ ref('int_sessions') }}

    {% if is_incremental() %}
        -- Rebuild the last 3 days to handle late session closes
        where session_date >= date_sub(current_date(), 3)
    {% endif %}
),

daily_agg as (
    select
        session_date                                        as metric_date,
        -- We join back to events for event_type-level breakdowns
        'all'                                               as event_type,
        revenue_tier,
        engagement_tier,

        -- Volume metrics
        count(distinct session_id)                          as unique_sessions,
        count(distinct user_id)                             as unique_users,

        -- Engagement metrics
        round(avg(duration_seconds), 2)                     as avg_session_duration_seconds,
        round(avg(total_events), 2)                         as avg_events_per_session,
        round(avg(product_lines_count), 2)                  as avg_products_per_session,

        -- Revenue metrics
        round(sum(session_revenue), 2)                      as gross_revenue,
        round(avg(session_revenue), 2)                      as avg_revenue_per_session,
        count(case when had_payment then 1 end)             as paying_sessions,
        round(
            count(case when had_payment then 1 end)
            / nullif(count(distinct session_id), 0) * 100,
            2
        )                                                   as conversion_rate_pct,

        -- Percentiles (approximate)
        percentile_approx(session_revenue, 0.50)            as p50_revenue,
        percentile_approx(session_revenue, 0.90)            as p90_revenue,
        percentile_approx(session_revenue, 0.99)            as p99_revenue,
        percentile_approx(duration_seconds, 0.50)           as p50_duration_seconds,

        -- Audit
        current_timestamp()                                 as _mart_processed_at,
        '{{ invocation_id }}'                               as _dbt_invocation_id

    from sessions
    group by
        session_date,
        revenue_tier,
        engagement_tier
),

with_mom_context as (
    -- Month-over-month comparison context (window function for trend signals)
    select
        *,
        sum(gross_revenue) over (
            partition by revenue_tier
            order by metric_date
            rows between 29 preceding and current row
        )                                                   as rolling_30d_revenue,
        sum(unique_sessions) over (
            partition by revenue_tier
            order by metric_date
            rows between 6 preceding and current row
        )                                                   as rolling_7d_sessions
    from daily_agg
)

select * from with_mom_context
