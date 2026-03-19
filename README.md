# data-engineering-patterns

Production Medallion architecture patterns for Databricks. Z-ORDER. CDC. Kafka lag monitoring. dbt incremental models. Tested patterns, not toy examples.

Built for the real constraints of production Databricks environments: multi-terabyte event tables, late-arriving data, stalled consumers, schema drift, and SLA breaches at 3 AM.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    MEDALLION ARCHITECTURE                    │
├─────────────┬─────────────────┬───────────────────────────┤
│   BRONZE    │     SILVER      │           GOLD            │
│             │                 │                           │
│  Raw Data   │  Cleaned +      │  Business Aggregates      │
│  Auto Loader│  Deduped +      │  Daily Metrics            │
│  Schema     │  Type Cast +    │  User Cohorts             │
│  Inference  │  GX Validated   │  Revenue Summary          │
│  Bad Record │  MERGE upserts  │  Partition + Z-ORDER      │
│  Quarantine │                 │  SLA tracking             │
└─────────────┴─────────────────┴───────────────────────────┘
        │               │                    │
        ▼               ▼                    ▼
   Delta Table    Delta Table          Delta Table
   (append-only)  (merge-on-key)       (overwrite partition)

┌─────────────────────────────────────────────────────────────┐
│                    STREAMING LAYER                          │
│                                                             │
│  Kafka Topic → Structured Streaming → Watermarks           │
│       │                                                     │
│       ├── Good Records → Bronze (exactly-once)             │
│       └── Bad Records  → DLQ (quarantine + reprocess)      │
│                                                             │
│  Lag Monitor: polls every 30s, alerts at >10K msgs lag      │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                     DELTA PATTERNS                          │
│                                                             │
│  Time Travel   → VERSION AS OF / RESTORE TABLE             │
│  CDC           → Change Data Feed → fan-out to N targets   │
│  OPTIMIZE      → ZORDER BY + partition-aware compaction     │
│  Schema Evol.  → mergeSchema / column mapping / widening    │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                      dbt PATTERNS                           │
│                                                             │
│  stg_events    → view  (rename + type cast + filter)       │
│  int_sessions  → incremental merge, 3-day lookback         │
│  mart_daily    → insert_overwrite by partition             │
│  schema.yml    → column-level tests (unique, not_null,     │
│                   accepted_values, range)                   │
│  macros        → surrogate key (MD5 / SHA-256)             │
└─────────────────────────────────────────────────────────────┘
```

---

## Benchmark Results

| Scenario | Before | After | Improvement | Speedup |
|----------|--------|-------|-------------|---------|
| Z-ORDER on `user_id` | 45.2s | 8.1s | **82%** | 5.6x |
| Partition by `event_date` | 38.7s | 4.3s | **89%** | 9.0x |
| Partition + Z-ORDER combined | 41.5s | 3.2s | **92%** | 13.0x |
| Small files compaction | 22.1s | 14.8s | **33%** | 1.5x |

_10M rows, Databricks Runtime 14.3, i3.xlarge × 2 workers. Full methodology in [benchmarks/RESULTS.md](benchmarks/RESULTS.md)._

---

## Repository Structure

```
data-engineering-patterns/
├── medallion/
│   ├── bronze_ingestion.py       # Auto Loader + bad record quarantine
│   ├── silver_transform.py       # Dedup + type cast + Great Expectations checks
│   ├── gold_aggregation.py       # Business aggregates + SLA tracking
│   └── pipeline_runner.py        # Bronze→Silver→Gold with retry
├── delta_patterns/
│   ├── time_travel.py            # VERSION AS OF, RESTORE TABLE, DESCRIBE HISTORY
│   ├── cdc_stream.py             # Change Data Feed consumer + fan-out
│   ├── optimize_zorder.py        # OPTIMIZE + ZORDER benchmark harness
│   └── schema_evolution.py       # mergeSchema, column mapping, type widening
├── streaming/
│   ├── kafka_consumer.py         # Structured Streaming + watermarks + DLQ routing
│   ├── lag_monitor.py            # Consumer group lag checker (alerts >10K)
│   └── dlq_handler.py            # Quarantine + reprocess dead letter queue
├── dbt_patterns/
│   ├── models/
│   │   ├── staging/stg_events.sql          # Source freshness + rename
│   │   ├── intermediate/int_sessions.sql   # Incremental merge, 3-day lookback
│   │   └── marts/mart_daily_metrics.sql    # Final mart + rolling windows
│   ├── schema.yml                # Column-level tests + accepted values
│   └── macros/generate_surrogate_key.sql   # MD5 + SHA-256 surrogate keys
├── benchmarks/
│   ├── query_benchmark.py        # Automated before/after benchmark runner
│   └── RESULTS.md                # Concrete numbers from production runs
├── tests/
│   └── test_patterns.py          # 12 unit tests (no live Spark required)
├── .github/workflows/ci.yml      # Syntax check + pytest + ruff
└── requirements.txt
```

---

## Key Patterns

### Bronze: Bad Record Quarantine

Every bad record is written to a quarantine Delta table instead of crashing the pipeline. The main table only receives records where `_corrupt_record IS NULL`.

```python
from medallion.bronze_ingestion import BronzeIngestionConfig, run_bronze_batch

config = BronzeIngestionConfig(
    source_path="s3://my-bucket/raw/events/",
    bronze_table="bronze.events",
    quarantine_table="bronze.events_quarantine",
    checkpoint_location="/checkpoints/bronze_events",
    batch_id="2026-03-18",
)
metrics = run_bronze_batch(config)
# {"good_records": 9_847_231, "bad_records": 1_523}
```

### Silver: Idempotent Merge + GX Checks

Every Silver write is a MERGE on the natural key. Great Expectations-style checks run before any write — if null rate exceeds threshold, the pipeline halts.

```python
from medallion.silver_transform import SilverTransformConfig, run_silver_transform

config = SilverTransformConfig(
    bronze_table="bronze.events",
    silver_table="silver.events",
    dedup_key="event_id",
    order_by_col="event_ts",
    merge_key="event_id",
    null_threshold=0.05,
    expected_columns=["event_id", "user_id", "event_ts", "event_type"],
)
result = run_silver_transform(config)
```

### Delta CDC: Change Data Feed Fan-out

Stream changes from a source Delta table to N downstream tables. Only insert and update_postimage rows are forwarded.

```python
from delta_patterns.cdc_stream import CdcConsumerConfig, run_cdc_streaming

config = CdcConsumerConfig(
    source_table="silver.users",
    checkpoint_location="/checkpoints/cdc_users",
    starting_version=0,
    downstream_targets={
        "gold.users_us":   "country = 'US'",
        "gold.users_eu":   "country IN ('GB', 'DE', 'FR')",
        "gold.users_all":  None,  # No filter — gets all changes
    },
)
run_cdc_streaming(config, spark)
```

### Z-ORDER Benchmark

```python
from delta_patterns.optimize_zorder import OptimizeConfig, benchmark_zorder

config = OptimizeConfig(
    table_name="silver.events",
    z_order_columns=["user_id"],
    benchmark_query="SELECT count(*), sum(revenue_amount) FROM silver.events WHERE user_id = 'u_12345'",
    benchmark_filter="user_id = 'u_12345'",
)
report = benchmark_zorder(spark, config)
# {"improvement_pct": 82.1, "speedup_factor": 5.58, "verdict": "EXCELLENT"}
```

### Kafka Lag Monitoring

Alerts when any consumer group partition exceeds 10,000 messages. Replace `log_lag_report` with your PagerDuty/Slack webhook.

```python
from streaming.lag_monitor import LagMonitorConfig, run_lag_monitor

config = LagMonitorConfig(
    bootstrap_servers=["kafka-broker:9092"],
    consumer_group_id="my-structured-streaming-consumer",
    topics=["events", "users"],
    alert_threshold=10_000,
    critical_threshold=100_000,
    poll_interval_seconds=30.0,
)
run_lag_monitor(config)
```

---

## Running Tests

```bash
pip install pytest pytest-mock pyspark==3.5.4 delta-spark==3.2.1
PYTHONPATH=. pytest tests/ -v
```

Expected output:
```
tests/test_patterns.py::test_bronze_quarantine_logic_skips_when_no_bad_records PASSED
tests/test_patterns.py::test_bronze_metadata_columns_added PASSED
tests/test_patterns.py::test_silver_dedup_removes_duplicates PASSED
tests/test_patterns.py::test_gx_row_count_check_fails_on_empty_dataframe PASSED
tests/test_patterns.py::test_gx_row_count_check_passes_with_data PASSED
tests/test_patterns.py::test_gx_columns_exist_catches_missing PASSED
tests/test_patterns.py::test_sla_tracking_detects_breach PASSED
tests/test_patterns.py::test_sla_tracking_passes_within_threshold PASSED
tests/test_patterns.py::test_retry_backoff_is_exponential PASSED
tests/test_patterns.py::test_retry_backoff_capped_at_max PASSED
tests/test_patterns.py::test_run_with_retry_succeeds_on_second_attempt PASSED
tests/test_patterns.py::test_scenario_result_improvement_calculation PASSED
12 passed in 0.31s
```

---

## Why These Patterns

These patterns came from production incidents:

- **Bronze quarantine**: a single malformed JSON record was crashing a 6-hour ingestion job
- **Silver GX checks**: a upstream schema change silently zeroed out `revenue_amount` for 3 days
- **Z-ORDER benchmark**: a user-level filter query was scanning 128 files and taking 45s; after Z-ORDER it scanned 4 files in 8s
- **DLQ reprocessing**: a Kafka schema registry outage sent 800K records to the DLQ; reprocess_dlq() recovered all of them in one run after the fix
- **Lag monitor**: a stalled consumer was 2M messages behind before anyone noticed; the lag monitor would have caught it at 10K

---

## Requirements

- Python 3.10+
- PySpark 3.5+ / Databricks Runtime 13.3 LTS+
- Delta Lake 3.0+
- For CDC: `delta.enableChangeDataFeed = true` on source tables
- For column rename/drop: column mapping mode must be enabled first
