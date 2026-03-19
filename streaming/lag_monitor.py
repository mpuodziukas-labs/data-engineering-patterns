"""
Kafka Consumer Group Lag Monitor: alert if lag exceeds threshold.
Production pattern — catches stalled consumers before they fall hours behind.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

LAG_ALERT_THRESHOLD: int = 10_000   # Alert if any partition lag exceeds this
LAG_CRITICAL_THRESHOLD: int = 100_000  # Critical: streaming consumer likely stalled


@dataclass(frozen=True)
class LagMonitorConfig:
    bootstrap_servers: list[str]
    consumer_group_id: str
    topics: list[str]
    alert_threshold: int = LAG_ALERT_THRESHOLD
    critical_threshold: int = LAG_CRITICAL_THRESHOLD
    poll_interval_seconds: float = 30.0


@dataclass(frozen=True)
class PartitionLag:
    topic: str
    partition: int
    consumer_offset: int
    log_end_offset: int
    lag: int

    @property
    def severity(self) -> str:
        if self.lag >= LAG_CRITICAL_THRESHOLD:
            return "CRITICAL"
        if self.lag >= LAG_ALERT_THRESHOLD:
            return "WARN"
        return "OK"


@dataclass(frozen=True)
class LagReport:
    consumer_group: str
    polled_at: float
    partitions: list[PartitionLag]
    total_lag: int
    max_partition_lag: int
    alert_triggered: bool
    critical_triggered: bool

    def summary(self) -> str:
        return (
            f"Group={self.consumer_group} TotalLag={self.total_lag:,} "
            f"MaxPartitionLag={self.max_partition_lag:,} "
            f"Alert={self.alert_triggered} Critical={self.critical_triggered}"
        )


# ---------------------------------------------------------------------------
# Offset fetching
# ---------------------------------------------------------------------------


def _get_log_end_offsets(bootstrap_servers: list[str], topics: list[str]) -> dict[tuple[str, int], int]:
    """Fetch the latest (log end) offset for each topic-partition."""
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
    partitions = []
    for topic in topics:
        tp_meta = consumer.partitions_for_topic(topic)
        if tp_meta is None:
            logger.warning("Topic %s not found on broker", topic)
            continue
        from kafka import TopicPartition
        partitions.extend([TopicPartition(topic, p) for p in tp_meta])

    end_offsets = consumer.end_offsets(partitions)
    consumer.close()

    return {(tp.topic, tp.partition): offset for tp, offset in end_offsets.items()}


def _get_consumer_committed_offsets(
    bootstrap_servers: list[str],
    group_id: str,
    topics: list[str],
) -> dict[tuple[str, int], int]:
    """Fetch committed offsets for the consumer group."""
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        from kafka import TopicPartition
        all_partitions: list[Any] = []
        temp_consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
        for topic in topics:
            tp_meta = temp_consumer.partitions_for_topic(topic)
            if tp_meta:
                all_partitions.extend([TopicPartition(topic, p) for p in tp_meta])
        temp_consumer.close()

        committed: dict[tuple[str, int], int] = {}
        for tp in all_partitions:
            offset_info = admin.list_consumer_group_offsets(group_id)
            if tp in offset_info:
                committed[(tp.topic, tp.partition)] = offset_info[tp].offset
            else:
                committed[(tp.topic, tp.partition)] = 0
    finally:
        admin.close()

    return committed


# ---------------------------------------------------------------------------
# Lag computation
# ---------------------------------------------------------------------------


def compute_lag(
    config: LagMonitorConfig,
) -> LagReport:
    """
    Compute per-partition lag for the consumer group.
    Returns a structured LagReport with alert flags.
    """
    log_end = _get_log_end_offsets(config.bootstrap_servers, config.topics)
    committed = _get_consumer_committed_offsets(config.bootstrap_servers, config.consumer_group_id, config.topics)

    partitions: list[PartitionLag] = []
    for (topic, partition), leo in log_end.items():
        consumer_offset = committed.get((topic, partition), 0)
        lag = max(0, leo - consumer_offset)
        partitions.append(PartitionLag(topic=topic, partition=partition, consumer_offset=consumer_offset, log_end_offset=leo, lag=lag))

    total_lag = sum(p.lag for p in partitions)
    max_lag = max((p.lag for p in partitions), default=0)

    report = LagReport(
        consumer_group=config.consumer_group_id,
        polled_at=time.time(),
        partitions=partitions,
        total_lag=total_lag,
        max_partition_lag=max_lag,
        alert_triggered=max_lag >= config.alert_threshold,
        critical_triggered=max_lag >= config.critical_threshold,
    )

    return report


# ---------------------------------------------------------------------------
# Alert hooks
# ---------------------------------------------------------------------------


def log_lag_report(report: LagReport) -> None:
    """Log a lag report. Replace with PagerDuty / Slack webhook in production."""
    if report.critical_triggered:
        logger.critical("LAG CRITICAL — %s", report.summary())
        for p in report.partitions:
            if p.severity == "CRITICAL":
                logger.critical("  %s[%d] lag=%d", p.topic, p.partition, p.lag)
    elif report.alert_triggered:
        logger.warning("LAG ALERT — %s", report.summary())
        for p in report.partitions:
            if p.severity != "OK":
                logger.warning("  %s[%d] lag=%d", p.topic, p.partition, p.lag)
    else:
        logger.info("LAG OK — %s", report.summary())


# ---------------------------------------------------------------------------
# Continuous monitor
# ---------------------------------------------------------------------------


def run_lag_monitor(config: LagMonitorConfig, max_iterations: int | None = None) -> None:
    """
    Poll lag continuously at config.poll_interval_seconds.
    max_iterations=None → run forever (use for long-running monitoring jobs).
    max_iterations=N → useful for testing.
    """
    iteration = 0
    while max_iterations is None or iteration < max_iterations:
        try:
            report = compute_lag(config)
            log_lag_report(report)
        except KafkaError as exc:
            logger.error("Kafka error during lag poll: %s", exc)
        except Exception as exc:
            logger.error("Unexpected error during lag poll: %s", exc, exc_info=True)

        iteration += 1
        if max_iterations is None or iteration < max_iterations:
            time.sleep(config.poll_interval_seconds)
