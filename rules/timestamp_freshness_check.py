"""
Rule 5: Timestamp Freshness Check

# DECISION: Measures the age of the newest record in a dataset. If the newest
# record is older than the SLA threshold, the data is stale. This is an
# AGGREGATE check — it doesn't identify individual bad records.

# TRADEOFF: Could check every record's individual timestamp (is each record
# fresh?). Chose max-age approach because: (1) freshness is about the dataset,
# not individual records, (2) individual age doesn't make sense for historical
# data, (3) max-age is what SLA monitoring cares about.

# FAILURE MODE: Clock skew between Spark cluster and source system. If the
# source timestamps are in a different timezone and we compare against UTC
# current_timestamp(), freshness calculations are wrong by hours.
# Mitigation: Always normalize timestamps to UTC before comparison.

# INTERVIEW: "What's the difference between data freshness and data latency?"
# → "Freshness = how old is the newest record right now. Latency = how long
#    did it take from event creation to availability. Freshness is a point-in-time
#    measure; latency is a pipeline performance measure. A fresh dataset can
#    have high latency records (late-arriving events)."

# SCALE: O(1) — single MAX() aggregation, regardless of data size.
# At 1B rows, MAX() on a timestamp column with partition pruning is ~1 second.
"""

import time
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.rule_configs import TimestampFreshnessRule
from rules.validation_result import ValidationResult


def timestamp_freshness_check(
    df: DataFrame,
    rule: TimestampFreshnessRule,
    dataset_name: str = "",
    reference_time: datetime = None,
) -> ValidationResult:
    """
    Check that the newest record in the dataset is within the freshness SLA.
    
    Args:
        df: DataFrame to validate
        rule: TimestampFreshnessRule configuration
        dataset_name: Name of dataset being validated
        reference_time: Optional reference time (defaults to current time)
    
    Returns:
        ValidationResult (aggregate check — no failed_df)
    """
    start_time = time.time()
    
    # Guard: check column exists
    if rule.column_name not in df.columns:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="timestamp_freshness_check",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={"error": f"Column '{rule.column_name}' not found"},
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    total = df.count()
    if total == 0:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="timestamp_freshness_check",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            total_records=0,
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={"error": "DataFrame is empty — no records to check freshness"},
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    # Get the max timestamp from the data
    # DECISION: Use Spark's current_timestamp() for consistency across cluster.
    # Python datetime.now() varies per executor node.
    col_type = dict(df.dtypes).get(rule.column_name)
    
    if col_type == "string":
        # Attempt to parse string timestamps
        max_ts_row = df.agg(
            F.max(F.to_timestamp(F.col(rule.column_name))).alias("max_ts")
        ).collect()[0]
    elif col_type in ("timestamp", "date"):
        max_ts_row = df.agg(
            F.max(F.col(rule.column_name)).alias("max_ts")
        ).collect()[0]
    else:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="timestamp_freshness_check",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            total_records=total,
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={"error": f"Column '{rule.column_name}' type {col_type} is not a timestamp"},
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    max_ts = max_ts_row["max_ts"]
    
    if max_ts is None:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="timestamp_freshness_check",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            total_records=total,
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={"error": "All timestamp values are null or unparseable"},
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    # Calculate age
    if reference_time is None:
        reference_time = datetime.utcnow()
    
    age_seconds = (reference_time - max_ts).total_seconds()
    age_hours = age_seconds / 3600.0
    
    # Check against SLA
    passed = age_hours <= rule.max_age_hours
    
    elapsed_ms = (time.time() - start_time) * 1000
    
    return ValidationResult(
        rule_id=rule.rule_id,
        rule_name="timestamp_freshness_check",
        rule_version=rule.rule_version,
        passed=passed,
        severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
        total_records=total,
        failed_records=0 if passed else total,  # Aggregate check — all or nothing
        failure_rate=0.0 if passed else 1.0,
        failed_df=None,  # No per-record failures — this is aggregate
        dataset_name=dataset_name,
        layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
        metadata={
            "column_name": rule.column_name,
            "max_timestamp": str(max_ts),
            "reference_time": str(reference_time),
            "age_hours": round(age_hours, 2),
            "sla_hours": rule.max_age_hours,
            "breach_hours": round(age_hours - rule.max_age_hours, 2) if not passed else 0,
        },
        execution_time_ms=elapsed_ms,
    )
