"""
Rule 9: Row Count Anomaly Detection

# DECISION: Compares current row count against historical baseline (mean of
# previous N runs). Flags as anomaly if deviation exceeds configured threshold.
# This is an OBSERVABILITY check — it doesn't fix data, it alerts on patterns.

# TRADEOFF: Could use more sophisticated models (exponential smoothing, Prophet).
# Chose simple mean + percentage deviation because: (1) interpretable — anyone
# can understand "50% fewer rows than usual", (2) no ML dependencies,
# (3) sufficient for 90% of production cases.

# FAILURE MODE: Seasonal patterns cause false positives. Monday always has 2x
# Friday's volume, but mean-based detection flags Monday as anomalous.
# Mitigation: day-of-week aware baselines (future enhancement). For now,
# use wider thresholds to accommodate regular variance.

# INTERVIEW: "Why is COUNT(*) alone insufficient for data quality?"
# → "COUNT(*) tells you HOW MANY rows, not WHETHER those rows are valid.
#    A table with 1M rows could have 1M identical duplicates — COUNT looks
#    fine, quality is terrible. Count anomaly detection catches VOLUME
#    changes; other rules catch CONTENT issues. You need both."

# SCALE: O(1) — just a count and comparison. Same cost at 10K and 1B rows.
# The count itself varies: 10K → instant, 1B → ~10 seconds (scan metadata
# if using Delta — Delta stores row count in transaction log).
"""

import time
from typing import List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.rule_configs import RowCountAnomalyRule
from rules.validation_result import ValidationResult


def row_count_anomaly_detection(
    df: DataFrame,
    rule: RowCountAnomalyRule,
    historical_counts: List[int] = None,
    dataset_name: str = "",
) -> ValidationResult:
    """
    Detect anomalous row count compared to historical baseline.
    
    Args:
        df: DataFrame to validate
        rule: RowCountAnomalyRule configuration
        historical_counts: List of row counts from previous pipeline runs.
                          If None or empty, this is treated as the first run
                          and the check always passes (establishes baseline).
        dataset_name: Name of dataset being validated
    
    Returns:
        ValidationResult with anomaly details in metadata
    """
    start_time = time.time()
    
    current_count = df.count()
    
    # First run — no historical data to compare against
    # DECISION: Pass on first run, log baseline. Can't detect anomalies
    # without a baseline. Failing first run would block deployment.
    if not historical_counts:
        elapsed_ms = (time.time() - start_time) * 1000
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="row_count_anomaly_detection",
            rule_version=rule.rule_version,
            passed=True,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            total_records=current_count,
            failed_records=0,
            failure_rate=0.0,
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={
                "current_count": current_count,
                "historical_counts": [],
                "baseline_established": True,
                "note": "First run — establishing baseline, no anomaly detection possible",
            },
            execution_time_ms=elapsed_ms,
        )
    
    # Use last N runs for baseline
    recent_counts = historical_counts[-rule.lookback_runs:]
    
    # Calculate baseline statistics
    baseline_mean = sum(recent_counts) / len(recent_counts)
    baseline_min = min(recent_counts)
    baseline_max = max(recent_counts)
    
    # Standard deviation for context
    if len(recent_counts) > 1:
        variance = sum((x - baseline_mean) ** 2 for x in recent_counts) / len(recent_counts)
        baseline_stddev = variance ** 0.5
    else:
        baseline_stddev = 0.0
    
    # Calculate deviation
    if baseline_mean > 0:
        deviation_pct = ((current_count - baseline_mean) / baseline_mean) * 100
    else:
        deviation_pct = 100.0 if current_count > 0 else 0.0
    
    # Check against thresholds
    # DECISION: Asymmetric thresholds — drops are more concerning than spikes.
    is_anomaly = (
        deviation_pct < rule.min_deviation_pct or
        deviation_pct > rule.max_deviation_pct
    )
    
    anomaly_type = None
    if deviation_pct < rule.min_deviation_pct:
        anomaly_type = "count_drop"
    elif deviation_pct > rule.max_deviation_pct:
        anomaly_type = "count_spike"
    
    passed = not is_anomaly
    
    elapsed_ms = (time.time() - start_time) * 1000
    
    return ValidationResult(
        rule_id=rule.rule_id,
        rule_name="row_count_anomaly_detection",
        rule_version=rule.rule_version,
        passed=passed,
        severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
        total_records=current_count,
        failed_records=current_count if is_anomaly else 0,
        failure_rate=1.0 if is_anomaly else 0.0,
        failed_df=None,  # Aggregate check — no per-record failures
        dataset_name=dataset_name,
        layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
        metadata={
            "current_count": current_count,
            "baseline_mean": round(baseline_mean, 1),
            "baseline_stddev": round(baseline_stddev, 1),
            "baseline_min": baseline_min,
            "baseline_max": baseline_max,
            "deviation_pct": round(deviation_pct, 2),
            "min_threshold_pct": rule.min_deviation_pct,
            "max_threshold_pct": rule.max_deviation_pct,
            "lookback_runs": len(recent_counts),
            "is_anomaly": is_anomaly,
            "anomaly_type": anomaly_type,
            "historical_counts": recent_counts,
        },
        execution_time_ms=elapsed_ms,
    )
