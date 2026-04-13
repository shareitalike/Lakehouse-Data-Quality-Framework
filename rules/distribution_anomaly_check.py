"""
Rule 10: Distribution Anomaly Check

# =============================================================================
# WHY DISTRIBUTION DRIFT MATTERS
# =============================================================================
# Distribution drift is when the statistical shape of a column changes 
# unexpectedly. Examples:
#
# 1. PRICE MANIPULATION: A bot changes 10% of prices to $0.01.
#    Mean drops slightly (still "looks normal"), but P25 collapses.
# 
# 2. CURRENCY BUG: A code change applies currency conversion twice.
#    All EUR prices are 100x higher. Mean spikes, but so does every percentile.
#
# 3. DATA PIPELINE BUG: A filter is accidentally removed, doubling the data.
#    Distribution shape stays the same (just more of it), but row count 
#    anomaly detection catches this — distribution check won't.
#
# 4. SEASONAL SHIFT: Black Friday doubles revenue. This is EXPECTED drift.
#    Distribution check flags it; the team acknowledges and adjusts baseline.
#    Without distribution monitoring, you can't distinguish expected from 
#    unexpected shifts.
#
# 5. ML FEATURE DRIFT: An ML model uses "quantity" as a feature. If quantity
#    distribution shifts (users start buying in bulk), model predictions degrade.
#    Distribution monitoring is the first defense against silent ML failures.

# =============================================================================
# WHY MEAN COMPARISON ALONE IS INSUFFICIENT
# =============================================================================
# Anscombe's Quartet: four datasets with identical mean (7.5), variance (4.122),
# and linear regression — but completely different shapes (linear, parabolic,
# outlier-driven, and a single extreme point).
#
# Real-world example:
#   Dataset A: 1000 orders, all at $50.00 → mean = $50
#   Dataset B: 1 order at $50,000 + 999 orders at $0.05 → mean ≈ $50
#   Same mean. Completely different reality. Mean-only detection is blind.
#
# You MUST check the SHAPE of the distribution, not just its center.

# =============================================================================
# Z-SCORE VS PERCENTILE DETECTION
# =============================================================================
# Z-score: (current_mean - historical_mean) / historical_stddev
#   - PARAMETRIC: assumes normal distribution
#   - Good for: detecting shifts in the CENTER of bell-curve data
#   - Bad for: skewed data (prices, quantities are often right-skewed),
#     multi-modal data, fat-tailed distributions
#   - Example: detects "mean price jumped from $50 to $80" effectively
#   - Blind to: "prices are now bimodal — some at $5, some at $500"
#
# Percentile-based: compare P25/P50/P75 between current and historical
#   - NON-PARAMETRIC: works for any distribution shape
#   - Good for: detecting shape changes, works with skewed data
#   - Bad for: might miss tail changes if percentile granularity is coarse
#   - Example: detects "P25 dropped from $30 to $5" — lower quartile collapsed
#   - Blind to: changes only in extreme tails (P1, P99) if not tracked

# DECISION: Use BOTH z-score AND percentile detection. They complement each
# other. Z-score catches center shifts in normal-ish data. Percentiles catch
# shape changes in any distribution. Together they provide robust detection.

# TRADEOFF: More complex than single-method detection. But the alternative is
# missing entire categories of anomalies. In production, a missed anomaly
# costs more than the complexity of dual detection.

# FAILURE MODE: If historical baseline has high variance (e.g., prices range
# from $1 to $10,000), both methods lose sensitivity. Everything looks "normal"
# because the baseline is so wide. Mitigation: segment by product category
# before comparing (future enhancement).

# INTERVIEW: "Why do you use both z-score and percentile detection?"
# → "Z-score catches outlier injection (single bad values). Percentile
#    catches distribution shift (entire shape changes). A price manipulation
#    bot might not change the mean much but will destroy the percentile
#    structure. Conversely, a single massive outlier shows in z-score
#    but might not move percentiles."

# SCALE: 
# 10K rows → exact percentiles, instant
# 10M rows → approxQuantile(), ~5 seconds (O(n) Greenwald-Khanna algorithm)
# 1B rows → approxQuantile() with 1% relative error, ~30 seconds
# Optimization at extreme scale: use pre-computed histograms from Delta
# statistics or compute percentiles on a 1% sample.
"""

import time
import json
from typing import List, Dict, Optional, Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.rule_configs import DistributionAnomalyRule
from rules.validation_result import ValidationResult


def _compute_statistics(
    df: DataFrame,
    column: str,
    percentile_bands: List[float],
    use_approx: bool = True,
    relative_error: float = 0.01,
) -> Dict[str, Any]:
    """
    Compute comprehensive statistics for a numeric column.
    
    # DECISION: Compute all statistics in a single pass where possible.
    # PySpark's agg() evaluates multiple aggregation functions in one scan.
    # Percentiles require a separate call (approxQuantile is not an agg func).
    """
    # Basic statistics in one pass
    stats_row = df.agg(
        F.count(column).alias("count"),
        F.mean(column).alias("mean"),
        F.stddev(column).alias("stddev"),
        F.min(column).alias("min_val"),
        F.max(column).alias("max_val"),
        F.skewness(column).alias("skewness"),
        F.kurtosis(column).alias("kurtosis"),
    ).collect()[0]
    
    stats = {
        "count": int(stats_row["count"]) if stats_row["count"] else 0,
        "mean": float(stats_row["mean"]) if stats_row["mean"] else None,
        "stddev": float(stats_row["stddev"]) if stats_row["stddev"] else None,
        "min": float(stats_row["min_val"]) if stats_row["min_val"] else None,
        "max": float(stats_row["max_val"]) if stats_row["max_val"] else None,
        "skewness": float(stats_row["skewness"]) if stats_row["skewness"] else None,
        "kurtosis": float(stats_row["kurtosis"]) if stats_row["kurtosis"] else None,
    }
    
    # Compute percentiles
    # DECISION: Use approxQuantile for performance at scale.
    # Exact quantiles require sorting the entire dataset — O(n log n).
    # approxQuantile uses Greenwald-Khanna — O(n) time, O(1/ε) space.
    if use_approx:
        percentiles = df.approxQuantile(column, percentile_bands, relative_error)
    else:
        # Exact quantiles for small datasets
        percentiles = df.approxQuantile(column, percentile_bands, 0.0)
    
    stats["percentiles"] = {
        f"p{int(p*100)}": v 
        for p, v in zip(percentile_bands, percentiles)
    }
    
    return stats


def _compute_z_score(
    current_mean: float,
    historical_mean: float,
    historical_stddev: float,
) -> Optional[float]:
    """
    Compute z-score of current mean against historical baseline.
    
    # DECISION: Handle edge cases explicitly:
    # - stddev = 0 → all historical values identical → any change is infinite z
    # - stddev is None → insufficient data
    """
    if historical_stddev is None or historical_stddev == 0:
        if current_mean == historical_mean:
            return 0.0
        else:
            return float('inf')  # Any deviation from constant is infinite z
    
    return (current_mean - historical_mean) / historical_stddev


def _compute_percentile_drift(
    current_percentiles: Dict[str, float],
    historical_percentiles: Dict[str, float],
) -> Dict[str, float]:
    """
    Compute percentage drift for each percentile band.
    
    Returns dict of {percentile_name: drift_pct}
    """
    drift = {}
    for key in current_percentiles:
        if key in historical_percentiles:
            hist_val = historical_percentiles[key]
            curr_val = current_percentiles[key]
            
            if hist_val is not None and hist_val != 0:
                pct_change = ((curr_val - hist_val) / abs(hist_val)) * 100
                drift[key] = round(pct_change, 2)
            elif hist_val == 0:
                drift[key] = 100.0 if curr_val != 0 else 0.0
            else:
                drift[key] = None
    
    return drift


def distribution_anomaly_check(
    current_df: DataFrame,
    rule: DistributionAnomalyRule,
    historical_stats: Dict[str, Any] = None,
    dataset_name: str = "",
) -> ValidationResult:
    """
    Detect abnormal distribution changes in a numeric column.
    
    Uses dual detection:
    1. Z-score: Is the mean significantly different from historical?
    2. Percentile drift: Has the distribution shape changed?
    
    Args:
        current_df: Current DataFrame to analyze
        rule: DistributionAnomalyRule configuration
        historical_stats: Statistics from previous runs. If None, establishes
                         baseline and returns pass.
                         Expected format: output of _compute_statistics()
        dataset_name: Name of dataset being validated
    
    Returns:
        ValidationResult with comprehensive distribution metadata
    """
    start_time = time.time()
    
    # Guard: check column exists  
    if rule.column_name not in current_df.columns:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="distribution_anomaly_check",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={"error": f"Column '{rule.column_name}' not found"},
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    # Filter to non-null values for statistics
    analysis_df = current_df.filter(F.col(rule.column_name).isNotNull())
    record_count = analysis_df.count()
    total_count = current_df.count()
    
    # Check minimum sample size
    if record_count < rule.min_sample_size:
        elapsed_ms = (time.time() - start_time) * 1000
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="distribution_anomaly_check",
            rule_version=rule.rule_version,
            passed=True,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            total_records=total_count,
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={
                "note": f"Skipped — only {record_count} non-null records, "
                        f"minimum required: {rule.min_sample_size}",
                "column_name": rule.column_name,
            },
            execution_time_ms=elapsed_ms,
        )
    
    # Compute current statistics
    current_stats = _compute_statistics(
        analysis_df,
        rule.column_name,
        rule.percentile_bands,
        use_approx=rule.use_approx_quantile,
        relative_error=rule.relative_error,
    )
    
    # First run — establish baseline
    if historical_stats is None:
        elapsed_ms = (time.time() - start_time) * 1000
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="distribution_anomaly_check",
            rule_version=rule.rule_version,
            passed=True,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            total_records=total_count,
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={
                "column_name": rule.column_name,
                "baseline_established": True,
                "current_stats": current_stats,
                "note": "First run — baseline established, no comparison available",
            },
            execution_time_ms=elapsed_ms,
        )
    
    # Compare against historical baseline
    anomalies = []
    
    # 1. Z-score check
    z_score = _compute_z_score(
        current_stats["mean"],
        historical_stats.get("mean", 0),
        historical_stats.get("stddev", 0),
    )
    
    z_score_anomaly = abs(z_score) > rule.z_score_threshold if z_score is not None else False
    if z_score_anomaly:
        anomalies.append(
            f"Z-score anomaly: z={z_score:.2f} exceeds threshold ±{rule.z_score_threshold}"
        )
    
    # 2. Percentile drift check
    percentile_drift = {}
    max_drift = 0.0
    percentile_anomaly = False
    
    if "percentiles" in current_stats and "percentiles" in historical_stats:
        percentile_drift = _compute_percentile_drift(
            current_stats["percentiles"],
            historical_stats["percentiles"],
        )
        
        for pct_name, drift_val in percentile_drift.items():
            if drift_val is not None and abs(drift_val) > rule.max_percentile_drift_pct:
                percentile_anomaly = True
                anomalies.append(
                    f"Percentile drift: {pct_name} shifted {drift_val:.1f}% "
                    f"(threshold: ±{rule.max_percentile_drift_pct}%)"
                )
            if drift_val is not None:
                max_drift = max(max_drift, abs(drift_val))
    
    # Determine overall pass/fail
    is_anomaly = z_score_anomaly or percentile_anomaly
    passed = not is_anomaly
    
    elapsed_ms = (time.time() - start_time) * 1000
    
    return ValidationResult(
        rule_id=rule.rule_id,
        rule_name="distribution_anomaly_check",
        rule_version=rule.rule_version,
        passed=passed,
        severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
        total_records=total_count,
        failed_records=total_count if is_anomaly else 0,
        failure_rate=1.0 if is_anomaly else 0.0,
        failed_df=None,  # Aggregate check — distribution affects all records
        dataset_name=dataset_name,
        layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
        metadata={
            "column_name": rule.column_name,
            "current_stats": current_stats,
            "historical_stats": historical_stats,
            "z_score": round(z_score, 4) if z_score is not None else None,
            "z_score_threshold": rule.z_score_threshold,
            "z_score_anomaly": z_score_anomaly,
            "percentile_drift": percentile_drift,
            "max_percentile_drift_pct": round(max_drift, 2),
            "percentile_threshold_pct": rule.max_percentile_drift_pct,
            "percentile_anomaly": percentile_anomaly,
            "anomalies": anomalies,
            "detection_method": "dual (z-score + percentile)",
            "used_approx_quantile": rule.use_approx_quantile,
        },
        execution_time_ms=elapsed_ms,
    )
