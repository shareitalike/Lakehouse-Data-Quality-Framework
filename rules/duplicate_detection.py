"""
Rule 6: Duplicate Detection

# DECISION: Uses window function ROW_NUMBER() to identify WHICH records to
# keep and WHICH to quarantine. This is a TRANSFORMATION rule, not just
# validation — it produces a deduped DataFrame AND identifies duplicates.

# TRADEOFF: Could use dropDuplicates() — simpler, one line. But dropDuplicates
# is non-deterministic about WHICH record it keeps. With ROW_NUMBER() + orderBy,
# we deterministically keep the latest (or earliest) record. In production,
# non-deterministic deduplication is a bug.

# FAILURE MODE: If order_by_column has ties (two records with same timestamp),
# ROW_NUMBER() breaks ties arbitrarily. Mitigation: add a tiebreaker column
# (e.g., secondary sort by ingestion timestamp or a hash).

# INTERVIEW: "Why ROW_NUMBER instead of dropDuplicates?"
# → "dropDuplicates is non-deterministic — you don't control which record
#    survives. ROW_NUMBER with orderBy gives deterministic results: I always
#    keep the latest record. Reproducibility matters in production pipelines."

# SCALE: Window function requires a shuffle (partitionBy). At 10K → instant.
# At 10M → ~15 seconds. At 1B → ~3 minutes. If a single key has millions
# of duplicates (data skew), the executor handling that key OOMs.
# Mitigation: pre-filter extreme duplicate groups before windowing.
"""

import time
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.rule_configs import DuplicateDetectionRule
from rules.validation_result import ValidationResult


def duplicate_detection(
    df: DataFrame,
    rule: DuplicateDetectionRule,
    dataset_name: str = "",
) -> ValidationResult:
    """
    Detect and identify duplicate records based on key columns.
    
    Unlike unique_key_check (which just counts duplicates), this rule
    identifies WHICH records to keep and WHICH to quarantine.
    
    Args:
        df: DataFrame to validate
        rule: DuplicateDetectionRule configuration
        dataset_name: Name of dataset being validated
    
    Returns:
        ValidationResult where failed_df contains the duplicate records
        (the records that would be removed in deduplication)
    """
    start_time = time.time()
    
    # Guard: check columns exist
    missing_cols = [c for c in rule.key_columns if c not in df.columns]
    if missing_cols:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="duplicate_detection",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={"error": f"Missing columns: {missing_cols}"},
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    total = df.count()
    
    # Build window: partition by key columns, order by tiebreaker
    # DECISION: Descending order when keep="last" so ROW_NUMBER=1 is the latest.
    # Ascending when keep="first" so ROW_NUMBER=1 is the earliest.
    if rule.order_by_column and rule.order_by_column in df.columns:
        if rule.keep == "last":
            order_expr = F.col(rule.order_by_column).desc()
        else:
            order_expr = F.col(rule.order_by_column).asc()
    else:
        # No order column — use monotonically_increasing_id as tiebreaker
        # DECISION: This makes dedup deterministic at least within a single run.
        df = df.withColumn("_dedup_tiebreak", F.monotonically_increasing_id())
        if rule.keep == "last":
            order_expr = F.col("_dedup_tiebreak").desc()
        else:
            order_expr = F.col("_dedup_tiebreak").asc()
    
    window = Window.partitionBy(rule.key_columns).orderBy(order_expr)
    
    # Add row number
    df_with_rn = df.withColumn("_row_num", F.row_number().over(window))
    
    # Records to KEEP (row_num = 1)
    # Records to QUARANTINE (row_num > 1) — these are the duplicates
    duplicates_df = df_with_rn.filter(F.col("_row_num") > 1)
    
    # Clean up temporary columns
    temp_cols = ["_row_num"]
    if "_dedup_tiebreak" in df_with_rn.columns:
        temp_cols.append("_dedup_tiebreak")
    
    duplicates_clean = duplicates_df.drop(*temp_cols)
    dup_count = duplicates_clean.count()
    
    failure_rate = dup_count / total if total > 0 else 0.0
    passed = dup_count == 0
    
    elapsed_ms = (time.time() - start_time) * 1000
    
    return ValidationResult(
        rule_id=rule.rule_id,
        rule_name="duplicate_detection",
        rule_version=rule.rule_version,
        passed=passed,
        severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
        total_records=total,
        failed_records=dup_count,
        failure_rate=failure_rate,
        failed_df=duplicates_clean if dup_count > 0 else None,
        dataset_name=dataset_name,
        layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
        metadata={
            "key_columns": rule.key_columns,
            "order_by": rule.order_by_column,
            "keep_strategy": rule.keep,
            "duplicate_records_found": dup_count,
            "unique_records": total - dup_count,
            "dedup_ratio": round(1 - (dup_count / total), 4) if total > 0 else 1.0,
        },
        execution_time_ms=elapsed_ms,
    )


def deduplicate_df(
    df: DataFrame,
    rule: DuplicateDetectionRule,
) -> DataFrame:
    """
    Return a deduplicated DataFrame (keeping the record per rule strategy).
    
    This is a convenience function that returns the CLEAN data after
    deduplication, for use in pipeline transformations.
    
    # DECISION: Separate function from the check. The check returns
    # ValidationResult (observability). This returns the cleaned DataFrame
    # (transformation). Same logic, different output.
    """
    if rule.order_by_column and rule.order_by_column in df.columns:
        if rule.keep == "last":
            order_expr = F.col(rule.order_by_column).desc()
        else:
            order_expr = F.col(rule.order_by_column).asc()
    else:
        df = df.withColumn("_dedup_tiebreak", F.monotonically_increasing_id())
        if rule.keep == "last":
            order_expr = F.col("_dedup_tiebreak").desc()
        else:
            order_expr = F.col("_dedup_tiebreak").asc()
    
    window = Window.partitionBy(rule.key_columns).orderBy(order_expr)
    
    result = (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    
    if "_dedup_tiebreak" in result.columns:
        result = result.drop("_dedup_tiebreak")
    
    return result
