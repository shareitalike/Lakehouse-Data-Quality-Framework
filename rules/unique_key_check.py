"""
Rule 2: Unique Key Check

# DECISION: Uses groupBy().count().filter(count > 1) to find duplicate keys.
# This is the standard PySpark pattern for uniqueness validation.

# TRADEOFF: Could use window function ROW_NUMBER() to identify duplicates.
# GroupBy is more efficient for just COUNTING duplicates because it doesn't
# need to assign row numbers to every record. Window function is better when
# you need to IDENTIFY which specific records to keep (that's duplicate_detection).

# FAILURE MODE: If key columns contain nulls, NULL groups collapse into one
# group. Two records with (null, null) as the key are counted as ONE group,
# not two. This means nulls slip through the uniqueness check.
# Mitigation: Always pair unique_key_check with not_null_check on key columns.

# INTERVIEW: "What's the difference between unique_key_check and duplicate_detection?"
# → "Unique key check answers 'ARE there duplicates?' (counts them).
#    Duplicate detection answers 'WHICH record do I keep?' (deduplicates them).
#    Unique check is validation. Duplicate detection is transformation."

# SCALE: GroupBy triggers a shuffle — O(n) data transfer.
# At 10K → instant. At 10M → ~10 seconds. At 1B → ~2 minutes.
# Optimization at scale: salting if key has extreme skew (one key with
# millions of duplicates). AQE handles moderate skew automatically.
"""

import time
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.rule_configs import UniqueKeyRule
from rules.validation_result import ValidationResult


def unique_key_check(
    df: DataFrame,
    rule: UniqueKeyRule,
    dataset_name: str = "",
) -> ValidationResult:
    """
    Check that columns form a unique key (no duplicate combinations).
    
    Args:
        df: DataFrame to validate
        rule: UniqueKeyRule configuration
        dataset_name: Name of the dataset being validated
    
    Returns:
        ValidationResult with duplicate key groups
    """
    start_time = time.time()
    
    # Guard: check all columns exist
    missing_cols = [c for c in rule.columns if c not in df.columns]
    if missing_cols:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="unique_key_check",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={"error": f"Missing columns: {missing_cols}"},
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    total = df.count()
    
    # Find duplicate key groups
    # DECISION: groupBy on key columns, count occurrences, filter count > 1
    dup_groups = (
        df.groupBy(rule.columns)
        .agg(F.count("*").alias("_dup_count"))
        .filter(F.col("_dup_count") > 1)
    )
    
    dup_group_count = dup_groups.count()
    
    # Count total records involved in duplicates
    # DECISION: Sum of counts gives total affected records, not just groups.
    # If order_id "ORD-001" appears 3 times, that's 1 group but 3 records.
    if dup_group_count > 0:
        total_dup_records = dup_groups.agg(F.sum("_dup_count")).collect()[0][0]
        # Total excess records (total dups - one per group = extras that shouldn't exist)
        excess_records = int(total_dup_records - dup_group_count)
    else:
        total_dup_records = 0
        excess_records = 0
    
    # Build failed DataFrame: all records that participate in duplicates
    if dup_group_count > 0:
        # Join back to original to get full records
        failed_df = df.join(
            dup_groups.select(rule.columns),
            on=rule.columns,
            how="inner"
        )
    else:
        failed_df = None
    
    failure_rate = excess_records / total if total > 0 else 0.0
    passed = dup_group_count == 0
    
    elapsed_ms = (time.time() - start_time) * 1000
    
    return ValidationResult(
        rule_id=rule.rule_id,
        rule_name="unique_key_check",
        rule_version=rule.rule_version,
        passed=passed,
        severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
        total_records=total,
        failed_records=excess_records,
        failure_rate=failure_rate,
        failed_df=failed_df,
        dataset_name=dataset_name,
        layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
        metadata={
            "key_columns": rule.columns,
            "duplicate_groups": dup_group_count,
            "total_duplicate_records": int(total_dup_records) if total_dup_records else 0,
            "excess_records": excess_records,
        },
        execution_time_ms=elapsed_ms,
    )
