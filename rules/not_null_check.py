"""
Rule 1: Not Null Check

# DECISION: Simplest rule, but most commonly needed. Every table has columns
# that must not be null. Implemented as a pure function that takes a DataFrame
# and config, returns a ValidationResult.

# TRADEOFF: Could use df.na.drop() to filter nulls, but that modifies the
# DataFrame. We use .filter(col.isNull()) to SELECT bad records without
# modifying the input. Non-destructive validation is a core principle.

# FAILURE MODE: If column_name doesn't exist in the DataFrame, PySpark throws
# AnalysisException. We catch this and return a failed result with metadata
# explaining the column is missing. This prevents pipeline crashes.

# INTERVIEW: "Why is not-null checking separate from schema enforcement?"
# → "Schema enforcement prevents null values from being written. Not-null
#    checking DETECTS nulls in existing data. Different use cases: enforcement
#    is a gate, checking is observability. You need both."

# SCALE: O(n) single-pass filter. At 10K → instant. At 1B → ~30 seconds.
# No shuffle required — embarrassingly parallel across partitions.
"""

import time
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.rule_configs import NotNullRule
from rules.validation_result import ValidationResult


def not_null_check(
    df: DataFrame,
    rule: NotNullRule,
    dataset_name: str = "",
) -> ValidationResult:
    """
    Check that a column has no null values.
    
    Args:
        df: DataFrame to validate
        rule: NotNullRule configuration
        dataset_name: Name of the dataset being validated
    
    Returns:
        ValidationResult with failed records (rows where column is null)
    """
    start_time = time.time()
    
    # Guard: check column exists
    if rule.column_name not in df.columns:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="not_null_check",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            total_records=0,
            failed_records=0,
            failure_rate=0.0,
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={
                "error": f"Column '{rule.column_name}' does not exist in DataFrame",
                "available_columns": df.columns,
            },
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    # Count total records
    total = df.count()
    
    # Find null records
    # DECISION: Use isNull() not == None. In PySpark, col == None evaluates
    # differently than col.isNull(). Always use isNull() for null checks.
    failed_df = df.filter(F.col(rule.column_name).isNull())
    failed_count = failed_df.count()
    
    # Calculate failure rate
    failure_rate = failed_count / total if total > 0 else 0.0
    
    # Determine pass/fail
    passed = failed_count == 0
    
    elapsed_ms = (time.time() - start_time) * 1000
    
    return ValidationResult(
        rule_id=rule.rule_id,
        rule_name="not_null_check",
        rule_version=rule.rule_version,
        passed=passed,
        severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
        total_records=total,
        failed_records=failed_count,
        failure_rate=failure_rate,
        failed_df=failed_df if failed_count > 0 else None,
        dataset_name=dataset_name,
        layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
        metadata={
            "column_name": rule.column_name,
            "null_count": failed_count,
            "null_rate": failure_rate,
        },
        execution_time_ms=elapsed_ms,
    )
