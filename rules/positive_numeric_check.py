"""
Rule 4: Positive Numeric Check

# DECISION: Simple but critical for financial data. Negative prices or
# quantities can cascade into incorrect revenue calculations, refund errors,
# and inventory discrepancies.

# TRADEOFF: Could combine with a general range check (min_value, max_value).
# Kept positive-only for clarity — range checks are a different rule.

# FAILURE MODE: If column is StringType but contains numeric-looking values,
# the check silently passes (strings are never <= 0). Guard against this
# by checking the column type first.

# INTERVIEW: "How do you handle edge cases in numeric validation?"
# → "Three guards: (1) verify column exists, (2) verify column is numeric
#    type, (3) handle nulls separately. Nulls are not negative — they're
#    missing. Different rules for different problems."

# SCALE: O(n) filter — same as not_null_check. No shuffle.
"""

import time
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType

from config.rule_configs import PositiveNumericRule
from rules.validation_result import ValidationResult


def positive_numeric_check(
    df: DataFrame,
    rule: PositiveNumericRule,
    dataset_name: str = "",
) -> ValidationResult:
    """
    Check that a numeric column contains only positive values.
    
    Args:
        df: DataFrame to validate
        rule: PositiveNumericRule configuration
        dataset_name: Name of dataset being validated
    
    Returns:
        ValidationResult with records containing non-positive values
    """
    start_time = time.time()
    
    # Guard: check column exists
    if rule.column_name not in df.columns:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="positive_numeric_check",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={"error": f"Column '{rule.column_name}' not found"},
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    # Guard: check column is numeric
    col_type = dict(df.dtypes).get(rule.column_name)
    numeric_types = {"int", "bigint", "smallint", "tinyint", "float", "double", "decimal"}
    if col_type not in numeric_types:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="positive_numeric_check",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={
                "error": f"Column '{rule.column_name}' is {col_type}, not numeric",
            },
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    total = df.count()
    
    # Filter for non-positive values (exclude nulls — that's not_null_check's job)
    col = F.col(rule.column_name)
    if rule.allow_zero:
        failed_df = df.filter(col.isNotNull() & (col < 0))
    else:
        failed_df = df.filter(col.isNotNull() & (col <= 0))
    
    failed_count = failed_df.count()
    failure_rate = failed_count / total if total > 0 else 0.0
    passed = failed_count == 0
    
    # Collect summary stats for metadata
    stats = {}
    if failed_count > 0:
        stats_row = failed_df.agg(
            F.min(rule.column_name).alias("min_value"),
            F.max(rule.column_name).alias("max_value"),
            F.avg(rule.column_name).alias("avg_value"),
        ).collect()[0]
        stats = {
            "min_failing_value": float(stats_row["min_value"]) if stats_row["min_value"] else None,
            "max_failing_value": float(stats_row["max_value"]) if stats_row["max_value"] else None,
            "avg_failing_value": float(stats_row["avg_value"]) if stats_row["avg_value"] else None,
        }
    
    elapsed_ms = (time.time() - start_time) * 1000
    
    return ValidationResult(
        rule_id=rule.rule_id,
        rule_name="positive_numeric_check",
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
            "allow_zero": rule.allow_zero,
            "negative_count": failed_count,
            **stats,
        },
        execution_time_ms=elapsed_ms,
    )
