"""
Rule 3: Accepted Values Check

# DECISION: Uses .isin() for small value sets (<100 values) and anti-join
# for large reference tables. The rule config determines which approach.

# TRADEOFF: Could use a single approach (always join). isin() is faster
# for small sets because it avoids shuffle — the values are broadcast as
# a literal array. At >100 values, the broadcast overhead exceeds join cost.

# FAILURE MODE: Case sensitivity. If config says accepted=["USD","EUR"]
# and data has "usd", the check fails. The case_sensitive config flag
# controls this. Default is case-insensitive because production data is messy.

# INTERVIEW: "When does isin() become less efficient than a join?"
# → "Around 100-1000 values. isin() broadcasts the value list to every
#    partition; at thousands of values, the broadcast payload becomes large.
#    A reference table join with broadcast hint is more efficient — Spark
#    optimizes the broadcast at the executor level."

# SCALE: isin() with 10 values → O(n*10). Join with broadcast → O(n+m).
# At 1B rows with 10 values → isin() is fine. At 1B rows with 10K values
# → use broadcast join.
"""

import time
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.rule_configs import AcceptedValuesRule
from rules.validation_result import ValidationResult


def accepted_values_check(
    df: DataFrame,
    rule: AcceptedValuesRule,
    dataset_name: str = "",
    reference_df: DataFrame = None,
) -> ValidationResult:
    """
    Check that column values are within an accepted set.
    
    Args:
        df: DataFrame to validate
        rule: AcceptedValuesRule configuration
        dataset_name: Name of the dataset being validated
        reference_df: Optional reference DataFrame for large value sets
    
    Returns:
        ValidationResult with records containing invalid values
    """
    start_time = time.time()
    
    # Guard: check column exists
    if rule.column_name not in df.columns:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="accepted_values_check",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={"error": f"Column '{rule.column_name}' not found"},
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    total = df.count()
    
    # Handle case sensitivity
    check_col = rule.column_name
    if not rule.case_sensitive:
        df_check = df.withColumn("_check_val", F.lower(F.col(check_col)))
        accepted = [v.lower() for v in rule.accepted_values]
        check_col_expr = F.col("_check_val")
    else:
        df_check = df
        accepted = rule.accepted_values
        check_col_expr = F.col(check_col)
    
    # Find invalid values
    # DECISION: Use ~isin() with null handling. isin() returns null for null
    # input, and ~null = null (not True). We handle nulls separately to avoid
    # counting nulls as invalid values (that's the not_null_check's job).
    invalid_df = df_check.filter(
        check_col_expr.isNotNull() & ~check_col_expr.isin(accepted)
    )
    
    failed_count = invalid_df.count()
    
    # Get the actual failed records from original DataFrame (without temp columns)
    if failed_count > 0 and "_check_val" in invalid_df.columns:
        failed_df = invalid_df.drop("_check_val")
    elif failed_count > 0:
        failed_df = invalid_df
    else:
        failed_df = None
    
    # Collect unique invalid values for metadata (limit to prevent OOM)
    # DECISION: Collect top 20 invalid values for debugging visibility.
    # Don't collect all — at 1B rows, millions of unique invalid values
    # would crash the driver.
    invalid_values_sample = []
    if failed_count > 0:
        sample = (
            invalid_df.select(F.col(rule.column_name))
            .distinct()
            .limit(20)
            .collect()
        )
        invalid_values_sample = [row[0] for row in sample]
    
    failure_rate = failed_count / total if total > 0 else 0.0
    passed = failed_count == 0
    
    elapsed_ms = (time.time() - start_time) * 1000
    
    return ValidationResult(
        rule_id=rule.rule_id,
        rule_name="accepted_values_check",
        rule_version=rule.rule_version,
        passed=passed,
        severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
        total_records=total,
        failed_records=failed_count,
        failure_rate=failure_rate,
        failed_df=failed_df,
        dataset_name=dataset_name,
        layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
        metadata={
            "column_name": rule.column_name,
            "accepted_values": accepted,
            "invalid_values_sample": invalid_values_sample,
            "case_sensitive": rule.case_sensitive,
        },
        execution_time_ms=elapsed_ms,
    )
