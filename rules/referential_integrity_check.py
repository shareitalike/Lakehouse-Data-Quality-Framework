"""
Rule 8: Referential Integrity Check

# DECISION: Uses left anti-join to find records in the child table whose
# foreign key doesn't exist in the parent table. Anti-join is the most
# efficient Spark pattern for this — it returns only non-matching rows
# without duplicating data.

# TRADEOFF: Could use NOT IN subquery (SQL style), but NOT IN has dangerous
# behavior with nulls — if the parent column contains ANY null, NOT IN
# returns empty results for ALL rows. Anti-join handles nulls correctly.

# FAILURE MODE: If the parent table is stale (not updated recently), valid
# new foreign keys are incorrectly flagged as FK violations. The child table
# references a new product that exists but the reference table hasn't been
# refreshed.
# Mitigation: always read parent table fresh, not from cache.

# INTERVIEW: "Why anti-join instead of NOT IN?"
# → "NOT IN has the null trap. If any value in the parent column is null,
#    the NOT IN check returns false for EVERY row. Anti-join handles nulls
#    correctly and is also more efficient — Spark can broadcast the smaller
#    table and avoid a full shuffle."

# SCALE: At 10K child + 100 parent → broadcast join, instant.
# At 10M child + 10K parent → broadcast join, ~5 seconds.
# At 1B child + 10M parent → shuffle join, ~2 minutes.
# Optimization: if parent fits in memory (<1GB), use broadcast hint.
"""

import time
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.rule_configs import ReferentialIntegrityRule
from rules.validation_result import ValidationResult


def referential_integrity_check(
    df: DataFrame,
    rule: ReferentialIntegrityRule,
    parent_df: DataFrame,
    dataset_name: str = "",
) -> ValidationResult:
    """
    Check that all foreign key values in child table exist in parent table.
    
    Args:
        df: Child DataFrame to validate 
        rule: ReferentialIntegrityRule configuration
        parent_df: Parent/reference DataFrame containing valid values
        dataset_name: Name of dataset being validated
    
    Returns:
        ValidationResult with orphan records (FK violations)
    """
    start_time = time.time()
    
    # Guard: check columns exist
    if rule.child_column not in df.columns:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="referential_integrity_check",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={"error": f"Child column '{rule.child_column}' not found"},
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    if rule.parent_column not in parent_df.columns:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="referential_integrity_check",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={"error": f"Parent column '{rule.parent_column}' not found"},
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    total = df.count()
    parent_count = parent_df.count()
    
    # Left anti-join: find child records with no matching parent
    # DECISION: Anti-join instead of left join + filter IS NULL.
    # Anti-join generates a more efficient physical plan because Spark
    # knows it only needs to find non-matches, not carry full payloads.
    
    # Handle column name mismatch: rename parent column to match child
    parent_key = parent_df.select(
        F.col(rule.parent_column).alias(rule.child_column)
    ).distinct()
    
    # Apply broadcast hint if parent is small
    # DECISION: Auto-broadcast if parent < 10K rows. This avoids shuffle
    # join for the common case where the reference table is small.
    if parent_count < 10000:
        parent_key = F.broadcast(parent_key)
    
    orphans_df = df.join(
        parent_key,
        on=rule.child_column,
        how="left_anti"
    )
    
    # Exclude nulls from orphan count — null FK is a not_null issue, not FK issue
    # DECISION: Separate concerns. Null FK → not_null_check. Invalid FK → here.
    orphans_df = orphans_df.filter(F.col(rule.child_column).isNotNull())
    
    orphan_count = orphans_df.count()
    failure_rate = orphan_count / total if total > 0 else 0.0
    passed = orphan_count == 0
    
    # Sample invalid FK values for debugging
    invalid_fk_sample = []
    if orphan_count > 0:
        sample = (
            orphans_df.select(rule.child_column)
            .distinct()
            .limit(20)
            .collect()
        )
        invalid_fk_sample = [row[0] for row in sample]
    
    elapsed_ms = (time.time() - start_time) * 1000
    
    return ValidationResult(
        rule_id=rule.rule_id,
        rule_name="referential_integrity_check",
        rule_version=rule.rule_version,
        passed=passed,
        severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
        total_records=total,
        failed_records=orphan_count,
        failure_rate=failure_rate,
        failed_df=orphans_df if orphan_count > 0 else None,
        dataset_name=dataset_name,
        layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
        metadata={
            "child_column": rule.child_column,
            "parent_column": rule.parent_column,
            "parent_record_count": parent_count,
            "orphan_count": orphan_count,
            "invalid_fk_sample": invalid_fk_sample,
            "used_broadcast": parent_count < 10000,
        },
        execution_time_ms=elapsed_ms,
    )
