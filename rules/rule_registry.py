"""
Rule Registry — discovers, configures, and executes validation rules.

# DECISION: Registry pattern for rule discovery. Instead of hardcoding
# "run not_null_check, then unique_key_check, then..." we register rules
# in a map and execute them by config. This is the plugin architecture:
# adding a new rule = implement function + register. No existing code changes.

# TRADEOFF: Could use Python decorators for auto-registration (more magical).
# Chose explicit registration for clarity — you can read the registry and 
# see every rule in one place.

# FAILURE MODE: If a rule function signature doesn't match what the registry
# expects, it fails at runtime, not at import time. Mitigation: each rule
# follows the ValidationFunction protocol (df, rule, dataset_name) → Result.

# INTERVIEW: "How do you add a new validation rule to your framework?"
# → "Three steps: (1) define the rule config dataclass, (2) implement the
#    PySpark function, (3) register in the rule registry. No existing code
#    changes needed. This is the Open/Closed Principle — open for extension,
#    closed for modification."

# SCALE: Registry overhead is negligible — it's a dict lookup. The bottleneck
# is always the rule execution itself, not the dispatch.
"""

import time
from typing import Dict, Callable, List, Any, Optional
from dataclasses import dataclass

from pyspark.sql import DataFrame

from config.rule_configs import (
    BaseRuleConfig, NotNullRule, UniqueKeyRule, AcceptedValuesRule,
    PositiveNumericRule, TimestampFreshnessRule, DuplicateDetectionRule,
    SchemaDriftRule, ReferentialIntegrityRule, RowCountAnomalyRule,
    DistributionAnomalyRule,
)
from rules.validation_result import ValidationResult
from rules.not_null_check import not_null_check
from rules.unique_key_check import unique_key_check
from rules.accepted_values_check import accepted_values_check
from rules.positive_numeric_check import positive_numeric_check
from rules.timestamp_freshness_check import timestamp_freshness_check
from rules.duplicate_detection import duplicate_detection
from rules.schema_drift_detection import schema_drift_detection
from rules.referential_integrity_check import referential_integrity_check
from rules.row_count_anomaly_detection import row_count_anomaly_detection
from rules.distribution_anomaly_check import distribution_anomaly_check


# =============================================================================
# RULE TYPE REGISTRY
# =============================================================================

# Maps rule config type → execution function
# DECISION: Type-based dispatch instead of string-based. Type checking at
# registration prevents "typo in rule name" bugs.

RULE_REGISTRY: Dict[type, Callable] = {
    NotNullRule: not_null_check,
    UniqueKeyRule: unique_key_check,
    AcceptedValuesRule: accepted_values_check,
    PositiveNumericRule: positive_numeric_check,
    TimestampFreshnessRule: timestamp_freshness_check,
    DuplicateDetectionRule: duplicate_detection,
    SchemaDriftRule: schema_drift_detection,
    # Note: ReferentialIntegrityRule and RowCountAnomalyRule require extra args
    # They are handled specially in execute_rules()
}


def execute_rule(
    df: DataFrame,
    rule: BaseRuleConfig,
    dataset_name: str = "",
    **kwargs,
) -> ValidationResult:
    """
    Execute a single validation rule.
    
    # DECISION: Wrap execution in try/except to convert crashes into failed
    # results. A single rule crash should never kill the entire validation
    # pipeline.
    
    Args:
        df: DataFrame to validate
        rule: Rule configuration (any subclass of BaseRuleConfig)
        dataset_name: Name of dataset
        **kwargs: Extra arguments for rules that need them
                 (parent_df for ref integrity, historical_counts for row count, etc.)
    
    Returns:
        ValidationResult (always — even on exception)
    """
    if not rule.enabled:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name=f"SKIPPED_{type(rule).__name__}",
            rule_version=rule.rule_version,
            passed=True,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={"skipped": True, "reason": "Rule is disabled"},
        )
    
    try:
        rule_type = type(rule)
        
        # Special handling for rules with extra arguments
        if isinstance(rule, ReferentialIntegrityRule):
            parent_df = kwargs.get("parent_df")
            if parent_df is None:
                return ValidationResult(
                    rule_id=rule.rule_id,
                    rule_name="referential_integrity_check",
                    rule_version=rule.rule_version,
                    passed=False,
                    severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
                    dataset_name=dataset_name,
                    layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
                    metadata={"error": "parent_df not provided for referential integrity check"},
                )
            return referential_integrity_check(df, rule, parent_df, dataset_name)
        
        elif isinstance(rule, RowCountAnomalyRule):
            historical_counts = kwargs.get("historical_counts", [])
            return row_count_anomaly_detection(df, rule, historical_counts, dataset_name)
        
        elif isinstance(rule, DistributionAnomalyRule):
            historical_stats = kwargs.get("historical_stats")
            return distribution_anomaly_check(df, rule, historical_stats, dataset_name)
        
        # Standard rules — simple dispatch
        elif rule_type in RULE_REGISTRY:
            func = RULE_REGISTRY[rule_type]
            return func(df, rule, dataset_name)
        
        else:
            return ValidationResult(
                rule_id=rule.rule_id,
                rule_name=f"UNKNOWN_{type(rule).__name__}",
                rule_version=rule.rule_version,
                passed=False,
                severity="warning",
                dataset_name=dataset_name,
                metadata={"error": f"No handler registered for rule type: {rule_type}"},
            )
    
    except Exception as e:
        # DECISION: Catch ALL exceptions and return a failed result.
        # This is intentional — a validation framework must never crash.
        # Log the error, report it as a failed rule, continue.
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name=f"ERROR_{type(rule).__name__}",
            rule_version=rule.rule_version,
            passed=False,
            severity="critical",
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={
                "error": str(e),
                "error_type": type(e).__name__,
                "note": "Rule execution failed — this is a framework error, not a data error",
            },
        )


def execute_rules(
    df: DataFrame,
    rules: List[BaseRuleConfig],
    dataset_name: str = "",
    **kwargs,
) -> List[ValidationResult]:
    """
    Execute multiple validation rules and collect results.
    
    # DECISION: Execute rules sequentially, not in parallel. Parallel execution
    # would require submitting multiple Spark jobs simultaneously, which
    # contends for cluster resources and complicates error handling.
    # Sequential execution is simpler and sufficient for <100 rules.
    
    # SCALE: At 10 rules → ~30 seconds total. At 100 rules → ~5 minutes.
    # If this becomes a bottleneck, parallelize using Python ThreadPoolExecutor
    # (Spark jobs can run concurrently from different threads).
    
    Args:
        df: DataFrame to validate
        rules: List of rule configurations
        dataset_name: Name of dataset
        **kwargs: Extra arguments passed to individual rules
    
    Returns:
        List of ValidationResult objects
    """
    results = []
    total_start = time.time()
    
    enabled_rules = [r for r in rules if r.enabled]
    skipped_rules = [r for r in rules if not r.enabled]
    
    print(f"\n{'='*60}")
    print(f"VALIDATION ENGINE — {dataset_name}")
    print(f"{'='*60}")
    print(f"Rules to execute: {len(enabled_rules)} (skipped: {len(skipped_rules)})")
    print(f"{'='*60}")
    
    for i, rule in enumerate(rules, 1):
        result = execute_rule(df, rule, dataset_name, **kwargs)
        results.append(result)
        
        # Print summary
        status = "✅" if result.passed else "❌"
        print(f"  [{i}/{len(rules)}] {status} {result.rule_name} "
              f"({result.rule_id}) — {result.execution_time_ms:.0f}ms")
        if not result.passed and result.metadata:
            error = result.metadata.get("error", "")
            if error:
                print(f"         Error: {error}")
    
    total_elapsed = (time.time() - total_start) * 1000
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)
    
    print(f"\n{'='*60}")
    print(f"SUMMARY: {passed} passed, {failed} failed ({total_elapsed:.0f}ms total)")
    print(f"{'='*60}\n")
    
    return results
