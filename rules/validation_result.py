"""
Validation result dataclass — standardized output for all rules.

# DECISION: Every rule returns the same ValidationResult structure regardless
# of what it checks. This standardization enables: (1) uniform metrics collection,
# (2) consistent quarantine routing, (3) pluggable rule architecture where the
# engine doesn't need to know what each rule does internally.

# TRADEOFF: Could return different result types per rule (e.g., SchemaDriftResult
# with added_columns field). Chose uniform results because: (1) the engine
# treats all results identically, (2) rule-specific details go in metadata dict,
# (3) uniform types simplify the observability layer.

# FAILURE MODE: If a rule throws an exception instead of returning a result,
# the entire validation pipeline breaks. The rule_registry wraps each rule
# in a try/except to convert exceptions into failed ValidationResults.

# INTERVIEW: "Why do all your rules return the same type?"
# → "Uniform interface. The validation engine doesn't care if it's checking
#    nulls or schema drift — it processes ValidationResults generically.
#    This is the Strategy pattern: different algorithms, same interface."
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional
from datetime import datetime

from pyspark.sql import DataFrame


@dataclass
class ValidationResult:
    """
    Standardized result from any validation rule execution.
    
    # SCALE: This object itself is tiny (Python object, not distributed).
    # The heavy item is failed_df, which is a lazy DataFrame reference —
    # it's not materialized until someone calls .count() or .write().
    # At 1B rows, the result object is still a few KB; the failed_df
    # might reference millions of rows but only materializes on demand.
    """
    # Rule identification
    rule_id: str
    rule_name: str
    rule_version: str = "1.0"
    
    # Result
    passed: bool = True
    severity: str = "warning"
    
    # Metrics
    total_records: int = 0
    failed_records: int = 0
    failure_rate: float = 0.0
    
    # Failed records for quarantine
    failed_df: Optional[DataFrame] = None
    # DECISION: Optional[DataFrame] not DataFrame. Some rules (like freshness)
    # don't produce per-record failures — they're aggregate checks.
    # None means "this rule doesn't identify individual bad records."
    
    # Rule-specific metadata (flexible)
    metadata: Dict[str, Any] = field(default_factory=dict)
    # Examples:
    # - schema_drift: {"added_columns": [...], "removed_columns": [...]}
    # - distribution: {"current_mean": 50.0, "historical_mean": 48.0, "z_score": 1.2}
    # - freshness: {"max_age_hours": 5.2, "sla_hours": 4.0}
    
    # Execution context
    dataset_name: str = ""
    layer: str = ""
    run_timestamp: str = field(
        default_factory=lambda: datetime.utcnow().isoformat()
    )
    execution_time_ms: float = 0.0
    
    @property
    def pass_rate(self) -> float:
        """Percentage of records that passed."""
        if self.total_records == 0:
            return 100.0
        return ((self.total_records - self.failed_records) / self.total_records) * 100.0
    
    def summary(self) -> str:
        """Human-readable summary of the result."""
        status = "✅ PASSED" if self.passed else "❌ FAILED"
        return (
            f"{status} | {self.rule_name} ({self.rule_id})\n"
            f"  Records: {self.total_records:,} total, {self.failed_records:,} failed "
            f"({self.failure_rate:.2%})\n"
            f"  Severity: {self.severity} | Layer: {self.layer}\n"
            f"  Execution: {self.execution_time_ms:.0f}ms"
        )
    
    def to_metrics_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for observability metrics storage.
        
        # DECISION: Separate serialization method instead of __dict__
        # because we exclude the failed_df (not serializable) and add
        # computed fields like pass_rate.
        """
        return {
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "rule_version": self.rule_version,
            "passed": self.passed,
            "severity": self.severity,
            "total_records": self.total_records,
            "failed_records": self.failed_records,
            "failure_rate": self.failure_rate,
            "pass_rate": self.pass_rate,
            "dataset_name": self.dataset_name,
            "layer": self.layer,
            "run_timestamp": self.run_timestamp,
            "execution_time_ms": self.execution_time_ms,
            "metadata": str(self.metadata),
        }
