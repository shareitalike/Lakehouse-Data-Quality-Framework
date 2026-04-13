"""
Contract Enforcer — validates DataFrames against data contracts.

# DECISION: A data contract is the formal agreement between data producers
# and consumers. The contract enforcer checks that data meets ALL contract
# terms: schema, quality, and SLA. It's the "integration test" for data.

# TRADEOFF: Could rely entirely on individual rules (not_null, uniqueness, etc.)
# without a contract abstraction. Contracts add value because: (1) they bundle
# related rules into a coherent agreement, (2) they're reviewable by non-engineers
# ("this table should have <1% nulls"), (3) they can be versioned and published.

# FAILURE MODE: Contract enforcement at write-time can reject valid data if
# the contract is too strict or outdated. Always test contract changes against
# recent data before deploying.

# INTERVIEW: "What's the relationship between a data contract and data quality rules?"
# → "A contract is the WHAT (expectations). Rules are the HOW (implementation).
#    A contract says 'customer_id must never be null.' The not_null_check rule
#    implements that expectation. Contracts are for communication; rules are for
#    execution."

# SCALE: Contract enforcement overhead is the sum of its constituent rule
# executions. A contract with 10 rules = 10 rule executions.
"""

from typing import Dict, List, Optional, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from config.layer_schemas import DataContract
from config.rule_configs import (
    NotNullRule, UniqueKeyRule, TimestampFreshnessRule,
    Severity, Layer,
)
from rules.validation_result import ValidationResult
from rules.rule_registry import execute_rules
from engine.validation_engine import ValidationEngine


class ContractEnforcer:
    """
    Validates DataFrames against data contracts.
    
    A data contract defines what the data SHOULD look like.
    The enforcer checks if the data ACTUALLY matches.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def check_schema_contract(
        self,
        df: DataFrame,
        contract: DataContract,
    ) -> Dict[str, Any]:
        """
        Validate DataFrame schema against contract schema.
        
        Returns dict with:
        - passed: bool
        - missing_columns: list
        - extra_columns: list
        - type_mismatches: list
        """
        expected_fields = {f.name: f for f in contract.schema.fields}
        actual_fields = {f.name: f for f in df.schema.fields}
        
        missing = [n for n in expected_fields if n not in actual_fields]
        extra = [n for n in actual_fields if n not in expected_fields]
        
        type_mismatches = []
        for name in set(expected_fields) & set(actual_fields):
            expected_type = expected_fields[name].dataType.simpleString()
            actual_type = actual_fields[name].dataType.simpleString()
            if expected_type != actual_type:
                type_mismatches.append({
                    "column": name,
                    "expected": expected_type,
                    "actual": actual_type,
                })
        
        # Schema contract passes if:
        # - No missing columns
        # - No type mismatches
        # - Extra columns allowed if contract says so
        passed = (
            len(missing) == 0 and
            len(type_mismatches) == 0 and
            (len(extra) == 0 or contract.allow_schema_evolution)
        )
        
        return {
            "passed": passed,
            "missing_columns": missing,
            "extra_columns": extra,
            "type_mismatches": type_mismatches,
            "enforce_schema": contract.enforce_schema,
            "allow_evolution": contract.allow_schema_evolution,
        }
    
    def check_quality_contract(
        self,
        df: DataFrame,
        contract: DataContract,
    ) -> Dict[str, Any]:
        """
        Validate DataFrame quality metrics against contract thresholds.
        """
        results = {}
        total = df.count()
        
        # Check not-null rates
        null_results = {}
        for col_name, min_rate in contract.min_not_null_rate.items():
            if col_name in df.columns:
                null_count = df.filter(F.col(col_name).isNull()).count()
                actual_rate = 1 - (null_count / total) if total > 0 else 1.0
                null_results[col_name] = {
                    "min_required": min_rate,
                    "actual": round(actual_rate, 4),
                    "passed": actual_rate >= min_rate,
                }
        results["not_null_rates"] = null_results
        
        # Check duplicate rate
        if contract.primary_key_columns:
            pk_cols = [c for c in contract.primary_key_columns if c in df.columns]
            if pk_cols:
                dup_groups = (
                    df.groupBy(pk_cols)
                    .count()
                    .filter(F.col("count") > 1)
                )
                dup_count = dup_groups.count()
                dup_rate = dup_count / total if total > 0 else 0.0
                results["duplicate_rate"] = {
                    "max_allowed": contract.max_duplicate_rate,
                    "actual": round(dup_rate, 4),
                    "passed": dup_rate <= contract.max_duplicate_rate,
                }
        
        # Check row count bounds
        results["row_count"] = {
            "actual": total,
            "min_required": contract.min_row_count,
            "max_allowed": contract.max_row_count,
            "passed": (
                total >= contract.min_row_count and
                (contract.max_row_count is None or total <= contract.max_row_count)
            ),
        }
        
        # Overall quality pass/fail
        all_passed = all(
            v.get("passed", True)
            for v in results.values()
            if isinstance(v, dict) and "passed" in v
        )
        
        # Check nested null results
        if "not_null_rates" in results:
            for col_result in results["not_null_rates"].values():
                if not col_result.get("passed", True):
                    all_passed = False
        
        results["overall_passed"] = all_passed
        return results
    
    def enforce_contract(
        self,
        df: DataFrame,
        contract: DataContract,
    ) -> Dict[str, Any]:
        """
        Full contract enforcement — schema + quality + SLA.
        
        # DECISION: Run ALL checks even if early checks fail.
        # This gives a complete picture of contract violations in one pass.
        # Fail-fast saves time but provides less information.
        
        Returns comprehensive contract validation report.
        """
        print(f"\n{'='*60}")
        print(f"CONTRACT ENFORCEMENT — {contract.dataset_name} (v{contract.version})")
        print(f"{'='*60}")
        
        # Schema check
        schema_result = self.check_schema_contract(df, contract)
        status = "✅" if schema_result["passed"] else "❌"
        print(f"  Schema Contract:  {status}")
        if not schema_result["passed"]:
            if schema_result["missing_columns"]:
                print(f"    Missing: {schema_result['missing_columns']}")
            if schema_result["type_mismatches"]:
                print(f"    Type changes: {schema_result['type_mismatches']}")
        
        # Quality check
        quality_result = self.check_quality_contract(df, contract)
        status = "✅" if quality_result["overall_passed"] else "❌"
        print(f"  Quality Contract: {status}")
        
        # Overall
        overall_passed = schema_result["passed"] and quality_result["overall_passed"]
        status = "✅ PASSED" if overall_passed else "❌ FAILED"
        print(f"  Overall: {status}")
        print(f"{'='*60}\n")
        
        return {
            "contract_name": contract.dataset_name,
            "contract_version": contract.version,
            "layer": contract.layer,
            "overall_passed": overall_passed,
            "schema": schema_result,
            "quality": quality_result,
        }
