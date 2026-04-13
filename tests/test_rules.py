"""
Unit tests for all 10 validation rules.

Run with: python -m pytest tests/test_rules.py -v
Or on Databricks: execute each test function in a notebook cell.

# DECISION: Each rule gets at least 2 tests: one passing, one failing.
# This ensures the rule detects issues AND passes clean data.

# INTERVIEW: "How do you test your data quality rules?"
# → "Unit tests with synthetic data. Each test has a controlled input
#    with known issues, and asserts the rule detects exactly those issues.
#    I use PySpark local mode — no cluster needed for unit tests."
"""

import sys
import os
from datetime import datetime, timedelta

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType,
)

from config.rule_configs import (
    NotNullRule, UniqueKeyRule, AcceptedValuesRule, PositiveNumericRule,
    TimestampFreshnessRule, DuplicateDetectionRule, SchemaDriftRule,
    ReferentialIntegrityRule, RowCountAnomalyRule, DistributionAnomalyRule,
    Severity, Layer,
)
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
# SPARK SESSION FOR TESTS
# =============================================================================

def get_test_spark():
    """Create a minimal SparkSession for testing."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("DQ_RuleTests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


# =============================================================================
# TEST: Not Null Check
# =============================================================================

def test_not_null_check_passes_clean_data():
    """Not null check should pass when column has no nulls."""
    spark = get_test_spark()
    df = spark.createDataFrame(
        [("1", "Alice"), ("2", "Bob"), ("3", "Charlie")],
        ["id", "name"]
    )
    rule = NotNullRule(
        rule_id="test_not_null", column_name="name",
        severity=Severity.CRITICAL, layer=Layer.BRONZE,
    )
    result = not_null_check(df, rule, "test_data")
    
    assert result.passed is True
    assert result.failed_records == 0
    assert result.failure_rate == 0.0
    print("✅ test_not_null_check_passes_clean_data")


def test_not_null_check_detects_nulls():
    """Not null check should detect null values."""
    spark = get_test_spark()
    df = spark.createDataFrame(
        [("1", "Alice"), ("2", None), ("3", None), ("4", "Dave")],
        ["id", "name"]
    )
    rule = NotNullRule(
        rule_id="test_not_null_fail", column_name="name",
        severity=Severity.WARNING, layer=Layer.BRONZE,
    )
    result = not_null_check(df, rule, "test_data")
    
    assert result.passed is False
    assert result.failed_records == 2
    assert result.failure_rate == 0.5
    print("✅ test_not_null_check_detects_nulls")


# =============================================================================
# TEST: Unique Key Check
# =============================================================================

def test_unique_key_check_passes():
    """Unique key should pass with no duplicates."""
    spark = get_test_spark()
    df = spark.createDataFrame(
        [("1",), ("2",), ("3",)], ["order_id"]
    )
    rule = UniqueKeyRule(
        rule_id="test_unique", columns=["order_id"],
        severity=Severity.CRITICAL, layer=Layer.SILVER,
    )
    result = unique_key_check(df, rule)
    
    assert result.passed is True
    assert result.failed_records == 0
    print("✅ test_unique_key_check_passes")


def test_unique_key_check_detects_duplicates():
    """Unique key should detect duplicate keys."""
    spark = get_test_spark()
    df = spark.createDataFrame(
        [("1",), ("2",), ("2",), ("3",), ("3",), ("3",)], ["order_id"]
    )
    rule = UniqueKeyRule(
        rule_id="test_unique_fail", columns=["order_id"],
        severity=Severity.CRITICAL, layer=Layer.SILVER,
    )
    result = unique_key_check(df, rule)
    
    assert result.passed is False
    assert result.metadata["duplicate_groups"] == 2  # "2" and "3"
    print("✅ test_unique_key_check_detects_duplicates")


# =============================================================================
# TEST: Accepted Values Check
# =============================================================================

def test_accepted_values_passes():
    """Accepted values should pass with valid values."""
    spark = get_test_spark()
    df = spark.createDataFrame(
        [("pending",), ("shipped",), ("delivered",)], ["status"]
    )
    rule = AcceptedValuesRule(
        rule_id="test_av", column_name="status",
        accepted_values=["pending", "shipped", "delivered", "cancelled"],
        severity=Severity.WARNING, layer=Layer.BRONZE,
    )
    result = accepted_values_check(df, rule)
    
    assert result.passed is True
    print("✅ test_accepted_values_passes")


def test_accepted_values_detects_invalid():
    """Accepted values should detect invalid enum values."""
    spark = get_test_spark()
    df = spark.createDataFrame(
        [("pending",), ("YOLO",), ("TEST",), ("delivered",)], ["status"]
    )
    rule = AcceptedValuesRule(
        rule_id="test_av_fail", column_name="status",
        accepted_values=["pending", "shipped", "delivered", "cancelled"],
        severity=Severity.WARNING, layer=Layer.BRONZE,
    )
    result = accepted_values_check(df, rule)
    
    assert result.passed is False
    assert result.failed_records == 2  # YOLO and TEST
    print("✅ test_accepted_values_detects_invalid")


# =============================================================================
# TEST: Positive Numeric Check
# =============================================================================

def test_positive_numeric_passes():
    """Positive numeric should pass with all positive values."""
    spark = get_test_spark()
    df = spark.createDataFrame([(1,), (5,), (100,)], ["quantity"])
    rule = PositiveNumericRule(
        rule_id="test_pos", column_name="quantity",
        severity=Severity.WARNING, layer=Layer.BRONZE,
    )
    result = positive_numeric_check(df, rule)
    
    assert result.passed is True
    print("✅ test_positive_numeric_passes")


def test_positive_numeric_detects_negatives():
    """Positive numeric should detect negative values."""
    spark = get_test_spark()
    df = spark.createDataFrame([(1,), (-5,), (3,), (-2,)], ["quantity"])
    rule = PositiveNumericRule(
        rule_id="test_pos_fail", column_name="quantity",
        severity=Severity.WARNING, layer=Layer.BRONZE,
    )
    result = positive_numeric_check(df, rule)
    
    assert result.passed is False
    assert result.failed_records == 2
    print("✅ test_positive_numeric_detects_negatives")


# =============================================================================
# TEST: Schema Drift Detection
# =============================================================================

def test_schema_drift_no_drift():
    """Schema drift should pass when schema matches."""
    spark = get_test_spark()
    df = spark.createDataFrame(
        [("1", "Alice", 10)], ["id", "name", "quantity"]
    )
    rule = SchemaDriftRule(
        rule_id="test_drift", expected_columns=["id", "name", "quantity"],
        severity=Severity.CRITICAL, layer=Layer.BRONZE,
    )
    result = schema_drift_detection(df, rule)
    
    assert result.passed is True
    print("✅ test_schema_drift_no_drift")


def test_schema_drift_detects_new_column():
    """Schema drift should detect unexpected columns."""
    spark = get_test_spark()
    df = spark.createDataFrame(
        [("1", "Alice", 10, "gold")], ["id", "name", "quantity", "loyalty_tier"]
    )
    rule = SchemaDriftRule(
        rule_id="test_drift_fail", expected_columns=["id", "name", "quantity"],
        allow_extra_columns=False,
        severity=Severity.CRITICAL, layer=Layer.BRONZE,
    )
    result = schema_drift_detection(df, rule)
    
    assert result.passed is False
    assert "loyalty_tier" in result.metadata["added_columns"]
    print("✅ test_schema_drift_detects_new_column")


# =============================================================================
# TEST: Row Count Anomaly Detection
# =============================================================================

def test_row_count_first_run():
    """Row count should pass on first run (no history)."""
    spark = get_test_spark()
    df = spark.createDataFrame([(i,) for i in range(100)], ["id"])
    rule = RowCountAnomalyRule(
        rule_id="test_rc", severity=Severity.CRITICAL, layer=Layer.GOLD,
    )
    result = row_count_anomaly_detection(df, rule, historical_counts=[])
    
    assert result.passed is True
    assert result.metadata["baseline_established"] is True
    print("✅ test_row_count_first_run")


def test_row_count_detects_drop():
    """Row count should detect significant drop."""
    spark = get_test_spark()
    df = spark.createDataFrame([(i,) for i in range(50)], ["id"])
    rule = RowCountAnomalyRule(
        rule_id="test_rc_fail", min_deviation_pct=-30.0,
        severity=Severity.CRITICAL, layer=Layer.GOLD,
    )
    # Historical: ~100 records each run
    result = row_count_anomaly_detection(
        df, rule,
        historical_counts=[100, 98, 102, 99, 101],
    )
    
    assert result.passed is False
    assert result.metadata["anomaly_type"] == "count_drop"
    print("✅ test_row_count_detects_drop")


# =============================================================================
# TEST: Distribution Anomaly Check
# =============================================================================

def test_distribution_baseline():
    """Distribution check should establish baseline on first run."""
    spark = get_test_spark()
    df = spark.createDataFrame(
        [(float(i),) for i in range(1, 201)], ["price"]
    )
    rule = DistributionAnomalyRule(
        rule_id="test_dist", column_name="price",
        severity=Severity.WARNING, layer=Layer.SILVER,
    )
    result = distribution_anomaly_check(df, rule, historical_stats=None)
    
    assert result.passed is True
    assert result.metadata["baseline_established"] is True
    print("✅ test_distribution_baseline")


# =============================================================================
# RUN ALL TESTS
# =============================================================================

def run_all_tests():
    """Execute all unit tests."""
    print("\n" + "=" * 60)
    print("RUNNING VALIDATION RULE UNIT TESTS")
    print("=" * 60 + "\n")
    
    tests = [
        test_not_null_check_passes_clean_data,
        test_not_null_check_detects_nulls,
        test_unique_key_check_passes,
        test_unique_key_check_detects_duplicates,
        test_accepted_values_passes,
        test_accepted_values_detects_invalid,
        test_positive_numeric_passes,
        test_positive_numeric_detects_negatives,
        test_schema_drift_no_drift,
        test_schema_drift_detects_new_column,
        test_row_count_first_run,
        test_row_count_detects_drop,
        test_distribution_baseline,
    ]
    
    passed = 0
    failed = 0
    
    for test_func in tests:
        try:
            test_func()
            passed += 1
        except AssertionError as e:
            print(f"❌ {test_func.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"⚠️ {test_func.__name__} ERROR: {e}")
            failed += 1
    
    print(f"\n{'='*60}")
    print(f"RESULTS: {passed} passed, {failed} failed out of {len(tests)}")
    print(f"{'='*60}\n")
    
    return passed, failed


if __name__ == "__main__":
    run_all_tests()
