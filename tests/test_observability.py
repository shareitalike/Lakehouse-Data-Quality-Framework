"""
Tests for the observability metrics layer.
"""

import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.rule_configs import NotNullRule, Severity, Layer
from rules.not_null_check import not_null_check
from rules.validation_result import ValidationResult
from observability.metrics_collector import MetricsCollector
from schemas.observability_schemas import OBSERVABILITY_METRICS_SCHEMA


def get_test_spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("DQ_ObservabilityTest")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def test_metrics_collection():
    """Metrics collector should produce properly structured DataFrame."""
    spark = get_test_spark()
    
    # Create a simple validation result
    df = spark.createDataFrame(
        [("1", "Alice"), ("2", None), ("3", "Charlie")],
        ["id", "name"]
    )
    rule = NotNullRule(
        rule_id="test_null", column_name="name",
        severity=Severity.WARNING, layer=Layer.BRONZE,
    )
    result = not_null_check(df, rule, "test_dataset")
    
    # Collect metrics
    collector = MetricsCollector(spark)
    metrics_df = collector.collect_from_results([result], "test-run-id", "test_pipeline")
    
    # Verify schema
    assert metrics_df.count() > 0
    assert "run_id" in metrics_df.columns
    assert "metric_name" in metrics_df.columns
    assert "metric_value" in metrics_df.columns
    
    # Verify run_id was set
    run_ids = [row["run_id"] for row in metrics_df.select("run_id").distinct().collect()]
    assert "test-run-id" in run_ids
    
    # Verify failure_rate metric exists
    metric_names = [row["metric_name"] for row in metrics_df.select("metric_name").distinct().collect()]
    assert "failure_rate" in metric_names
    assert "pass_rate" in metric_names
    
    print("✅ test_metrics_collection")


def test_metrics_schema():
    """Metrics DataFrame should conform to OBSERVABILITY_METRICS_SCHEMA."""
    spark = get_test_spark()
    
    df = spark.createDataFrame(
        [("1", "Alice")],
        ["id", "name"]
    )
    rule = NotNullRule(
        rule_id="test_schema", column_name="name",
        severity=Severity.CRITICAL, layer=Layer.SILVER,
    )
    result = not_null_check(df, rule, "test_dataset")
    
    collector = MetricsCollector(spark)
    metrics_df = collector.collect_from_results([result], "run-123")
    
    # Verify column names match expected schema
    expected_cols = [f.name for f in OBSERVABILITY_METRICS_SCHEMA.fields]
    actual_cols = metrics_df.columns
    
    for col in expected_cols:
        assert col in actual_cols, f"Missing column: {col}"
    
    print("✅ test_metrics_schema")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("RUNNING OBSERVABILITY TESTS")
    print("=" * 60 + "\n")
    
    test_metrics_collection()
    test_metrics_schema()
    
    print("\nAll observability tests passed!")
