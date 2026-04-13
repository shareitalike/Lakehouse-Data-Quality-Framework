"""
Integration tests for the full pipeline.

Run with: python -m pytest tests/test_pipeline.py -v
"""

import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from pyspark.sql import SparkSession

from config.pipeline_configs import PipelineConfig
from pipelines.orchestrator import run_full_pipeline


def get_test_spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("DQ_PipelineTest")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def test_full_pipeline_clean_data():
    """Full pipeline should complete successfully with clean data."""
    spark = get_test_spark()
    config = PipelineConfig.for_testing()
    
    results = run_full_pipeline(
        config=config,
        inject_issues=False,  # Clean data
        spark=spark,
    )
    
    assert results["bronze"]["status"] == "SUCCESS"
    assert results["silver"]["status"] == "SUCCESS"
    assert results["gold"]["status"] == "SUCCESS"
    print("✅ test_full_pipeline_clean_data")


def test_full_pipeline_with_issues():
    """Full pipeline should handle injected issues without crashing."""
    spark = get_test_spark()
    config = PipelineConfig.for_testing()
    
    results = run_full_pipeline(
        config=config,
        inject_issues=True,  # Inject issues
        spark=spark,
    )
    
    # Pipeline should complete (quarantine bad records, not crash)
    assert results["bronze"]["status"] == "SUCCESS"
    assert results["silver"]["status"] == "SUCCESS"
    assert results["gold"]["status"] == "SUCCESS"
    
    # Silver should have fewer records than Bronze (cleaning removed some)
    assert results["silver"]["orders_count"] < results["bronze"]["record_count"]
    print("✅ test_full_pipeline_with_issues")


def test_silver_deduplication():
    """Silver pipeline should deduplicate records."""
    spark = get_test_spark()
    config = PipelineConfig.for_testing()
    config.num_records = 500
    
    results = run_full_pipeline(
        config=config,
        inject_issues=True,
        spark=spark,
    )
    
    # Read Silver orders and verify uniqueness
    orders_df = spark.read.format("delta").load(config.paths.silver_orders)
    total = orders_df.count()
    unique = orders_df.select("order_id").distinct().count()
    
    assert total == unique, f"Silver has duplicates: {total} total, {unique} unique"
    print("✅ test_silver_deduplication")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("RUNNING INTEGRATION TESTS")
    print("=" * 60 + "\n")
    
    tests = [
        test_full_pipeline_clean_data,
        test_full_pipeline_with_issues,
        test_silver_deduplication,
    ]
    
    for test in tests:
        try:
            test()
        except Exception as e:
            print(f"❌ {test.__name__}: {e}")
