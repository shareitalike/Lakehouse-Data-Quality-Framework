"""
Pipeline Orchestrator — runs the full Bronze → Silver → Gold pipeline.

# DECISION: Single entry point for end-to-end pipeline execution.
# The orchestrator manages dependencies between layers and ensures
# proper ordering: Bronze must complete before Silver starts, etc.

# TRADEOFF: Could use Airflow or Prefect for orchestration. Chose a simple
# Python orchestrator because: (1) no external dependencies, (2) runs on
# Databricks CE without extra setup, (3) demonstrates the orchestration
# pattern without infrastructure overhead.

# FAILURE MODE: If Bronze fails, Silver and Gold run on stale data (or crash).
# The orchestrator catches layer failures and decides whether to continue
# or abort based on configuration.

# INTERVIEW: "How do you orchestrate your data pipelines?"
# → "In production, I'd use Airflow or Databricks Workflows with DAG
#    dependencies. For this framework, I use a Python orchestrator that
#    enforces the same dependency ordering (Bronze→Silver→Gold) and
#    handles failure propagation. The logic is identical — only the
#    execution engine differs."

# SCALE: Orchestration overhead is ~0ms. The pipeline execution time is
# dominated by Spark jobs. Even at 1B rows, orchestration adds <1 second.
"""

import time
import uuid
from typing import Dict, Any

from pyspark.sql import SparkSession

from config.pipeline_configs import PipelineConfig
from utils.path_resolver import get_spark_session
from pipelines.bronze_pipeline import BronzePipeline
from pipelines.silver_pipeline import SilverPipeline
from pipelines.gold_pipeline import GoldPipeline
from observability.sql_queries import SQLObservabilityQueries


def run_full_pipeline(
    config: PipelineConfig = None,
    inject_issues: bool = True,
    spark: SparkSession = None,
) -> Dict[str, Any]:
    """
    Execute the full Medallion pipeline: Bronze → Silver → Gold.
    
    # DECISION: Deterministic execution — same config produces same results.
    # This is critical for debugging, testing, and demo reproducibility.
    
    Args:
        config: Pipeline configuration (uses defaults if None)
        inject_issues: Whether to inject data quality issues in Bronze
        spark: Optional SparkSession (creates one if None)
    
    Returns:
        Combined results from all three layers
    """
    if config is None:
        config = PipelineConfig.default()
    
    if spark is None:
        spark = get_spark_session("LakehouseDQ_Pipeline")
    
    pipeline_start = time.time()
    master_run_id = str(uuid.uuid4())
    
    print("\n" + "=" * 70)
    print("🏗️  LAKEHOUSE DATA QUALITY + OBSERVABILITY FRAMEWORK")
    print("=" * 70)
    print(f"Master Run ID: {master_run_id}")
    print(f"Pipeline: {config.pipeline_name} v{config.pipeline_version}")
    print(f"Records: {config.num_records}")
    print(f"Issue Injection: {'ENABLED' if inject_issues else 'DISABLED'}")
    print("=" * 70)
    
    results = {
        "master_run_id": master_run_id,
        "config": {
            "pipeline_name": config.pipeline_name,
            "num_records": config.num_records,
            "inject_issues": inject_issues,
        },
    }
    
    # =========================================================================
    # BRONZE LAYER
    # =========================================================================
    print("\n" + "🥉" * 25)
    print("LAYER 1: BRONZE — Raw Data Ingestion")
    print("🥉" * 25)
    
    try:
        bronze = BronzePipeline(spark, config)
        bronze_results = bronze.run(inject_issues=inject_issues)
        results["bronze"] = {
            "status": "SUCCESS",
            "run_id": bronze_results["run_id"],
            "record_count": bronze_results["record_count"],
            "output_path": bronze_results["output_path"],
        }
        print(f"\n✅ Bronze complete: {bronze_results['record_count']} records")
    except Exception as e:
        results["bronze"] = {"status": "FAILED", "error": str(e)}
        print(f"\n❌ Bronze FAILED: {e}")
        if config.fail_pipeline_on_critical:
            raise
    
    # =========================================================================
    # SILVER LAYER
    # =========================================================================
    print("\n" + "🥈" * 25)
    print("LAYER 2: SILVER — Cleaned & Normalized")
    print("🥈" * 25)
    
    try:
        silver = SilverPipeline(spark, config)
        silver_results = silver.run()
        results["silver"] = {
            "status": "SUCCESS",
            "run_id": silver_results["run_id"],
            "orders_count": silver_results["orders_count"],
            "quarantine_count": silver_results.get("quarantine_count", 0),
            "customers_count": silver_results["customers_count"],
            "products_count": silver_results["products_count"],
        }
        print(f"\n✅ Silver complete: {silver_results['orders_count']} orders, "
              f"{silver_results.get('quarantine_count', 0)} quarantined")
    except Exception as e:
        results["silver"] = {"status": "FAILED", "error": str(e)}
        print(f"\n❌ Silver FAILED: {e}")
        if config.fail_pipeline_on_critical:
            raise
    
    # =========================================================================
    # GOLD LAYER
    # =========================================================================
    print("\n" + "🥇" * 25)
    print("LAYER 3: GOLD — Business Aggregations")
    print("🥇" * 25)
    
    try:
        gold = GoldPipeline(spark, config)
        gold_results = gold.run()
        results["gold"] = {
            "status": "SUCCESS",
            "run_id": gold_results["run_id"],
            "daily_revenue_rows": gold_results["daily_revenue_count"],
            "product_rows": gold_results["product_count"],
            "customer_rows": gold_results["customer_count"],
        }
        print(f"\n✅ Gold complete: {gold_results['daily_revenue_count']} daily revenue rows, "
              f"{gold_results['product_count']} products, "
              f"{gold_results['customer_count']} customers")
    except Exception as e:
        results["gold"] = {"status": "FAILED", "error": str(e)}
        print(f"\n❌ Gold FAILED: {e}")
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    total_time = time.time() - pipeline_start
    results["total_execution_seconds"] = round(total_time, 2)
    
    print("\n" + "=" * 70)
    print("📊 PIPELINE EXECUTION SUMMARY")
    print("=" * 70)
    
    for layer in ["bronze", "silver", "gold"]:
        layer_info = results.get(layer, {})
        status = layer_info.get("status", "SKIPPED")
        emoji = "✅" if status == "SUCCESS" else "❌"
        print(f"  {emoji} {layer.upper()}: {status}")
    
    print(f"\n  Total execution time: {total_time:.1f} seconds")
    print("=" * 70)
    
    return results


def run_sql_observability_demo(spark: SparkSession, config: PipelineConfig = None):
    """
    Run SQL-based observability queries against the pipeline output.
    
    This demonstrates dbt-style testing philosophy using SparkSQL.
    """
    if config is None:
        config = PipelineConfig.default()
    
    sql = SQLObservabilityQueries(spark)
    
    print("\n" + "=" * 70)
    print("📋 SQL OBSERVABILITY QUERIES (dbt-style)")
    print("=" * 70)
    
    # Register Silver tables as temp views
    try:
        orders_df = spark.read.format("delta").load(config.paths.silver_orders)
        products_df = spark.read.format("delta").load(config.paths.silver_products)
        customers_df = spark.read.format("delta").load(config.paths.silver_customers)
        
        sql.register_temp_views({
            "silver_orders": orders_df,
            "silver_products": products_df,
            "silver_customers": customers_df,
        })
    except Exception as e:
        print(f"Error loading Silver tables: {e}")
        return
    
    # Run SQL checks
    print("\n--- Uniqueness Check ---")
    uniqueness_query = sql.uniqueness_sql("silver_orders", ["order_id"])
    print(uniqueness_query)
    result = spark.sql(uniqueness_query)
    result.show(5)
    
    print("\n--- Not Null Check ---")
    null_query = sql.not_null_sql("silver_orders", "customer_id")
    print(null_query)
    result = spark.sql(null_query)
    result.show()
    
    print("\n--- Accepted Values Check ---")
    av_query = sql.accepted_values_sql(
        "silver_orders", "order_status",
        ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned", "refunded"]
    )
    print(av_query)
    result = spark.sql(av_query)
    result.show(5)
    
    print("\n--- Freshness Check ---")
    fresh_query = sql.freshness_sql("silver_orders", "ingestion_timestamp", 4.0)
    print(fresh_query)
    result = spark.sql(fresh_query)
    result.show()


# Entry point for direct execution
if __name__ == "__main__":
    config = PipelineConfig.default()
    spark = get_spark_session("LakehouseDQ_Main")
    
    # Run full pipeline
    results = run_full_pipeline(config, inject_issues=False, spark=spark)
    
    # Run SQL observability demo
    run_sql_observability_demo(spark, config)
    
    # Stop Spark
    spark.stop()
