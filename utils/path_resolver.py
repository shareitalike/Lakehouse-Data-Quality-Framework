"""
Utility module for environment detection and path resolution.

# DECISION: Centralize all path logic in one module so every other module
# imports paths from here instead of hardcoding. This makes the framework
# portable across Databricks CE, local Spark, and CI/CD environments.

# TRADEOFF: Could use environment variables (e.g., LAKEHOUSE_BASE_PATH)
# for maximum flexibility. Chose auto-detection instead because it requires
# zero configuration — important for interview demos and quick onboarding.

# FAILURE MODE: If running on a Spark cluster where /dbfs exists but is not
# actually DBFS (e.g., a custom Docker image), will misdetect as Databricks.
# Mitigation: FORCE_LOCAL env var override.

# INTERVIEW: "How do you handle environment-specific configurations in your
# data pipelines?" → "I use an auto-detecting path resolver with override
# capability, so the same code runs on local dev, CI, and Databricks without
# config changes."

# SCALE: Path resolution is a one-time cost per pipeline run. No scale concern.
# At 1B rows, the bottleneck is never path resolution — it's I/O and shuffle.
"""

import os
from dataclasses import dataclass, field
from typing import Optional


def _is_databricks() -> bool:
    """
    Detect if running inside Databricks environment.
    
    # DECISION: Check for DATABRICKS_RUNTIME_VERSION env var — this is the
    # most reliable indicator. Checking for /dbfs existence is fragile.
    """
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _get_base_path() -> str:
    """
    Determine base storage path based on environment.
    
    # DECISION: Use Unity Catalog Volumes on Databricks Serverless because
    # DBFS is disabled. On local, use a relative path under the project
    # directory for easy cleanup.
    
    # TRADEOFF: Could use /mnt/ paths on classic clusters, but Unity Catalog
    # Volumes work on both classic and serverless compute.
    """
    force_local = os.environ.get("FORCE_LOCAL_PATHS", "").lower() == "true"
    
    if _is_databricks() and not force_local:
        # UPDATED FOR SERVERLESS: Use Unity Catalog Volume instead of DBFS
        # DBFS paths like /tmp/ and /user/ don't work on serverless compute
        return "/Volumes/dev/default/lakehouse_dq"
    else:
        # Local development — use project-relative path
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return os.path.join(project_root, "delta_output")


@dataclass
class LakehousePaths:
    """
    Centralized path configuration for all Delta tables in the framework.
    
    # DECISION: Dataclass instead of dict for IDE autocomplete and type safety.
    # Every path is derived from base_path, ensuring consistency.
    
    # INTERVIEW: "Why not just use string constants?" → "Dataclasses give us
    # type safety, immutability hints, and a single place to modify if the
    # storage strategy changes. It's the difference between junior and senior
    # thinking."
    """
    base_path: str = field(default_factory=_get_base_path)
    
    @property
    def bronze_raw(self) -> str:
        return os.path.join(self.base_path, "bronze", "raw_events")
    
    @property
    def bronze_quarantine(self) -> str:
        return os.path.join(self.base_path, "bronze", "quarantine")
    
    @property
    def silver_orders(self) -> str:
        return os.path.join(self.base_path, "silver", "orders")
    
    @property
    def silver_customers(self) -> str:
        return os.path.join(self.base_path, "silver", "customers")
    
    @property
    def silver_products(self) -> str:
        return os.path.join(self.base_path, "silver", "products")
    
    @property
    def silver_quarantine(self) -> str:
        return os.path.join(self.base_path, "silver", "quarantine")
    
    @property
    def gold_daily_revenue(self) -> str:
        return os.path.join(self.base_path, "gold", "daily_revenue")
    
    @property
    def gold_product_performance(self) -> str:
        return os.path.join(self.base_path, "gold", "product_performance")
    
    @property
    def gold_customer_ltv(self) -> str:
        return os.path.join(self.base_path, "gold", "customer_lifetime_value")
    
    @property
    def gold_quarantine(self) -> str:
        return os.path.join(self.base_path, "gold", "quarantine")
    
    @property
    def observability_metrics(self) -> str:
        return os.path.join(self.base_path, "observability", "metrics")
    
    @property
    def observability_rule_results(self) -> str:
        return os.path.join(self.base_path, "observability", "rule_results")
    
    def get_layer_paths(self, layer: str) -> dict:
        """Return all paths for a given Medallion layer."""
        layer_map = {
            "bronze": {
                "raw": self.bronze_raw,
                "quarantine": self.bronze_quarantine,
            },
            "silver": {
                "orders": self.silver_orders,
                "customers": self.silver_customers,
                "products": self.silver_products,
                "quarantine": self.silver_quarantine,
            },
            "gold": {
                "daily_revenue": self.gold_daily_revenue,
                "product_performance": self.gold_product_performance,
                "customer_ltv": self.gold_customer_ltv,
                "quarantine": self.gold_quarantine,
            },
        }
        if layer not in layer_map:
            raise ValueError(f"Unknown layer: {layer}. Must be bronze/silver/gold.")
        return layer_map[layer]


# Module-level singleton for convenience
# DECISION: Singleton pattern so all modules share the same paths instance.
# This prevents inconsistency if someone accidentally creates multiple instances
# with different base_paths.
PATHS = LakehousePaths()


def get_spark_session(app_name: str = "LakehouseDQ"):
    """
    Create or get SparkSession with Delta Lake support.
    
    # DECISION: Centralize SparkSession creation so Delta Lake extensions
    # are always configured. Avoids the common bug where someone creates a
    # session without Delta support and gets cryptic errors.
    
    # TRADEOFF: On Databricks, SparkSession already exists. We use getOrCreate()
    # to reuse it. On local, we configure Delta extensions explicitly.
    
    # FAILURE MODE: If delta-spark package is not installed locally, this
    # fails with a confusing ClassNotFoundException. We add a clear error message.
    """
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip
    
    if _is_databricks():
        # On Databricks, session is pre-configured with Delta
        return SparkSession.builder.appName(app_name).getOrCreate()
    else:
        try:
            builder = (
                SparkSession.builder
                .appName(app_name)
                .master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog"
                )
                .config("spark.sql.warehouse.dir", os.path.join(PATHS.base_path, "warehouse"))
                .config("spark.driver.memory", "4g")
                .config("spark.sql.shuffle.partitions", "8")
                .config("spark.sql.adaptive.enabled", "true")
            )
            return configure_spark_with_delta_pip(builder).getOrCreate()
        except Exception as e:
            if "ClassNotFoundException" in str(e) or "delta" in str(e).lower():
                raise RuntimeError(
                    "Delta Lake not found. Install with: "
                    "pip install delta-spark\n"
                    "Original error: " + str(e)
                )
            raise
