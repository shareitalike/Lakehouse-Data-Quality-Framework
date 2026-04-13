"""
Metrics Store — writes observability metrics to Delta table.

# DECISION: Append-only Delta table for metrics storage. WHY:
# 1. IMMUTABLE AUDIT TRAIL: Every metric ever recorded is preserved.
#    "What was the null rate 3 months ago?" — queryable instantly.
# 2. TIME-SERIES ANALYSIS: Trend detection requires historical data.
#    "Is null rate trending upward over the last 30 days?" needs append-only.
# 3. REGULATORY COMPLIANCE: Some industries require proof of data quality
#    over time. Append-only with Delta time travel provides this.
# 4. NO ACCIDENTAL DELETION: An errant DELETE or TRUNCATE on the metrics
#    table destroys ALL monitoring history. Append-only mode prevents this.

# TRADEOFF: Append-only means the table grows forever. Mitigation:
# (1) Partition by date for efficient pruning, (2) retention policy
# (VACUUM after 90 days), (3) aggregate old data into daily summaries.

# FAILURE MODE: If the metrics write fails, the pipeline should NOT fail.
# Metrics are observability — losing a metrics write is acceptable.
# Losing pipeline data because of a metrics write failure is not.

# INTERVIEW: "Why is your observability table append-only?"
# → "Append-only gives me an immutable audit trail, enables time-series
#    trend analysis, and prevents accidental data loss. I can always
#    answer 'what was the data quality on March 15th?' because no one
#    can delete that row. It's the same principle as a database WAL —
#    append-only for reliability."

# SCALE: 100 rules × 10 metrics × 24 hourly runs × 365 days = ~8.7M rows/year.
# That's nothing for Delta. At 1000 rules with minute-level runs: ~525M rows/year.
# Still manageable with date partitioning and VACUUM.
"""

from typing import List, Optional
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from utils.path_resolver import PATHS
from rules.validation_result import ValidationResult
from observability.metrics_collector import MetricsCollector


class MetricsStore:
    """
    Persists observability metrics to append-only Delta table.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        metrics_path: str = None,
    ):
        self.spark = spark
        self.metrics_path = metrics_path or PATHS.observability_metrics
        self.collector = MetricsCollector(spark)
    
    def write_metrics(
        self,
        results: List[ValidationResult],
        run_id: str,
        pipeline_name: str = "",
    ) -> int:
        """
        Write validation results as metrics to the Delta table.
        
        # DECISION: Convert results → metrics DataFrame → Delta write.
        # This two-step process allows inspection of metrics before writing
        # (useful for debugging).
        
        Args:
            results: List of ValidationResult objects
            run_id: Unique identifier for this pipeline run
            pipeline_name: Name of the pipeline
        
        Returns:
            Number of metric rows written
        """
        try:
            # Collect metrics
            metrics_df = self.collector.collect_from_results(
                results, run_id, pipeline_name
            )
            
            metric_count = metrics_df.count()
            
            if metric_count == 0:
                print("[MetricsStore] No metrics to write")
                return 0
            
            # Write to Delta (APPEND ONLY)
            # DECISION: mode="append" is the ONLY acceptable mode here.
            # Overwrite would destroy historical metrics.
            # Merge is unnecessary — each run produces new rows.
            (
                metrics_df
                .write
                .format("delta")
                .mode("append")
                .save(self.metrics_path)
            )
            
            print(f"[MetricsStore] Wrote {metric_count} metrics to {self.metrics_path}")
            return metric_count
            
        except Exception as e:
            # DECISION: Never crash on metrics write failure.
            # Log the error and continue. The pipeline's primary job is
            # data processing, not metrics emission.
            print(f"[MetricsStore] ERROR writing metrics: {e}")
            print(f"[MetricsStore] Pipeline will continue — metrics loss is acceptable")
            return 0
    
    def read_metrics(
        self,
        dataset_name: str = None,
        layer: str = None,
        rule_name: str = None,
        last_n_days: int = None,
    ) -> Optional[DataFrame]:
        """
        Read metrics from the Delta table with optional filters.
        
        Args:
            dataset_name: Filter by dataset
            layer: Filter by Medallion layer
            rule_name: Filter by rule name
            last_n_days: Filter to last N days
        
        Returns:
            Filtered metrics DataFrame
        """
        try:
            df = self.spark.read.format("delta").load(self.metrics_path)
            
            if dataset_name:
                df = df.filter(F.col("dataset_name") == dataset_name)
            if layer:
                df = df.filter(F.col("layer") == layer)
            if rule_name:
                df = df.filter(F.col("rule_name") == rule_name)
            if last_n_days:
                cutoff = F.date_sub(F.current_date(), last_n_days)
                df = df.filter(F.col("run_timestamp") >= cutoff)
            
            return df
            
        except Exception as e:
            print(f"[MetricsStore] Error reading metrics: {e}")
            return None
    
    def get_trend(
        self,
        metric_name: str,
        dataset_name: str,
        rule_name: str = None,
        last_n_runs: int = 10,
    ) -> Optional[DataFrame]:
        """
        Get metric trend over recent runs.
        
        # DECISION: Return ordered by run_timestamp for time-series visualization.
        # This is the query behind "show me null rate trend for the last 10 runs."
        """
        df = self.read_metrics(dataset_name=dataset_name, rule_name=rule_name)
        if df is None:
            return None
        
        trend = (
            df
            .filter(F.col("metric_name") == metric_name)
            .orderBy(F.desc("run_timestamp"))
            .limit(last_n_runs)
            .select(
                "run_id", "run_timestamp", "metric_name",
                "metric_value", "passed", "rule_name"
            )
            .orderBy("run_timestamp")
        )
        
        return trend
    
    def get_historical_row_counts(
        self,
        dataset_name: str,
        last_n_runs: int = 10,
    ) -> List[int]:
        """
        Get historical row counts for row count anomaly detection.
        
        # DECISION: Read row_count metric from observability table to feed
        # into the next run's anomaly detection. This closes the feedback
        # loop: each run's metrics inform the next run's thresholds.
        """
        df = self.read_metrics(dataset_name=dataset_name)
        if df is None:
            return []
        
        counts_df = (
            df
            .filter(F.col("metric_name") == "row_count")
            .orderBy(F.desc("run_timestamp"))
            .limit(last_n_runs)
            .select("metric_value")
            .orderBy("run_timestamp")
        )
        
        rows = counts_df.collect()
        return [int(r["metric_value"]) for r in rows]
