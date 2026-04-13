"""
Quarantine Manager — routes failed records to quarantine Delta tables.

# DECISION: Quarantine instead of drop. Dropped records are unrecoverable.
# Quarantined records can be: (1) investigated to find root cause,
# (2) reprocessed after upstream fix, (3) used as evidence for data
# producers to improve data quality.

# TRADEOFF: Could log failures to a separate monitoring system (Elasticsearch,
# CloudWatch). Delta table is better because: (1) same tech stack — no new
# infra, (2) supports SQL queries for investigation, (3) Delta time travel
# for historical analysis, (4) MERGE for idempotent reprocessing.

# FAILURE MODE: Quarantine table grows unbounded if upstream issues aren't
# fixed. A daily injection of 5% bad records × 1M daily records = 50K bad
# records/day = 18M/year. Set retention policies and monitor quarantine growth.

# INTERVIEW: "How do you handle quarantined records?"
# → "Three-tier approach: (1) Alert data producers about patterns in
#    quarantine, (2) periodic review meetings with data owners,
#    (3) automated reprocessing pipeline for records that can be fixed.
#    Quarantine is not a graveyard — it's a hospital."

# SCALE: Quarantine tables are typically 1-5% of main table size.
# At 1B main rows → ~50M quarantine rows/year. Use date partitioning
# and retention policies (delete quarantine records > 90 days).
"""

import time
from typing import Optional, Dict, Any
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from config.pipeline_configs import PipelineConfig
from utils.path_resolver import PATHS


class QuarantineManager:
    """
    Routes failed records to quarantine Delta tables with metadata.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig = None,
    ):
        self.spark = spark
        self.config = config or PipelineConfig.default()
    
    def quarantine_records(
        self,
        failed_df: DataFrame,
        layer: str,
        run_id: str,
        failure_reasons: Dict[str, str] = None,
        dataset_name: str = "",
    ) -> int:
        """
        Write failed records to the quarantine Delta table.
        
        # DECISION: Add metadata columns to quarantine records:
        # - _quarantine_timestamp: when the record was quarantined
        # - _quarantine_run_id: which pipeline run quarantined it
        # - _quarantine_layer: which layer caught the issue
        # - _quarantine_dataset: which dataset the record belongs to
        # These columns enable efficient querying and investigation.
        
        Args:
            failed_df: DataFrame of records to quarantine
            layer: Medallion layer where failure was detected
            run_id: Current pipeline run ID
            failure_reasons: Optional dict of rule_name → failure reason
            dataset_name: Name of the source dataset
        
        Returns:
            Number of records quarantined
        """
        if failed_df is None or failed_df.rdd.isEmpty():
            return 0
        
        # Add quarantine metadata
        quarantine_df = (
            failed_df
            .withColumn("_quarantine_timestamp", F.current_timestamp())
            .withColumn("_quarantine_run_id", F.lit(run_id))
            .withColumn("_quarantine_layer", F.lit(layer))
            .withColumn("_quarantine_dataset", F.lit(dataset_name))
            .withColumn(
                "_quarantine_reasons",
                F.lit(str(failure_reasons) if failure_reasons else "multiple_rules")
            )
        )
        
        # Cap records
        max_records = self.config.quarantine.max_records_to_store
        record_count = quarantine_df.count()
        
        if record_count > max_records:
            # DECISION: Take a random sample instead of first N rows.
            # Random sample is more representative of the failure distribution.
            fraction = max_records / record_count
            quarantine_df = quarantine_df.sample(fraction=fraction, seed=42).limit(max_records)
            record_count = max_records
        
        # Get quarantine path for the layer
        paths = self.config.paths.get_layer_paths(layer)
        quarantine_path = paths.get("quarantine", "")
        
        if not quarantine_path:
            print(f"[QuarantineManager] WARNING: No quarantine path for layer '{layer}'")
            return 0
        
        # Write to Delta table (append mode — never overwrite quarantine)
        # DECISION: Append mode ensures quarantine history is preserved.
        # Each run adds new records without affecting previous quarantine data.
        try:
            (
                quarantine_df
                .write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                # DECISION: mergeSchema=True because quarantine records from
                # different runs may have different schemas (schema drift records
                # have extra columns). This is acceptable for quarantine — we
                # want to capture as much context as possible.
                .save(quarantine_path)
            )
            
            print(f"[QuarantineManager] Quarantined {record_count} records "
                  f"from {dataset_name} ({layer}) → {quarantine_path}")
            
        except Exception as e:
            print(f"[QuarantineManager] ERROR writing quarantine: {e}")
            return 0
        
        return record_count
    
    def get_quarantine_summary(
        self,
        layer: str,
    ) -> Optional[DataFrame]:
        """
        Read quarantine table and provide summary statistics.
        
        # DECISION: Read quarantine for analysis, not for reprocessing.
        # Reprocessing should read from the original source with fixes applied,
        # not from quarantine. Quarantine is for investigation only.
        """
        paths = self.config.paths.get_layer_paths(layer)
        quarantine_path = paths.get("quarantine", "")
        
        try:
            qdf = self.spark.read.format("delta").load(quarantine_path)
            
            summary = (
                qdf
                .groupBy("_quarantine_layer", "_quarantine_dataset", "_quarantine_reasons")
                .agg(
                    F.count("*").alias("record_count"),
                    F.min("_quarantine_timestamp").alias("first_quarantined"),
                    F.max("_quarantine_timestamp").alias("last_quarantined"),
                    F.countDistinct("_quarantine_run_id").alias("affected_runs"),
                )
                .orderBy(F.desc("record_count"))
            )
            
            return summary
            
        except Exception as e:
            print(f"[QuarantineManager] No quarantine data for {layer}: {e}")
            return None
