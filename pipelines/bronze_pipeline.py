"""
Bronze Pipeline — raw data ingestion with DQ validation.

# DECISION: Bronze is the "accept everything, validate everything" layer.
# Data enters as-is from the source. We apply minimal transformations (none)
# but full observability. The goal: preserve raw fidelity while flagging issues
# for Silver to handle.

# TRADEOFF: Could reject bad records at Bronze (fail-fast). Chose accept-all
# because: (1) Bronze is the system of record — losing raw data is fatal,
# (2) Silver handles cleanup — separation of concerns, (3) rejected records
# in Bronze are unrecoverable if the source doesn't support replay.

# FAILURE MODE: If Bronze accepts ALL data without any validation, downstream
# layers are overwhelmed with garbage. The middle ground: Bronze validates but
# quarantines (doesn't reject). Critical validation errors route records to
# quarantine; they're still preserved but don't pollute Silver.

# INTERVIEW: "What's the purpose of the Bronze layer?"
# → "Raw data preservation with observability. Bronze is the 'truth source' —
#    it stores data exactly as received. Validation runs but doesn't modify.
#    We quarantine critically bad records but never drop them. If someone
#    asks 'what did the source send us on March 15th?', Bronze answers that."

# SCALE: 10K rows → instant write. 10M → ~30 seconds. 1B → partition by
# ingestion date, use Delta OPTIMIZE + ZORDER post-write for query perf.
"""

import uuid
from datetime import datetime
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from config.pipeline_configs import PipelineConfig
from config.rule_configs import get_bronze_rules
from config.layer_schemas import BRONZE_RAW_EVENTS_CONTRACT
from data_generation.bronze_generator import generate_clean_bronze_data, get_product_reference_df
from data_generation.issue_injector import inject_all_issues, InjectionConfig
from engine.validation_engine import ValidationEngine
from engine.quarantine_manager import QuarantineManager
from engine.contract_enforcer import ContractEnforcer
from observability.metrics_store import MetricsStore
from schemas.bronze_schemas import BRONZE_RAW_EVENTS_SCHEMA


class BronzePipeline:
    """
    Bronze layer pipeline: ingest → validate → store → quarantine.
    
    # DECISION: Class-based pipeline for statefulness. The pipeline tracks
    # its run_id, start time, and results across multiple method calls.
    # This enables: (1) correlated metrics, (2) consistent quarantine
    # tagging, (3) clean lifecycle management.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig = None,
    ):
        self.spark = spark
        self.config = config or PipelineConfig.default()
        self.engine = ValidationEngine(spark, config)
        self.quarantine_mgr = QuarantineManager(spark, config)
        self.metrics_store = MetricsStore(spark)
        self.contract_enforcer = ContractEnforcer(spark)
        self.run_id = str(uuid.uuid4())
    
    def generate_data(
        self,
        inject_issues: bool = True,
    ) -> DataFrame:
        """
        Generate synthetic bronze data with optional issue injection.
        
        # DECISION: Data generation is part of the pipeline for demo purposes.
        # In production, this would be replaced by a Kafka consumer, S3 read,
        # or API ingestion. The interface is the same: returns a DataFrame.
        """
        print(f"\n{'='*60}")
        print(f"BRONZE PIPELINE — Data Generation")
        print(f"{'='*60}")
        
        # Generate clean data
        df = generate_clean_bronze_data(
            self.spark,
            num_records=self.config.num_records,
            seed=self.config.random_seed,
        )
        print(f"Generated {df.count()} clean records")
        
        # Inject issues if requested
        if inject_issues:
            injection_config = InjectionConfig(seed=self.config.random_seed)
            df = inject_all_issues(df, injection_config)
        
        return df
    
    def validate(
        self,
        df: DataFrame,
    ) -> Dict[str, Any]:
        """
        Run Bronze validation and return results with quarantine info.
        """
        print(f"\n{'='*60}")
        print(f"BRONZE PIPELINE — Validation (run_id: {self.run_id})")
        print(f"{'='*60}")
        
        # Run validation rules
        results = self.engine.validate_bronze(df)
        
        # Check data contract
        contract_result = self.contract_enforcer.enforce_contract(
            df, BRONZE_RAW_EVENTS_CONTRACT
        )
        
        # Determine quarantine
        quarantine_records = None
        quarantine_count = 0
        
        if self.engine.should_quarantine(results, df):
            quarantine_records = self.engine.get_quarantine_records(results)
            if quarantine_records is not None:
                quarantine_count = self.quarantine_mgr.quarantine_records(
                    quarantine_records,
                    layer="bronze",
                    run_id=self.run_id,
                    dataset_name="bronze_raw_events",
                )
        
        # Write observability metrics
        if self.config.enable_observability:
            self.metrics_store.write_metrics(
                results,
                run_id=self.run_id,
                pipeline_name=self.config.pipeline_name,
            )
        
        return {
            "run_id": self.run_id,
            "results": results,
            "contract_result": contract_result,
            "quarantine_count": quarantine_count,
            "summary": self.engine.generate_summary(results),
        }
    
    def write_to_delta(
        self,
        df: DataFrame,
        validation_results: Dict[str, Any] = None,
    ) -> str:
        """
        Write validated Bronze data to Delta table.
        
        # DECISION: Write ALL records to Bronze, including those that fail
        # validation. Bronze is the raw layer — it preserves everything.
        # Quarantine gets a COPY of bad records; Bronze keeps the originals.
        
        # DECISION: Schema enforcement mode (not evolution) for Bronze.
        # Extra columns from schema drift are dropped by providing explicit schema.
        # This is the Bronze contract: only expected columns enter the lakehouse.
        
        # TRADEOFF: Could use mergeSchema=True to accept new columns.
        # Chose enforcement because: uncontrolled schema expansion in Bronze
        # leads to "schema soup" — 50 columns nobody understands after 6 months.
        """
        output_path = self.config.paths.bronze_raw
        
        # Enforce expected schema (drop extra columns like loyalty_tier from drift)
        expected_columns = [f.name for f in BRONZE_RAW_EVENTS_SCHEMA.fields]
        available_columns = [c for c in expected_columns if c in df.columns]
        df_clean = df.select(available_columns)
        
        # Add ingestion metadata
        df_clean = (
            df_clean
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn("_run_id", F.lit(self.run_id))
        )
        
        # Write to Delta
        # DECISION: mode="overwrite" for demo simplicity. In production,
        # use mode="append" with idempotency based on run_id to prevent
        # duplicate writes on retry.
        (
            df_clean
            .write
            .format("delta")
            .mode("overwrite")
            .save(output_path)
        )
        
        print(f"[BronzePipeline] Wrote {df_clean.count()} records to {output_path}")
        return output_path
    
    def run(self, inject_issues: bool = True) -> Dict[str, Any]:
        """
        Execute full Bronze pipeline: generate → validate → write.
        
        # DECISION: Idempotent design — running the pipeline twice with
        # the same seed produces identical results. This is critical for
        # debugging and testing.
        """
        # Step 1: Generate data
        df = self.generate_data(inject_issues=inject_issues)
        
        # Step 2: Validate
        validation = self.validate(df)
        
        # Step 3: Write to Delta
        output_path = self.write_to_delta(df, validation)
        
        # Step 4: Return summary
        return {
            "pipeline": "bronze",
            "run_id": self.run_id,
            "output_path": output_path,
            "record_count": df.count(),
            "validation": validation,
        }
