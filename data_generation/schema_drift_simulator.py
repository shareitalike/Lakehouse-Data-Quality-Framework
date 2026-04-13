"""
Schema drift simulator — generates multiple batches with evolving schemas.

# DECISION: Schema drift is one of the most insidious production issues.
# Unlike null values or duplicates, schema drift changes the STRUCTURE of
# data, not the CONTENT. This module simulates progressive schema evolution
# across multiple batches to test our framework's detection capabilities.

# TRADEOFF: Could test schema drift with a single "before/after" comparison.
# Chose multi-batch simulation because real drift happens gradually:
# Batch 1: original schema → Batch 2: new column added → Batch 3: column
# renamed → Batch 4: column type changed. Each transition tests different
# detection logic.

# FAILURE MODE: Schema drift detection can produce false positives during
# planned schema evolution. If the data team intentionally adds a column
# and the drift detector fires, it creates alert fatigue. Mitigation:
# use the SchemaDriftRule.allow_extra_columns flag for expected evolution.

# INTERVIEW: "What's the difference between schema drift and schema evolution?"
# → "Schema drift is UNPLANNED change — upstream modifies without notice.
#    Schema evolution is PLANNED change — coordinated with downstream.
#    Both modify schema; the difference is intent and communication.
#    My framework detects both but treats them differently based on config."

# SCALE: Schema drift detection is O(1) — compares schema metadata, not data.
# Works identically for 10K and 1B rows.
"""

from typing import List, Dict, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, FloatType,
)

from data_generation.bronze_generator import generate_clean_bronze_data


class SchemaDriftSimulator:
    """
    Simulates progressive schema changes across batches.
    
    # DECISION: Class-based design because drift simulation is stateful —
    # we need to track which schema version each batch uses.
    """
    
    def __init__(self, spark: SparkSession, base_records: int = 2000, seed: int = 42):
        self.spark = spark
        self.base_records = base_records
        self.seed = seed
    
    def generate_batch_v1(self) -> Tuple[DataFrame, str]:
        """
        Batch 1: Original schema — baseline.
        
        Returns (DataFrame, description)
        """
        df = generate_clean_bronze_data(
            self.spark,
            num_records=self.base_records,
            seed=self.seed,
            start_date="2024-01-01",
            end_date="2024-03-31",
        )
        return df, "Original schema — 8 columns, no drift"
    
    def generate_batch_v2_new_column(self) -> Tuple[DataFrame, str]:
        """
        Batch 2: New column added (loyalty_tier).
        
        # DECISION: This simulates the most common drift scenario — upstream
        # adds a field. If our Bronze uses schema enforcement, this column
        # is silently dropped. If we use schema evolution (mergeSchema=True),
        # the column is accepted. Either way, drift detection should alert.
        """
        df = generate_clean_bronze_data(
            self.spark,
            num_records=self.base_records,
            seed=self.seed + 100,
            start_date="2024-04-01",
            end_date="2024-06-30",
        )
        # Add new column
        df = df.withColumn(
            "loyalty_tier",
            F.when(F.rand(42) < 0.3, F.lit("gold"))
            .when(F.rand(42) < 0.6, F.lit("silver"))
            .otherwise(F.lit("bronze"))
        )
        return df, "New column added: loyalty_tier (schema drift)"
    
    def generate_batch_v3_missing_column(self) -> Tuple[DataFrame, str]:
        """
        Batch 3: Column removed (currency).
        
        # DECISION: This simulates upstream dropping a field — possibly because
        # they deprecated it or renamed it. This is more dangerous than adding
        # a column because downstream queries that reference the column break.
        
        # FAILURE MODE: If downstream code does df["currency"] without checking
        # column existence, it throws AnalysisException, crashing the pipeline.
        """
        df = generate_clean_bronze_data(
            self.spark,
            num_records=self.base_records,
            seed=self.seed + 200,
            start_date="2024-07-01",
            end_date="2024-09-30",
        )
        # Drop a column
        df = df.drop("currency")
        return df, "Column removed: currency (breaking schema change)"
    
    def generate_batch_v4_type_change(self) -> Tuple[DataFrame, str]:
        """
        Batch 4: Column type changed (quantity from int to string).
        
        # DECISION: Type changes are the most subtle drift. The column name
        # is the same, data looks similar, but type is different. This can
        # cause silent data corruption if not detected.
        
        # INTERVIEW: "What's worse — a missing column or a type change?"
        # → "Type change. A missing column fails loud and fast.
        #    A type change passes silently — your int column becomes a
        #    string, SUM() returns null instead of a number, and nobody
        #    notices until the monthly report is wrong."
        """
        df = generate_clean_bronze_data(
            self.spark,
            num_records=self.base_records,
            seed=self.seed + 300,
            start_date="2024-10-01",
            end_date="2024-12-31",
        )
        # Change type of quantity from int to string
        df = df.withColumn("quantity", F.col("quantity").cast(StringType()))
        return df, "Type change: quantity IntegerType → StringType"
    
    def generate_all_batches(self) -> List[Tuple[DataFrame, str]]:
        """
        Generate all drift scenario batches.
        
        Returns list of (DataFrame, description) tuples.
        """
        batches = [
            self.generate_batch_v1(),
            self.generate_batch_v2_new_column(),
            self.generate_batch_v3_missing_column(),
            self.generate_batch_v4_type_change(),
        ]
        
        print(f"[SchemaDriftSimulator] Generated {len(batches)} batches:")
        for i, (df, desc) in enumerate(batches):
            print(f"  Batch {i+1}: {desc}")
            print(f"           Columns: {df.columns}")
            print(f"           Records: {df.count()}")
        
        return batches
    
    @staticmethod
    def compare_schemas(
        expected: StructType,
        actual: StructType,
    ) -> Dict[str, list]:
        """
        Compare two schemas and report differences.
        
        # DECISION: Return structured diff instead of boolean. A simple
        # "schemas don't match" is useless for debugging. We need to know
        # WHAT changed: added columns? removed columns? type changes?
        
        Returns:
            {
                "added_columns": [...],
                "removed_columns": [...],
                "type_changes": [{"column": ..., "expected": ..., "actual": ...}],
            }
        """
        expected_fields = {f.name: f for f in expected.fields}
        actual_fields = {f.name: f for f in actual.fields}
        
        added = [name for name in actual_fields if name not in expected_fields]
        removed = [name for name in expected_fields if name not in actual_fields]
        
        type_changes = []
        for name in set(expected_fields) & set(actual_fields):
            expected_type = expected_fields[name].dataType.simpleString()
            actual_type = actual_fields[name].dataType.simpleString()
            if expected_type != actual_type:
                type_changes.append({
                    "column": name,
                    "expected_type": expected_type,
                    "actual_type": actual_type,
                })
        
        return {
            "added_columns": added,
            "removed_columns": removed,
            "type_changes": type_changes,
            "has_drift": bool(added or removed or type_changes),
        }
