"""
Metrics Collector — captures DQ metrics from validation results.

# DECISION: Separate metrics collection from metrics storage. The collector
# transforms ValidationResults into metric records. The store writes them.
# This separation enables: (1) different storage backends (Delta, Postgres,
# S3), (2) metric enrichment before storage, (3) testable transformation logic.

# TRADEOFF: Could write metrics directly in each rule. Centralized collection
# ensures consistency — every metric has the same schema, timestamp format,
# and run_id. No rule "forgets" to emit a metric.

# FAILURE MODE: If metrics collection throws an exception, it should NEVER
# block the pipeline. Metrics are observability — the pipeline's job is to
# process data, not to emit metrics. Wrap collection in try/except.

# INTERVIEW: "What's the difference between validation and observability?"
# → "Validation GATES data — it decides if records pass or fail.
#    Observability MONITORS trends — it tracks metrics over time.
#    Validation is a checkpoint; observability is a time-series.
#    You need validation for correctness. You need observability for
#    detecting slow degradation that passes every individual check."

# SCALE: Metrics are tiny compared to data. 100 rules × 10 metrics each =
# 1000 metric rows per pipeline run. Even at hourly runs for a year,
# that's ~8.7M rows — trivial for Delta.
"""

import json
from typing import List, Dict, Any
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F

from rules.validation_result import ValidationResult
from schemas.observability_schemas import OBSERVABILITY_METRICS_SCHEMA


class MetricsCollector:
    """
    Transforms validation results into structured metric records.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def collect_from_results(
        self,
        results: List[ValidationResult],
        run_id: str,
        pipeline_name: str = "",
    ) -> DataFrame:
        """
        Convert validation results into an observability metrics DataFrame.
        
        Each ValidationResult produces one or more metric rows:
        - Core metric: pass/fail status with failure rate
        - Rule-specific metrics: null_rate, dup_rate, z_score, etc.
        
        # DECISION: Flatten ValidationResult into multiple metric rows.
        # Each metric has a single numeric value. This columnar design
        # enables efficient aggregation: "SELECT AVG(metric_value) WHERE
        # metric_name = 'null_rate'" — no JSON parsing needed.
        
        Returns:
            DataFrame conforming to OBSERVABILITY_METRICS_SCHEMA
        """
        metric_rows = []
        
        for result in results:
            timestamp = datetime.utcnow()
            severity = result.severity
            layer = result.layer or "unknown"
            dataset = result.dataset_name or "unknown"
            
            # Core metric: failure rate
            metric_rows.append(Row(
                run_id=run_id,
                dataset_name=dataset,
                layer=layer,
                rule_name=result.rule_name,
                rule_version=result.rule_version,
                metric_name="failure_rate",
                metric_value=float(result.failure_rate),
                passed=result.passed,
                severity=severity,
                total_records=int(result.total_records),
                failed_records=int(result.failed_records),
                run_timestamp=timestamp,
                pipeline_name=pipeline_name,
                metadata_json=json.dumps(
                    {k: str(v) for k, v in result.metadata.items()}
                    if result.metadata else {}
                ),
            ))
            
            # Core metric: pass rate
            metric_rows.append(Row(
                run_id=run_id,
                dataset_name=dataset,
                layer=layer,
                rule_name=result.rule_name,
                rule_version=result.rule_version,
                metric_name="pass_rate",
                metric_value=float(result.pass_rate),
                passed=result.passed,
                severity=severity,
                total_records=int(result.total_records),
                failed_records=int(result.failed_records),
                run_timestamp=timestamp,
                pipeline_name=pipeline_name,
                metadata_json=None,
            ))
            
            # Rule-specific metrics from metadata
            meta = result.metadata or {}
            
            # Null rate
            if "null_rate" in meta:
                metric_rows.append(self._create_metric_row(
                    run_id, dataset, layer, result, "null_rate",
                    float(meta["null_rate"]), timestamp, pipeline_name
                ))
            
            # Duplicate metrics
            if "duplicate_groups" in meta:
                metric_rows.append(self._create_metric_row(
                    run_id, dataset, layer, result, "duplicate_groups",
                    float(meta["duplicate_groups"]), timestamp, pipeline_name
                ))
            
            if "dedup_ratio" in meta:
                metric_rows.append(self._create_metric_row(
                    run_id, dataset, layer, result, "dedup_ratio",
                    float(meta["dedup_ratio"]), timestamp, pipeline_name
                ))
            
            # Freshness metrics
            if "age_hours" in meta:
                metric_rows.append(self._create_metric_row(
                    run_id, dataset, layer, result, "freshness_lag_hours",
                    float(meta["age_hours"]), timestamp, pipeline_name
                ))
            
            # Row count anomaly metrics
            if "deviation_pct" in meta:
                metric_rows.append(self._create_metric_row(
                    run_id, dataset, layer, result, "row_count_delta_pct",
                    float(meta["deviation_pct"]), timestamp, pipeline_name
                ))
            
            if "current_count" in meta:
                metric_rows.append(self._create_metric_row(
                    run_id, dataset, layer, result, "row_count",
                    float(meta["current_count"]), timestamp, pipeline_name
                ))
            
            # Distribution metrics
            if "z_score" in meta and meta["z_score"] is not None:
                metric_rows.append(self._create_metric_row(
                    run_id, dataset, layer, result, "distribution_z_score",
                    float(meta["z_score"]), timestamp, pipeline_name
                ))
            
            if "max_percentile_drift_pct" in meta:
                metric_rows.append(self._create_metric_row(
                    run_id, dataset, layer, result, "distribution_drift_pct",
                    float(meta["max_percentile_drift_pct"]), timestamp, pipeline_name
                ))
            
            # Schema drift metrics
            if "has_drift" in meta:
                metric_rows.append(self._create_metric_row(
                    run_id, dataset, layer, result, "schema_drift_detected",
                    1.0 if meta["has_drift"] else 0.0, timestamp, pipeline_name
                ))
            
            # Execution time
            metric_rows.append(self._create_metric_row(
                run_id, dataset, layer, result, "execution_time_ms",
                float(result.execution_time_ms), timestamp, pipeline_name
            ))
        
        # Create DataFrame
        if metric_rows:
            return self.spark.createDataFrame(metric_rows, OBSERVABILITY_METRICS_SCHEMA)
        else:
            return self.spark.createDataFrame([], OBSERVABILITY_METRICS_SCHEMA)
    
    def _create_metric_row(
        self,
        run_id: str,
        dataset: str,
        layer: str,
        result: ValidationResult,
        metric_name: str,
        metric_value: float,
        timestamp: datetime,
        pipeline_name: str,
    ) -> Row:
        """Helper to create a metric Row."""
        return Row(
            run_id=run_id,
            dataset_name=dataset,
            layer=layer,
            rule_name=result.rule_name,
            rule_version=result.rule_version,
            metric_name=metric_name,
            metric_value=metric_value,
            passed=result.passed,
            severity=result.severity,
            total_records=int(result.total_records),
            failed_records=int(result.failed_records),
            run_timestamp=timestamp,
            pipeline_name=pipeline_name,
            metadata_json=None,
        )
