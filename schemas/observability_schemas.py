"""
Observability layer schema definitions.

# DECISION: The observability table is the single most important table in this
# framework. It captures every validation result, every metric, every anomaly.
# It must be append-only, never updated, never deleted.

# TRADEOFF: Could store observability data in a relational database (PostgreSQL)
# for faster ad-hoc queries. Chose Delta because: (1) same technology stack
# reduces operational complexity, (2) Delta supports time travel for debugging
# past runs, (3) no additional infrastructure needed.

# FAILURE MODE: If the observability table itself becomes corrupted or
# accidentally truncated, you lose ALL historical trend data. Mitigation:
# (1) append-only mode prevents accidental overwrites, (2) Delta time travel
# allows recovery, (3) periodic backups of the Delta table.

# INTERVIEW: "Why is your observability table append-only?"
# → "Three reasons: immutable audit trail for compliance, time-series trend
#    analysis requires historical data, and append-only prevents accidental
#    deletion of critical monitoring data. You can't do trend-based alerting
#    if someone accidentally runs a DELETE."

# SCALE: At 10K pipeline runs per day with 10 rules each, that's 100K rows/day.
# Over a year: ~36M rows. Delta handles this easily with date partitioning.
# At extreme scale, set a retention policy (e.g., keep 90 days detailed,
# aggregate older data into daily summaries).
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    IntegerType,
    LongType,
    BooleanType,
    MapType,
)


# =============================================================================
# OBSERVABILITY METRICS TABLE
# =============================================================================
# This is the core metrics store — every validation run writes here.

OBSERVABILITY_METRICS_SCHEMA = StructType([
    StructField("run_id", StringType(), nullable=False),
    # DECISION: UUID-based run_id for globally unique identification.
    # Alternative: timestamp-based ID. Rejected because timestamps can collide
    # in parallel pipeline runs.
    
    StructField("dataset_name", StringType(), nullable=False),
    # Which dataset was validated (e.g., "bronze_raw_events", "silver_orders")
    
    StructField("layer", StringType(), nullable=False),
    # Medallion layer: "bronze", "silver", "gold"
    
    StructField("rule_name", StringType(), nullable=False),
    # Name of the validation rule (e.g., "not_null_check")
    
    StructField("rule_version", StringType(), nullable=False),
    # Version of the rule config used. Critical for audit: "which version
    # of the rule was active when this metric was captured?"
    # INTERVIEW: "Why version your validation rules?"
    # → "Because rule thresholds change over time. When investigating a past
    #    incident, you need to know what thresholds were active. Without
    #    versioning, you can't distinguish 'rule was too loose' from
    #    'data was genuinely bad.'"
    
    StructField("metric_name", StringType(), nullable=False),
    # Specific metric: "null_rate", "duplicate_rate", "freshness_lag_seconds",
    # "row_count", "pass_rate", "distribution_drift_score", etc.
    
    StructField("metric_value", DoubleType(), nullable=False),
    # Numeric value of the metric
    
    StructField("passed", BooleanType(), nullable=False),
    # Whether the rule passed or failed for this run
    
    StructField("severity", StringType(), nullable=False),
    # "critical", "warning", "info"
    
    StructField("total_records", LongType(), nullable=False),
    # Total records evaluated
    
    StructField("failed_records", LongType(), nullable=False),
    # Records that failed this rule
    
    StructField("run_timestamp", TimestampType(), nullable=False),
    # When this validation run occurred
    
    StructField("pipeline_name", StringType(), nullable=True),
    # Optional: which pipeline triggered this run
    
    StructField("metadata_json", StringType(), nullable=True),
    # DECISION: JSON string for flexible metadata instead of MapType.
    # MapType has limitations in Delta (can't filter on map values efficiently).
    # JSON string is queryable with get_json_object() and schema-flexible.
    # TRADEOFF: Loses type safety inside the JSON. Acceptable because metadata
    # is supplementary — core metrics are in typed columns.
])


# =============================================================================
# OBSERVABILITY RULE RESULTS TABLE
# =============================================================================
# Detailed per-record results for debugging. Higher cardinality than metrics.

OBSERVABILITY_RULE_RESULTS_SCHEMA = StructType([
    StructField("run_id", StringType(), nullable=False),
    StructField("rule_name", StringType(), nullable=False),
    StructField("dataset_name", StringType(), nullable=False),
    StructField("record_id", StringType(), nullable=True),
    # Primary key of the failed record for traceability
    
    StructField("failure_reason", StringType(), nullable=True),
    # Human-readable explanation: "customer_id is null", "duplicate order_id"
    
    StructField("record_snapshot", StringType(), nullable=True),
    # JSON serialization of the failed record for debugging.
    # SCALE: At 1B rows with 5% failure rate, this table gets 50M rows per run.
    # Consider sampling: store only first N failures per rule per run.
    # DECISION: Capped at 1000 records per rule per run by default.
    
    StructField("run_timestamp", TimestampType(), nullable=False),
])


# Partition strategy for observability tables
# DECISION: Partition by run_timestamp (date-truncated) for time-range pruning.
# Most queries are "show me metrics for the last 7 days" — date partitioning
# makes these queries scan only relevant partitions.
# SCALE: At 10K rows/day → no partitioning needed. At 1M rows/day → partition
# by date. At 100M rows/day → partition by date + layer.
OBSERVABILITY_PARTITION_COLUMNS = ["run_timestamp"]
